use core::pin::Pin;
use std::borrow::Cow;

use error_stack::{IntoReport, Result, ResultExt};
use futures::future::{FusedFuture, Future};
use futures::ready;
use futures::task::{Context, Poll};
use pin_project::pin_project;

use crate::execute::error::Error;

/// A custom `Future` that can be wrapped around a JoinHandle.
///
/// This converts panics (`tokio::task::JoinError`) into an
/// `anyhow::Error` for easier usage.
///
/// This is based on the implementation of `MapProj`, but allows
/// us to avoid dynamic dispatch while supporting a task name.
///
/// See <https://docs.rs/futures-util/0.3.21/src/futures_util/future/future/map.rs.html>.
#[pin_project(project = JoinTaskProj, project_replace = JoinTaskProjReplace)]
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub(crate) enum JoinTask<T> {
    Incomplete {
        name: Cow<'static, str>,
        #[pin]
        task: tokio::task::JoinHandle<Result<T, Error>>,
    },
    Complete,
}

impl<T> JoinTask<T> {
    /// Creates a new Map.
    pub fn new(
        name: impl Into<Cow<'static, str>>,
        task: tokio::task::JoinHandle<Result<T, Error>>,
    ) -> Self {
        Self::Incomplete {
            name: name.into(),
            task,
        }
    }
}

impl<T> FusedFuture for JoinTask<T> {
    fn is_terminated(&self) -> bool {
        match self {
            Self::Incomplete { .. } => false,
            Self::Complete => true,
        }
    }
}

impl<T> Future for JoinTask<T> {
    type Output = Result<T, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.as_mut().project() {
            JoinTaskProj::Incomplete { task, .. } => {
                let output = ready!(task.poll(cx));
                match self.project_replace(JoinTask::Complete) {
                    JoinTaskProjReplace::Incomplete { name, .. } => {
                        let output = output
                            .into_report()
                            .change_context(Error::internal_msg("panic in task"))
                            .attach_printable_lazy(|| format!("task name {name:?}"))?;
                        Poll::Ready(output)
                    }
                    JoinTaskProjReplace::Complete => unreachable!(),
                }
            }
            JoinTaskProj::Complete => {
                panic!("JoinTask must not be polled after it returned `Poll::Ready`")
            }
        }
    }
}
