use std::thread::ThreadId;

/// Create a monitor that watches for thread completions.
pub(crate) struct Monitor {
    tx: tokio::sync::mpsc::Sender<ThreadId>,
    pub(crate) rx: tokio::sync::mpsc::Receiver<ThreadId>,
}

pub(crate) struct MonitorGuard {
    tx: tokio::sync::mpsc::Sender<ThreadId>,
}

impl Drop for MonitorGuard {
    fn drop(&mut self) {
        match self.tx.blocking_send(std::thread::current().id()) {
            Ok(_) => (),
            Err(thread_id) => {
                tracing::error!("Failed to send thread completion for {thread_id:?}");
            }
        }
    }
}

impl Monitor {
    pub fn with_capacity(capacity: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(capacity);
        Monitor { tx, rx }
    }

    pub fn guard(&self) -> MonitorGuard {
        MonitorGuard {
            tx: self.tx.clone(),
        }
    }
}
