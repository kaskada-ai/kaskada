use sparrow_core::{ContextCode, ErrorCode};
use tracing::{error, info};

/// Trait for converting arbitrary types to a `tonic::Status`.
///
/// This allows converting external types (such as `anyhow::Error`)
/// to `tonic::Status`.
pub trait IntoStatus {
    type Output;
    fn into_status(self) -> Self::Output;
}

impl IntoStatus for anyhow::Error {
    type Output = tonic::Status;

    fn into_status(self) -> Self::Output {
        // Examine the underlying errors to determine the root cause.
        // This downcast checks both the underlying error *and* the context.
        // See https://docs.rs/anyhow/latest/anyhow/trait.Context.html#effect-on-downcasting.

        if self.is::<tonic::Status>() {
            // If the error or context already contain a status, just return that.
            // This case exists to deal with converting error diagnostics in the compiler.
            // We could (should) return the diagnostics directly as the context, and/or
            // treat those as non-errors (as discussed).
            return self
                .downcast::<tonic::Status>()
                .expect("expected `if` to ensure this was a Status");
        }

        // Determine the status code by looking for a `tonic::Code` or `ContextCode`.
        let code = if let Some(code) = self.downcast_ref::<tonic::Code>() {
            // The context (or error) was a `tonic::Status` code.
            *code
        } else if let Some(code) = self.downcast_ref::<ContextCode>() {
            // The context (or error) was a `ContextCode`.
            code.code()
        } else {
            tonic::Code::Internal
        };

        let message = format!("{self:#}");

        report(code, &message, &self);

        tonic::Status::new(code, message)
    }
}

impl<T, E> IntoStatus for Result<T, E>
where
    E: IntoStatus<Output = tonic::Status>,
{
    type Output = Result<T, E::Output>;

    fn into_status(self) -> Self::Output {
        self.map_err(|e| e.into_status())
    }
}

impl<C> IntoStatus for error_stack::Report<C>
where
    C: error_stack::Context + ErrorCode,
{
    type Output = tonic::Status;

    fn into_status(self) -> Self::Output {
        let context = self.current_context();
        let code = context.error_code();
        let message = self.current_context().to_string();

        report(code, &message, &self);
        tonic::Status::new(code, message)
    }
}

fn report(code: tonic::Code, message: &str, error: &dyn std::fmt::Debug) {
    // Determine the log level based on the code.
    // The `event!` macro requires the log_level to be a constant, so
    // there is no real benefit to compute the level and sharing the log.
    match code {
        tonic::Code::InvalidArgument => {
            // InvalidArgument means either the user sent invalid Fenl
            // or otherwise used the API incorrectly. Either way, it isn't
            // a *server* error, so we report it as info.
            info!("Returning status code {code:?} with message {message}:\n{error:?}")
        }
        _ => {
            // Other codes are treated as errors.
            error!("Returning status code {code:?} with message {message}:\n{error:?}")
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context;
    use error_stack::ResultExt;
    use sparrow_core::context_code;

    use crate::serve::error_status::IntoStatus;

    fn assert_err_status<E: Into<anyhow::Error>>(result: Result<(), E>, expected: tonic::Status) {
        let actual: E = result.err().unwrap();
        let actual: anyhow::Error = actual.into();
        let actual = actual.into_status();
        assert_eq!(actual.code(), expected.code());
        assert_eq!(actual.message(), expected.message());
    }

    #[test]
    fn test_io_error_to_status() {
        let io_error = || {
            Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "IO ERROR",
            ))
        };

        // Check the conversion of an arbitrary (io) error to a status.
        // This should prodcue an internal status with the message.
        assert_err_status(io_error(), tonic::Status::internal("IO ERROR"));

        // Should include the context, if set.
        assert_err_status(
            io_error().context("failed to do thing"),
            tonic::Status::internal("failed to do thing: IO ERROR"),
        );

        // If the context sets a code, it should be preferred.
        assert_err_status(
            io_error().context(tonic::Code::InvalidArgument),
            tonic::Status::invalid_argument("Client specified an invalid argument: IO ERROR"),
        );

        // If the context sets a code and message, it should be included.
        assert_err_status(
            io_error().context(context_code!(
                tonic::Code::InvalidArgument,
                "Failed to do {}",
                5
            )),
            tonic::Status::invalid_argument(
                "Client specified an invalid argument: Failed to do 5: IO ERROR",
            ),
        );
    }

    #[test]
    fn test_error_stack() {
        sparrow_testing::init_test_logging();

        let _span1 = tracing::info_span!("Foo").entered();
        let _span2 = tracing::info_span!("Bar").entered();
        let error: Result<(), _> = Err(error_stack::report!(
            sparrow_runtime::prepare::Error::PreparingColumn
        ))
        .change_context(sparrow_runtime::prepare::Error::SlicingBatch)
        .change_context(sparrow_runtime::prepare::Error::Internal);

        let error = error.into_status().err().unwrap();
        assert_eq!(error.code(), tonic::Code::Internal);
        assert_eq!(error.message(), "internal error");
    }
}
