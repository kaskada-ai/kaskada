/// Special wrapper for a string that with a specific status code.
///
/// This is intended for use when creating an error with `anyhow!`
/// or adding a context with `with_context(|| ...)`. It allows specifying
/// a specific status code to go with the message. This is only necessary
/// when the defalut status of an error is incorrect in a specific place.
///
/// Most errors default to `Internal`. See `IntoStatus` in `sparrow-main`.
#[derive(Debug)]
pub struct ContextCode {
    code: tonic::Code,
    message: String,
}

impl ContextCode {
    pub fn new(code: tonic::Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> tonic::Code {
        self.code
    }
}

impl std::fmt::Display for ContextCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

/// Macro for creating a ContextCode.
#[macro_export]
macro_rules! context_code {
    ($code:expr, $msg:literal $(,)?) =>  ({
        // context_code!(Code::Internal, "Message")
        // Handle $:literal as a special case to make cargo-expanded code more
        // concise in the common case.
        $crate::ContextCode::new($code, $msg.to_string())
    });
    ($code:expr, $fmt:expr, $($arg:tt)*) => ({
      // context_code!(Code::Internal, "{:?}", 54)
      $crate::ContextCode::new($code, format!($fmt, $($arg)*))
    })
}

pub trait ErrorCode {
    /// Return the Tonic error code associated with an error.
    fn error_code(&self) -> tonic::Code;
}
