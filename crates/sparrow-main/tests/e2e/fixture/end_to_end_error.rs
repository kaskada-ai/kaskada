use itertools::Itertools;
use serde::ser::SerializeStruct;
use serde::Serialize;
use sparrow_api::kaskada::v1alpha::{FenlDiagnostic, FenlDiagnostics, Severity};
use sparrow_main::IntoStatus;

/// A struct representing the errors of end to end tests.
///
/// These are designed to be used with `insta::assert_yaml_snapshot!(...)`.
/// The diagnostics are specially serialized for use with that method.
#[derive(Debug, Serialize)]
pub(crate) struct EndToEndError {
    code: String,
    message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    fenl_diagnostics: Vec<AdaptedDiagnostic>,
}

impl std::fmt::Display for EndToEndError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {}\n{}",
            self.code,
            self.message,
            self.fenl_diagnostics.iter().format("\n")
        )
    }
}

#[allow(clippy::print_stdout)]
fn print_test_error(err: &anyhow::Error) {
    println!("TEST ERROR: {err:?}");
    err.chain()
        .skip(1)
        .for_each(|cause| println!("because: {cause:?}"));
}

impl From<FenlDiagnostics> for EndToEndError {
    fn from(diagnostics: FenlDiagnostics) -> Self {
        let message = format!(
            "{} errors in Fenl statements; see diagnostics",
            diagnostics.num_errors
        );
        let fenl_diagnostics = diagnostics
            .fenl_diagnostics
            .into_iter()
            .map(AdaptedDiagnostic)
            .collect();
        EndToEndError {
            code: "Client specified an invalid argument".to_owned(),
            message,
            fenl_diagnostics,
        }
    }
}

impl From<anyhow::Error> for EndToEndError {
    fn from(err: anyhow::Error) -> Self {
        // Print information about the error before converting it to a status.
        // The status conversion will lose the backtrace.
        print_test_error(&err);

        let status = err.into_status();

        EndToEndError {
            code: status.code().description().to_owned(),
            message: status.message().to_owned(),
            fenl_diagnostics: vec![],
        }
    }
}

impl<C> From<error_stack::Report<C>> for EndToEndError
where
    C: error_stack::Context + sparrow_core::ErrorCode,
{
    fn from(err: error_stack::Report<C>) -> Self {
        let status = err.into_status();
        EndToEndError {
            code: status.code().description().to_owned(),
            message: status.message().to_owned(),
            fenl_diagnostics: vec![],
        }
    }
}

/// Wrapper around a FenlDiagnostic allowing us to provide a custom serializer.
///
/// We use this to treat the formatted string as a list of strings, which causes
/// them to print as separate lines. This makes it easier to interpret the test
/// results.
#[derive(Debug)]
struct AdaptedDiagnostic(FenlDiagnostic);

impl std::fmt::Display for AdaptedDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.formatted)
    }
}

struct FormattedDiagnostic<'a>(&'a str);

impl<'a> Serialize for FormattedDiagnostic<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_seq(self.0.split('\n'))
    }
}

impl Serialize for AdaptedDiagnostic {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_struct("AdaptedDiagnostic", 5)?;

        let severity_str =
            match Severity::from_i32(self.0.severity).unwrap_or(Severity::Unspecified) {
                Severity::Unspecified => "unspecified",
                Severity::Error => "error",
                Severity::Warning => "warning",
                Severity::Note => "note",
                Severity::Bug => "bug",
                Severity::Help => "help",
            };

        s.serialize_field("severity", severity_str)?;
        s.serialize_field("code", &self.0.code)?;
        s.serialize_field("message", &self.0.message)?;
        s.serialize_field("formatted", &FormattedDiagnostic(&self.0.formatted))?;

        s.end()
    }
}
