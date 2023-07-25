//! Define the diagnostic codes and associated messages.
//!
//! Each diagnostic code is associated with a code and a message string.
//!
//! When reporting the diagnostics, the [super::DiagnosticBuilder] should
//! be configured to report additional

use crate::diagnostics::DiagnosticBuilder;

/// Macro for registering the diagonstic codes.
macro_rules! register_diagnostics {
    ( $($name:ident ( $code:ident, $severity:ident, $message:expr, $web_link:expr ), )* ) => (
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum DiagnosticCode {
            $( $name ),*
        }

        impl DiagnosticCode {
            pub fn code_str(&self) -> &'static str {
                match &self {
                    $( Self::$name => stringify!($code) ),*
                }
            }

            pub fn message(&self) -> &'static str {
                match &self {
                    $( Self::$name => $message ),*
                }
            }

            pub fn severity(&self) -> codespan_reporting::diagnostic::Severity {
                match &self {
                    $( Self::$name => codespan_reporting::diagnostic::Severity::$severity ),*
                }
            }

            pub fn web_link(&self) -> &'static str {
                match &self {
                    $( Self::$name => $web_link),*
                }
            }
        }
    );
}

// Define the actual diagnostic codes.
register_diagnostics! {
// Errors: 0000 - 0999
IllegalFieldRef(E0001, Error, "Illegal field reference", ""),
IllegalCast(E0002, Error, "Illegal cast", ""),
IllegalIdentifier(E0003, Error, "Illegal identifier", ""),
FormulaAlreadyDefined(E0004, Error, "Formula already defined", ""),
// TableAlreadyDefined(E0005, Error, "Table already defined"),
UnboundReference(E0006, Error, "Unbound reference", ""),
UndefinedFunction(E0007, Error, "Undefined function", ""),
InvalidArguments(E0008, Error, "Invalid arguments", ""),
DuplicateFieldNames(E0009, Error, "Duplicate field names in record expression", ""),
InvalidArgumentType(E0010, Error, "Invalid argument type(s)", ""),
SyntaxError(E0011, Error, "Invalid syntax", ""),
CyclicReference(E0012, Error, "Circular dependency", ""),
InvalidOutputType(E0013, Error, "Invalid output type", "https://kaskada.io/docs-site/kaskada/main/fenl/fenl-diagnostic-codes.html#e0013"),
InvalidNonConstArgument(E0014, Error, "Invalid non-constant argument", ""),
IncompatibleArgumentTypes(E0015, Error, "Incompatible argument types", ""),

// Bugs: 1000 - 1999
InternalError(B1000, Bug, "Internal error", ""),
FailedToReport(B1001, Bug, "Failed to report diagnostic", ""),

// Compile Time Warnings: 2000 - 2999
IncompatibleTimeDomains(W2000, Warning, "Incompatible time domains", ""),
UnusedBinding(W2001, Warning, "Unused binding", ""),

// NOTE: While the code (W2001) indicates a warning, this is currently an error.
// This reflects the fact that we can't currently schedule nodes that occur
// with multiple groupings.
// TODO: Change scheduling to support the same node being in multiple groups,
// and then convert this back to a Warning.
IncompatibleGrouping(W2001, Error, "Incompatible grouping", ""),
}

impl DiagnosticCode {
    pub(crate) fn builder(self) -> DiagnosticBuilder {
        DiagnosticBuilder::new(self, self.severity())
    }
}

#[test]
fn test() {
    assert_eq!(DiagnosticCode::IllegalFieldRef.code_str(), "E0001");
    assert!(DiagnosticCode::IllegalFieldRef
        .message()
        .contains("Illegal field reference"));
}
