//! Definition, collection, and reporting of diagnostic messages.
//!
//! Diagnostic messages are messages to the user about the Fenl expressions
//! they have written. They have a variety of severities as indicated by
//! [codespan_reporting::diagnostic::Severity]. Diagnostic messages should
//! be reported against specific positions in the expressions the user wrote,
//! with supporting information as appropriate.
//!
//! A message is created using the [DiagnosticBuilder] and then emited to
//! the [DiagnosticCollector] associated with the current compilation.

mod builder;
mod code;
mod collector;
mod feature_set_parts;

pub(crate) use builder::*;
pub(crate) use code::*;
pub use collector::*;
