use codespan_reporting::diagnostic::{Diagnostic, Label, Severity};
use sparrow_syntax::FeatureSetPart;

use crate::diagnostics::code::DiagnosticCode;
use crate::diagnostics::collector::DiagnosticCollector;

/// Builder for creating and emitting a diagnostic.
#[must_use]
#[derive(Debug, PartialEq)]
pub struct DiagnosticBuilder {
    code: DiagnosticCode,
    diagnostic: Diagnostic<FeatureSetPart>,
}

impl DiagnosticBuilder {
    /// Create a new `DiagnosticBuilder` for the given code.
    ///
    /// The message (and severity) are determined by the code.
    /// Additional notes and at least one label should be added before
    /// calling [Self::emit].
    pub(crate) fn new(code: DiagnosticCode, severity: Severity) -> Self {
        let diagnostic = Diagnostic::new(severity)
            .with_code(code.code_str())
            .with_message(code.message());

        Self { code, diagnostic }
    }

    /// Add a note to the diagnostic. See [Self::with_notes] for details.
    pub(crate) fn with_note(self, note: String) -> Self {
        self.with_notes(vec![note])
    }

    /// Add notes to the diagnostic.
    ///
    /// Each note may be one or more lines providing additional information
    /// about the diagnostic, such as the actual and expected types or the
    /// fields that were actually available.
    ///
    /// If the note can be reasonably associated with a block of code, it
    /// should instead be reported as a secondary label so that the snippet
    /// is indicated.
    pub(crate) fn with_notes(mut self, notes: Vec<String>) -> Self {
        self.diagnostic = self.diagnostic.with_notes(notes);
        self
    }

    /// Add a label to the diagnostic. See [Self::with_labels] for details.
    ///
    /// ```rust,no_run
    /// DiagnosticCode::IncompatibleTimeDomains.builder()
    ///     .with_label(location.primary_label().with_message(message))
    /// ```
    pub(crate) fn with_label(self, label: Label<FeatureSetPart>) -> Self {
        self.with_labels(vec![label])
    }

    /// Adds labels to the diagnostic.
    ///
    /// Each label is associated with a specific point in the code and should
    /// not contain line breaks.
    ///
    /// The order that labels are added does not matter -- they will be shown in
    /// the order the referenced code appeared. The primary label(s) should
    /// be used for actual problem, while secondary label(s) should be used
    /// to provide additional information about specific parts of the code
    /// contributing to the error.
    pub(crate) fn with_labels(mut self, labels: Vec<Label<FeatureSetPart>>) -> Self {
        self.diagnostic = self.diagnostic.with_labels(labels);
        self
    }

    pub(crate) fn emit(self, collector: &mut DiagnosticCollector<'_>) {
        collector.add_diagnostic(self.code, self.diagnostic)
    }
}
