use codespan_reporting::diagnostic::{Diagnostic, Severity};
use codespan_reporting::term::{self, Chars, Config, DisplayStyle, Styles};
use sparrow_api::kaskada::v1alpha::FeatureSet;
use sparrow_api::kaskada::v1alpha::FenlDiagnostic;
use sparrow_syntax::FeatureSetPart;
use tracing::{error, info, warn};

use crate::diagnostics::feature_set_parts::FeatureSetParts;
use crate::diagnostics::DiagnosticCode;
use crate::DiagnosticBuilder;

/// Collects the diagnostic messages being reported.
pub struct DiagnosticCollector<'a> {
    feature_set: FeatureSetParts<'a>,
    /// Collect the diagnostic messages.
    collected: Vec<CollectedDiagnostic>,
    config: Config,
}

impl<'a> std::fmt::Debug for DiagnosticCollector<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiagnosticCollector")
            .field("collected", &self.collected)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct CollectedDiagnostic {
    code: DiagnosticCode,
    pub formatted: String,
    pub message: String,
}

impl CollectedDiagnostic {
    pub fn severity(&self) -> Severity {
        self.code.severity()
    }

    pub fn is_error(&self) -> bool {
        self.code.severity() == Severity::Error
    }

    pub fn is_bug(&self) -> bool {
        self.code.severity() == Severity::Bug
    }
}

impl From<CollectedDiagnostic> for FenlDiagnostic {
    fn from(diagnostic: CollectedDiagnostic) -> Self {
        use sparrow_api::kaskada::v1alpha::Severity as ApiSeverity;
        let severity = match diagnostic.code.severity() {
            Severity::Bug => ApiSeverity::Error,
            Severity::Error => ApiSeverity::Error,
            Severity::Warning => ApiSeverity::Warning,
            Severity::Note => ApiSeverity::Note,
            Severity::Help => ApiSeverity::Note,
        } as i32;

        Self {
            severity,
            code: diagnostic.code.code_str().to_owned(),
            message: diagnostic.code.message().to_owned(),
            formatted: diagnostic.formatted,
            web_link: diagnostic.code.web_link().to_owned(),
        }
    }
}

impl std::fmt::Display for CollectedDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.formatted)
    }
}

impl<'a> DiagnosticCollector<'a> {
    pub fn new(feature_set: &'a FeatureSet) -> Self {
        Self {
            feature_set: FeatureSetParts::new(feature_set),
            collected: Vec::new(),
            config: Config {
                chars: Chars::ascii(),
                display_style: DisplayStyle::Rich,
                tab_width: 4,
                styles: Styles::default(),
                end_context_lines: 1,
                start_context_lines: 3,
            },
        }
    }

    pub fn collect_all(&mut self, builders: impl IntoIterator<Item = DiagnosticBuilder>) {
        for builder in builders {
            builder.emit(self)
        }
    }

    /// Add a diagnostic to the collector.
    ///
    /// This method *doesn't* return an error panic, even though it could fail
    /// to format the diagnostic in unexpected situations. In those cases it
    /// logs the diagnostic at the "error" level and inserts a generic
    /// diagnostic.
    ///
    /// This makes it easier to use from non-erroring code and ensures that we
    /// don't fail to report any diagnostics in this exceptional situation.
    pub(super) fn add_diagnostic(
        &mut self,
        code: DiagnosticCode,
        diagnostic: Diagnostic<FeatureSetPart>,
    ) {
        let mut buffer = termcolor::Buffer::no_color();

        if let Err(err) = term::emit(&mut buffer, &self.config, &self.feature_set, &diagnostic) {
            error!(
                "Unable to report diagnostic: {:?} due to {}",
                diagnostic, err
            );
            self.collected.push(CollectedDiagnostic {
                code: DiagnosticCode::FailedToReport,
                formatted: "Failed to report diagnostic".to_owned(),
                message: "Failed to report diagnostic".to_owned(),
            });
            return;
        };
        let message = diagnostic.message.clone();
        let formatted = match String::from_utf8(buffer.into_inner()) {
            Ok(formatted) => formatted,
            Err(err) => {
                error!(
                    "Unable to report diagnostic: {:?} due to {}",
                    diagnostic, err
                );
                self.collected.push(CollectedDiagnostic {
                    code: DiagnosticCode::FailedToReport,
                    formatted: "Failed to report diagnostic".to_owned(),
                    message,
                });
                return;
            }
        };

        let diagnostic = CollectedDiagnostic {
            code,
            formatted,
            message,
        };

        match code.severity() {
            Severity::Bug | Severity::Error => {
                warn!("Collecting fatal diagnostic:\n{}", diagnostic)
            }
            _ => {
                info!("Collecting non-fatal diagnostic:\n{}", diagnostic)
            }
        }
        self.collected.push(diagnostic);
    }

    /// Return the count of error and bug diagnostics.
    pub fn num_errors(&self) -> usize {
        // Ideally, we could just look at what the maximum diagnostic is,
        // but `Severity` doesn't currently implement `Ord`.
        // https://github.com/brendanzab/codespan/issues/335
        self.collected
            .iter()
            .filter(|d| matches!(d.severity(), Severity::Bug | Severity::Error))
            .count()
    }

    pub fn finish(self) -> Vec<CollectedDiagnostic> {
        self.collected
    }
}

#[cfg(test)]
mod tests {
    use codespan_reporting::diagnostic::Label;
    use itertools::Itertools;
    use sparrow_api::kaskada::v1alpha::{FeatureSet, Formula};

    use super::*;
    use crate::diagnostics::code::DiagnosticCode;

    fn feature_set_fixture() -> FeatureSet {
        FeatureSet {
            formulas: vec![
                Formula {
                    name: "a".to_owned(),
                    formula: "Foo.x + 10".to_owned(),
                    source_location: "".to_owned(),
                },
                Formula {
                    name: "b".to_owned(),
                    formula: "6 + 10".to_owned(),
                    source_location: "".to_owned(),
                },
                Formula {
                    name: "c".to_owned(),
                    formula: "let sum = a + b\nin sum + 5".to_owned(),
                    source_location: "ViewFoo".to_owned(),
                },
            ],
            query: "let foo = a\nlet bar = b\nin { foo, bar, baz }".to_owned(),
        }
    }

    #[test]
    fn test_render_diagnostic_macro() {
        let feature_set = feature_set_fixture();
        let mut collector = DiagnosticCollector::new(&feature_set);

        DiagnosticCode::IllegalFieldRef
            .builder()
            .with_note("Valid fields: y, z".to_owned())
            .with_label(
                Label::primary(FeatureSetPart::Formula(0), 3..5)
                    .with_message("Field 'x' doesn't exist"),
            )
            .emit(&mut collector);

        let diagnostics = format!("{}", collector.finish().iter().format("\n"));
        insta::assert_snapshot!(diagnostics, @r###"
        error[E0001]: Illegal field reference
          --> 'Formula: a':1:4
          |
        1 | Foo.x + 10
          |    ^^ Field 'x' doesn't exist
          |
          = Valid fields: y, z
        "###);
    }
}
