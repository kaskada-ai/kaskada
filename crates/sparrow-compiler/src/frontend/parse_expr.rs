use codespan_reporting::diagnostic::Label;
use itertools::Itertools;
use lalrpop_util::ParseError;
use sparrow_syntax::{Expr, ExprRef, FeatureSetPart, Token};

use crate::{DiagnosticBuilder, DiagnosticCode};

/// Parse the given string to an expression.
pub(super) fn parse_expr(
    part_id: FeatureSetPart,
    expr: &str,
) -> Result<ExprRef, Vec<DiagnosticBuilder>> {
    match Expr::try_from_str(part_id, expr) {
        Ok(expr) => Ok(expr),
        Err(errors) => Err(errors
            .into_iter()
            .map(|error| parse_error_to_diagnostic(part_id, error))
            .collect()),
    }
}

fn parse_error_to_diagnostic(
    part_id: FeatureSetPart,
    error: ParseError<usize, Token<'_>, (usize, String, usize)>,
) -> DiagnosticBuilder {
    match error {
        ParseError::InvalidToken { location } => DiagnosticCode::SyntaxError
            .builder()
            .with_label(Label::primary(part_id, location..location).with_message("Invalid token")),
        ParseError::UnrecognizedEof { location, expected } => {
            let diagnostic = DiagnosticCode::SyntaxError.builder().with_label(
                Label::primary(part_id, location..location).with_message("Unexpected EOF"),
            );

            if expected.is_empty() {
                diagnostic
            } else {
                diagnostic.with_note(format!("Expected {}", expected.iter().format(",")))
            }
        }
        ParseError::UnrecognizedToken { token, expected } => {
            let diagnostic = DiagnosticCode::SyntaxError.builder().with_label(
                Label::primary(part_id, token.0..token.2)
                    .with_message(format!("Invalid token '{}'", token.1)),
            );

            if expected.is_empty() {
                diagnostic
            } else {
                diagnostic.with_note(format!("Expected {}", expected.iter().format(", ")))
            }
        }
        ParseError::ExtraToken { token } => DiagnosticCode::SyntaxError.builder().with_label(
            Label::primary(part_id, token.0..token.2)
                .with_message(format!("Unexpected token '{}'", token.1)),
        ),
        ParseError::User { error } => DiagnosticCode::SyntaxError
            .builder()
            .with_label(Label::primary(part_id, error.0..error.2).with_message(error.1)),
    }
}
