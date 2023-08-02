use std::borrow::Cow;

use sparrow_syntax::{
    Arguments, Expr, ExprOp, ExprRef, Located, ResolveError, Resolved, ResolvedExpr,
};
use static_init::dynamic;

use crate::{DiagnosticBuilder, DiagnosticCode};

#[dynamic]
pub(crate) static FIELD_REF_ARGUMENTS: [Located<String>; 1] = [Located::internal_string("record")];

#[dynamic]
static PIPE_ARGUMENTS: [Located<String>; 2] = [
    Located::internal_string("lhs"),
    Located::internal_string("rhs"),
];

#[dynamic]
static RECORD_EXTENSION_ARGUMENTS: [Located<String>; 2] = [
    Located::internal_string("extension"),
    Located::internal_string("base"),
];

#[dynamic]
static SELECT_REMOVE_FIELDS_ARGUMENTS: [Located<String>; 2] = [
    Located::internal_string("record"),
    Located::internal_string("fields"),
];

#[dynamic]
static CAST_ARGUMENTS: [Located<String>; 1] = [Located::internal_string("input")];

/// Recursively resolves the arguments to the given operator and
/// all sub-expressions.
///
/// # Arguments
/// * `expr` - The expression to resolve.
/// * `diagnostics` - Mutable vec of diagnostics. New diagnostics are added
///   here.
///
/// # Returns
/// The resolved expression. If the expression failed to resolve, the result
/// should be of an `ExprOp::Error` type with no `args`.
pub(crate) fn resolve_recursive(
    expr: &ExprRef,
    diagnostics: &mut Vec<DiagnosticBuilder>,
) -> anyhow::Result<ResolvedExpr> {
    // Resolve first -- this takes the reference to arguments we have and gives
    // us back an owned `Resolved<ExprRef>`.
    match resolve_arguments(expr.op(), expr.args()) {
        Ok(args) => {
            let args = args.try_transform(|arg| -> anyhow::Result<_> {
                let resolved_arg = resolve_recursive(arg.inner(), diagnostics)?;
                Ok(arg.with_value(Box::new(resolved_arg)))
            })?;
            Ok(ResolvedExpr {
                op: expr.op().clone(),
                args,
            })
        }
        Err(diagnostic) => {
            if let Some(diagnostic) = diagnostic {
                diagnostics.push(diagnostic);
            }
            Ok(ResolvedExpr {
                op: ExprOp::Error,
                args: Resolved::empty(),
            })
        }
    }
}

fn resolve_arguments(
    op: &ExprOp,
    arguments: &Arguments<ExprRef>,
) -> Result<Resolved<Located<ExprRef>>, Option<DiagnosticBuilder>> {
    let (operator_location, names, defaults, vararg): (
        _,
        Cow<'static, [Located<String>]>,
        Option<_>,
        bool,
    ) = match op {
        ExprOp::Literal(literal) => (literal.location(), Cow::Borrowed(&[]), None, false),
        ExprOp::Reference(reference) => (reference.location(), Cow::Borrowed(&[]), None, false),
        ExprOp::FieldRef(_, location) => {
            (location, Cow::Borrowed(&*FIELD_REF_ARGUMENTS), None, false)
        }
        ExprOp::Call(function_name) => match crate::functions::get_function(function_name) {
            Ok(function) => {
                let parameters = function.signature().parameters();
                (
                    function_name.location(),
                    Cow::Borrowed(parameters.names()),
                    Some(parameters.defaults()),
                    parameters.has_vararg,
                )
            }
            Err(candidates) => {
                let diagnostic = DiagnosticCode::UndefinedFunction
                    .builder()
                    .with_label(
                        function_name
                            .location()
                            .primary_label()
                            .with_message(format!("No function named '{function_name}'")),
                    )
                    .with_note(format!("Nearest matches: {candidates}"));
                return Err(Some(diagnostic));
            }
        },
        ExprOp::Pipe(location) => (location, Cow::Borrowed(&*PIPE_ARGUMENTS), None, false),
        ExprOp::Let(names, location) => (location, Cow::Owned(names.to_vec()), None, false),
        ExprOp::Record(names, location) => (location, Cow::Owned(names.to_vec()), None, false),
        ExprOp::ExtendRecord(location) => (
            location,
            Cow::Borrowed(&*RECORD_EXTENSION_ARGUMENTS),
            None,
            false,
        ),
        ExprOp::RemoveFields(location) => (
            location,
            Cow::Borrowed(&*SELECT_REMOVE_FIELDS_ARGUMENTS),
            None,
            true,
        ),
        ExprOp::SelectFields(location) => (
            location,
            Cow::Borrowed(&*SELECT_REMOVE_FIELDS_ARGUMENTS),
            None,
            true,
        ),
        ExprOp::Cast(_, location) => (location, Cow::Borrowed(&*CAST_ARGUMENTS), None, false),
        ExprOp::Error => return Err(None),
    };

    let input = Expr::implicit_input();
    let input = Located::internal_str("$input").with_value(input.clone());

    let result = if let Some(defaults) = defaults {
        // `names` are wrapped in `Cow`, meaning the cost of clone is low for static
        // references. The only real copies are done for user-defined let names
        // and record fields
        arguments.resolve(names.clone(), defaults, &input, vararg)
    } else {
        let defaults = vec![None; names.len()];
        arguments.resolve(names.clone(), &defaults, &input, vararg)
    };

    result.map_err(|err| match err {
        ResolveError::Internal(message) => Some(
            DiagnosticCode::InternalError
                .builder()
                .with_label(operator_location.primary_label().with_message(message)),
        ),
        ResolveError::PositionalAfterKeyword {
            keyword,
            positional,
        } => Some(
            DiagnosticCode::InvalidArguments
                .builder()
                .with_label(
                    positional
                        .primary_label()
                        .with_message("positional appears after keyword"),
                )
                .with_label(
                    keyword
                        .secondary_label()
                        .with_message("first keyword was here"),
                ),
        ),
        ResolveError::DuplicateKeywordAndPositional {
            keyword,
            positional,
        } => Some(
            DiagnosticCode::InvalidArguments
                .builder()
                .with_label(
                    positional
                        .primary_label()
                        .with_message("positional occurrence"),
                )
                .with_label(keyword.secondary_label().with_message("keyword was here")),
        ),
        ResolveError::TooManyArguments {
            expected,
            actual,
            first_unexpected,
        } => Some(
            DiagnosticCode::InvalidArguments
                .builder()
                .with_label(
                    first_unexpected
                        .primary_label()
                        .with_message("First unexpected argument"),
                )
                .with_note(format!("Expected {expected} but got {actual}")),
        ),
        ResolveError::NotEnoughArguments { expected, actual } => Some(
            DiagnosticCode::InvalidArguments
                .builder()
                .with_label(
                    operator_location
                        .primary_label()
                        .with_message("First unexpected argument"),
                )
                .with_note(format!("Expected {expected} but got {actual}")),
        ),
        ResolveError::PositionalArgumentForOptionalParam {
            parameter,
            positional,
        } => Some(DiagnosticCode::InvalidArguments.builder().with_label(
            positional.primary_label().with_message(format!(
                "Parameter '{parameter}' with default value must be keyword argument, but was positional",

            )),
        )),
        ResolveError::InvalidKeywordArgument { keyword } => {
            let nearest = crate::nearest_matches::NearestMatches::new_nearest_strs(
                keyword.inner(),
                names.iter().map(|s| s.inner().as_str()),
            );
            Some(
                DiagnosticCode::InvalidArguments
                    .builder()
                    .with_label(operator_location.primary_label())
                    .with_note(format!("Nearest matches: {nearest}")),
            )
        }
    })
}

#[cfg(test)]
mod tests {
    use sparrow_syntax::{ExprOp, FeatureSetPart};

    use super::resolve_recursive;
    use crate::frontend::parse_expr::parse_expr;

    // Asserts the expression recursively resolves sucessfully with no diagnostics.
    fn assert_successful_resolve(expr: &'_ str) {
        let mut diagnostics = Vec::new();
        let expr = match parse_expr(FeatureSetPart::Query, expr) {
            Ok(expr) => expr,
            Err(diagnostics) => panic!("Expected success, found errors {diagnostics:?}"),
        };
        match resolve_recursive(&expr, &mut diagnostics) {
            Ok(_) => (),
            Err(e) => panic!("Expected success, found errors: {e:?}"),
        }
        assert!(
            diagnostics.is_empty(),
            "{}",
            format!("Expected no diagnostics, saw {diagnostics:?}")
        );
    }

    #[test]
    fn test_recursive_resolve_basic() {
        let expr = "Foo + sum(Foo.a)";
        assert_successful_resolve(expr)
    }

    #[test]
    fn test_recursive_resolve_with_single_let() {
        let expr = "let amt = Foo.amount in { feature: amt }";
        assert_successful_resolve(expr)
    }

    #[test]
    fn test_recursive_resolve_with_pipe() {
        let expr = "Foo | $input + sum(Foo.m)";
        assert_successful_resolve(expr)
    }

    #[test]
    fn test_recursive_resolve_resolves_implicit_input() {
        // Tests that the implicit $input is resolved as an argument
        let expr = "{ pipe: Foo | sum() }";
        let mut diagnostics = Vec::new();
        let expr = match parse_expr(FeatureSetPart::Query, expr) {
            Ok(expr) => expr,
            Err(diagnostics) => panic!("Expected success, found errors {diagnostics:?}"),
        };
        let resolved = match resolve_recursive(&expr, &mut diagnostics) {
            Ok(resolved) => resolved,
            Err(e) => panic!("Expected success, found errors: {e:?}"),
        };
        assert!(
            diagnostics.is_empty(),
            "{}",
            format!("Expected no diagnostics, saw {diagnostics:?}")
        );

        // Record
        match resolved.op() {
            ExprOp::Record(names, _) => assert_eq!(names[0].inner(), "pipe"),
            _ => panic!("Expected record"),
        }

        // Record arg should be a pipe
        let args = resolved.args().values();
        assert_eq!(args.len(), 1);
        let pipe = args[0].inner();
        match pipe.op() {
            ExprOp::Pipe(_) => (),
            _ => panic!("Expected pipe"),
        }

        // Pipe should have two args, where rhs is the sum()
        let args = pipe.args().values();
        assert_eq!(args.len(), 2);
        match args[1].inner().op() {
            ExprOp::Call(call) => assert_eq!(call.inner(), "sum"),
            _ => panic!("expected call"),
        };

        // Sum should have two args: [input, window].
        let sum_arg = args[1].inner().args();
        assert_eq!(sum_arg.len(), 2);
        let input = sum_arg[0].inner();
        match input.op() {
            ExprOp::Reference(str) => assert_eq!(str.inner(), "$input"),
            _ => panic!("expected ref"),
        }
    }
}
