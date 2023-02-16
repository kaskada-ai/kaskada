use std::collections::BTreeSet;

use hashbrown::HashSet;
use sparrow_syntax::{ExprOp, Located, Resolved, ResolvedExpr};

use crate::{DiagnosticCode, DiagnosticCollector};

/// Return the names (identifiers) of variables which are free in `expr`.
///
/// Reports unused bindings as diagnostics.
///
/// TODO: Could return `BTreeSet` to preserve ordering.
pub(super) fn free_variables(
    expr: &ResolvedExpr,
    diagnostics: &mut DiagnosticCollector<'_>,
) -> HashSet<String> {
    analysis(expr, diagnostics)
        .into_iter()
        .map(|s| s.to_owned())
        .collect()
}

/// Analyzes the individual arguments and unions their free sets.
fn analyze_args<'a>(
    args: &'a Resolved<Located<Box<ResolvedExpr>>>,
    diagnostics: &mut DiagnosticCollector<'_>,
) -> BTreeSet<&'a str> {
    args.iter()
        .map(|arg| analysis(arg.inner(), diagnostics))
        .reduce(|mut a, mut b| {
            a.append(&mut b);
            a
        })
        // Should never be `None` since we always expect non-zero amount of arguments.
        .unwrap_or_default()
}

/// Recursively apply the analysis to `expr`.
fn analysis<'a>(
    expr: &'a ResolvedExpr,
    diagnostics: &mut DiagnosticCollector<'_>,
) -> BTreeSet<&'a str> {
    match expr.op() {
        ExprOp::Literal(_) => BTreeSet::new(),
        ExprOp::Reference(ident) => {
            let mut free = BTreeSet::new();
            free.insert(ident.inner().as_str());
            free
        }
        ExprOp::Pipe(location) => {
            // 1. Analyze the rhs
            let mut free = analysis(expr.args()[1].inner(), diagnostics);

            // 2. If the rhs does not contain "$input", report a warning
            if !free.remove("$input") {
                DiagnosticCode::UnusedBinding
                    .builder()
                    .with_label(
                        location
                            .primary_label()
                            .with_message("Left-hand side of pipe not used"),
                    )
                    .emit(diagnostics);
            };

            // 3. Add free variables from the lhs
            free.append(&mut analysis(expr.args()[0].inner(), diagnostics));
            free
        }
        ExprOp::Let(names, _) => {
            // Can add `anyhow::Result` instead of unwrapping here, but
            // it requires a larger refactoring upstream, plus we don't
            // expect this to fail.
            let (body, bindings) = expr.args().values().split_last().unwrap();
            let free = analysis(body.inner(), diagnostics);
            bindings
                .iter()
                .enumerate()
                .rev()
                .fold(free, |mut free, (index, value)| {
                    let name: &Located<String> = &names[index];

                    // 1. Remove this binding from the free set. Report an unused
                    //    error if it wasn't there.
                    if !free.remove(name.inner().as_str()) {
                        DiagnosticCode::UnusedBinding
                            .builder()
                            .with_label(
                                name.location()
                                    .primary_label()
                                    .with_message(format!("Unused binding '{}'", name.inner())),
                            )
                            .emit(diagnostics);
                    };

                    // 2. Add things that are free in the value definition.
                    free.append(&mut analysis(value.inner(), diagnostics));
                    free
                })
        }

        ExprOp::SelectFields(_)
        | ExprOp::RemoveFields(_)
        | ExprOp::FieldRef(_, _)
        | ExprOp::Cast(_, _)
        | ExprOp::Call(_)
        | ExprOp::Record(_, _)
        | ExprOp::ExtendRecord(_) => analyze_args(expr.args(), diagnostics),
        ExprOp::Error => {
            // Assume that sub-expressions we couldn't parse didn't
            // reference anything. This shouldn't cause  *more* errors,
            // because anything that references *this* formula could still
            // be placed *after* this.
            BTreeSet::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use sparrow_api::kaskada::v1alpha::FeatureSet;
    use sparrow_syntax::{Expr, FeatureSetPart};

    use super::*;
    use crate::resolve_arguments::resolve_recursive;

    fn feature_set_fixture(input: &'static str) -> FeatureSet {
        FeatureSet {
            formulas: vec![],
            query: input.to_owned(),
        }
    }

    fn test_free_variables(input: &'static str) -> Vec<String> {
        let part_id = FeatureSetPart::Internal(input);
        let expr = Expr::try_from_str(part_id, input).unwrap();
        let mut diagnostic_builder = Vec::new();
        let expr = resolve_recursive(&expr, &mut diagnostic_builder).unwrap();

        let feature_set = FeatureSet::default();
        let mut diagnostics = DiagnosticCollector::new(&feature_set);

        let free_variables = free_variables(&expr, &mut diagnostics);
        assert_eq!(diagnostics.num_errors(), 0, "Expected no warnings");

        free_variables.into_iter().sorted_unstable().collect()
    }

    fn test_free_variables_with_diagnostics(
        input: &'static str,
        diagnostics: &mut DiagnosticCollector<'_>,
    ) -> Vec<String> {
        let part_id = FeatureSetPart::Internal(input);
        let expr = Expr::try_from_str(part_id, input).unwrap();
        let mut diagnostic_builder = Vec::new();
        let expr = resolve_recursive(&expr, &mut diagnostic_builder).unwrap();

        let free_variables = free_variables(&expr, diagnostics);
        free_variables.into_iter().sorted_unstable().collect()
    }

    #[test]
    fn test_free_names() {
        assert_eq!(
            test_free_variables("Foo + Bar.x"),
            vec!["Bar".to_string(), "Foo".to_string()]
        );
    }

    #[test]
    fn test_free_pipe_explicit_input() {
        assert_eq!(
            test_free_variables("Foo.x | sum($input)"),
            vec!["Foo".to_string()]
        );
    }

    #[test]
    fn test_free_pipe_implicit_input() {
        assert_eq!(
            test_free_variables("Foo.x | sum()"),
            vec!["Foo".to_string()]
        );
    }

    #[test]
    fn test_free_multiple() {
        assert_eq!(
            test_free_variables("Foo.x + Foo.y"),
            vec!["Foo".to_string()]
        );
    }

    #[test]
    fn test_free_let_bound_shadow() {
        assert_eq!(
            test_free_variables("let Foo = { x: 8 } in Foo.x + Bar.y"),
            vec!["Bar".to_string()]
        );
    }

    #[test]
    fn test_free_let_binding() {
        assert_eq!(
            test_free_variables("Foo.x + (let Foo = { x: 8 } in Foo.x + Bar.y)"),
            vec!["Bar".to_string(), "Foo".to_string()]
        );
    }

    #[test]
    fn test_free_pipe_extend() {
        assert_eq!(
            test_free_variables("Foo.x | extend({ a: Foo.y })"),
            vec!["Foo".to_string()]
        );
    }

    #[test]
    fn test_free_record_multiple_fields() {
        assert_eq!(
            test_free_variables(
                "{ time: Foo.time, key: Foo.key, max: Foo.n | max(), min: Foo.n | min() }"
            ),
            vec!["Foo".to_string()]
        );
    }

    #[test]
    fn test_free_missing_input_pipe() {
        let input = "Foo.x | sum(Foo.y)";
        let feature_set = feature_set_fixture(input);
        let mut diagnostics = DiagnosticCollector::new(&feature_set);

        let free = test_free_variables_with_diagnostics(input, &mut diagnostics);
        assert_eq!(free, vec!["Foo".to_string()]);

        let collected = diagnostics.finish();
        assert_eq!(collected.len(), 1, "expected 1 warning");
        assert_eq!(format!("{}", collected[0]), "warning[W2001]: Unused binding\n  --> internal:1:7\n  |\n1 | Foo.x | sum(Foo.y)\n  |       ^ Left-hand side of pipe not used\n\n");
    }

    #[test]
    fn test_free_missing_input_let_pipe() {
        let input = "let foo = Foo.x | sum(Foo.y) in { bar: Bar.x }";
        let feature_set = feature_set_fixture(input);
        let mut diagnostics = DiagnosticCollector::new(&feature_set);

        let free = test_free_variables_with_diagnostics(input, &mut diagnostics);
        assert_eq!(free, vec!["Bar".to_string(), "Foo".to_string()]);

        let collected = diagnostics.finish();
        assert_eq!(collected.len(), 2, "expected 1 warning");
        assert_eq!(
            format!("{}", collected[0]),
            "warning[W2001]: Unused binding\n  --> internal:1:5\n  |\n1 | let foo = Foo.x | \
             sum(Foo.y) in { bar: Bar.x }\n  |     ^^^ Unused binding 'foo'\n\n"
        );
        assert_eq!(
            format!("{}", collected[1]),
            "warning[W2001]: Unused binding\n  --> internal:1:17\n  |\n1 | let foo = Foo.x | \
             sum(Foo.y) in { bar: Bar.x }\n  |                 ^ Left-hand side of pipe not \
             used\n\n"
        );
    }
}
