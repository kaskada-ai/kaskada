//! Defines the simplification rules applied to the DFG.

use std::time::Duration;

use egg::{rewrite, Applier, Id, Rewrite, Runner, Subst};
use smallvec::smallvec;
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_syntax::LiteralValue;
use static_init::dynamic;
use tracing::{info, info_span, warn};

use super::DfgAnalysis;
use crate::dfg::{DfgGraph, DfgLang, Expression, StepKind};
use crate::CompilerOptions;

#[dynamic]
static RULES: Vec<Rewrite<DfgLang, DfgAnalysis>> = vec![
    // Rules of negation
    rewrite!("neg-double"; "(neg (neg ?x ?op) ?op)" => "?x)"),
    rewrite!("neg-zero"; "(neg ?zero ?op)" => "?zero" if is_literal_satisfying("?zero", |s| s.is_zero())),
    // Rules of logical negation
    rewrite!("not-double"; "(not (not ?x ?op) ?op)" => "?x"),
    rewrite!("not-true"; "(not true ?op)" =>  { Literal::new(LiteralValue::False, "?op") }),
    rewrite!("not-false"; "(not false ?op)" => { Literal::new(LiteralValue::True, "?op") }),
    // Rules of addition
    rewrite!("add-commute"; "(add ?x ?y ?op)" => "(add ?y ?x ?op)"  if is_not_literal(vec!["?y"])),
    rewrite!("add-zero"; "(add ?x ?zero ?op)" => "(transform ?x ?op)" if is_literal_satisfying("?zero", |s| s.is_zero())),
    rewrite!("add-assoc"; "(add ?x (add ?y ?z ?op) ?op)" => "(add (add ?x ?y ?op) ?z ?op)"  if is_not_literal(vec!["?y", "?z"])),
    rewrite!("add-cancel"; "(add ?x (neg ?x ?op) ?op)" => { Literal::new(LiteralValue::Number("0".to_owned()), "?op") }  if is_not_literal(vec!["?x"])),
    // Rules of subtraction
    rewrite!("sub-canon"; "(sub ?x ?y ?op)" => "(add ?x (neg ?y ?op) ?op)"),
    rewrite!("sub-cancel"; "(sub ?x ?x ?op)" => { Literal::new(LiteralValue::Number("0".to_owned()), "?op") }),
    // Rules of subtraction
    rewrite!("mul-commute"; "(mul ?x ?y ?op)" => "(mul ?y ?x ?op)" if is_not_literal(vec!["?y"])),
    rewrite!("mul-assoc"; "(mul ?x (mul ?y ?z ?op) ?op)" => "(mul (mul ?x ?y ?op) ?z ?op)" if is_not_literal(vec!["?y", "?z"])),
    rewrite!("mul-zero"; "(mul ?x ?zero ?op)" => { Literal::new(LiteralValue::Number("0".to_owned()), "?op") } if is_literal_satisfying("?zero", |s| s.is_zero())),
    rewrite!("mul-one"; "(mul ?x ?one ?op)" => "?x" if is_literal_satisfying("?one", |s| s.is_one())),
    rewrite!("mul-neg-one"; "(mul ?x ?neg_one ?op)" => "(neg ?x ?op)" if is_literal_satisfying("?neg_one", |s| s.is_neg_one())),
    // Rules of division
    rewrite!("div-one"; "(div ?x ?one ?op)" => "?x" if is_literal_satisfying("?one", |s| s.is_one())),
    rewrite!("div-cancel"; "(div ?x ?x ?op)" => { Literal::new(LiteralValue::Number("1".to_owned()), "?op") } if is_literal_satisfying("?zero", |s| !s.is_zero())),
    // Rules of logical or
    rewrite!("or-commute"; "(logical_or ?x ?y ?op)" => "(logical_or ?y ?x ?op)" if is_not_literal(vec!["?y"])),
    rewrite!("or-reflexive"; "(logical_or ?x ?x ?op)" => "?x"),
    rewrite!("or-not-reflexive"; "(logical_or ?x (not ?x ?op) ?op)" => "(if (is_valid ?x ?op) (true ?op) ?op)"),
    rewrite!("or-identity"; "(logical_or ?x ?false ?op)" => "?x" if is_literal_satisfying("?false", |s| s.is_false())),
    rewrite!("or-annihilate"; "(logical_or ?x ?true ?op)" => { Literal::new(LiteralValue::True, "?op") } if is_literal_satisfying("?true", |s| s.is_true())),
    rewrite!("or-assoc"; "(logical_or ?x (logical_or ?y ?z ?op) ?op)" => "(logical_or (logical_or ?x ?y ?op) ?z ?op)" if is_not_literal(vec!["?y", "?z"])),
    rewrite!("or-distribute"; "(logical_or ?x (logical_and ?y ?z ?op) ?op)" => "(logical_and (logical_or ?x ?y ?op) (logical_or ?x ?z ?op) ?op)"),
    rewrite!("or-absorb";  "(logical_or ?x (logical_and ?x ?y ?op) ?op)" => "?x"),
    rewrite!("or-demorgan"; "(not (logical_or ?x ?y ?op) ?op)" => "(logical_and (not ?x ?op) (not ?y ?op) ?op)"),
    // Rules of logical and
    rewrite!("and-commute"; "(logical_and ?x ?y ?op)" => "(logical_and ?y ?x ?op)" if is_not_literal(vec!["?y"])),
    rewrite!("and-reflexive"; "(logical_and ?x ?x ?op)" => "?x"),
    rewrite!("and-not-reflexive"; "(logical_and ?x (not ?x ?op) ?op)" => "(if (is_valid ?x ?op) (false ?op) ?op)"),
    rewrite!("and-annihilate"; "(logical_and ?x ?false ?op)" => { Literal::new(LiteralValue::False, "?op") } if is_literal_satisfying("?false", |s| s.is_false())),
    rewrite!("and-identity"; "(logical_and ?x ?true ?op)" => "?x" if is_literal_satisfying("?true", |s| s.is_true())),
    rewrite!("and-assoc"; "(logical_and ?x (logical_and ?y ?z ?op) ?op)" => "(logical_and (logical_and ?x ?y ?op) ?z ?op)" if is_not_literal(vec!["?y", "?z"])),
    rewrite!("and-distribute"; "(logical_and ?x (logical_or ?y ?z ?op) ?op)" => "(logical_or (logical_and ?x ?y ?op) (logical_and ?x ?z ?op) ?op)"),
    rewrite!("and-absorb"; "(logical_and ?x (logical_or ?x ?y ?op) ?op)" => "?x"),
    rewrite!("and-demorgan"; "(not (logical_and ?x ?y) ?op)" => "(logical_or (not ?x ?op) (not ?y ?op) ?op)"),
    // Rules for comparisons
    rewrite!("eq-commute"; "(eq ?x ?y ?op)" => "(eq ?y ?x ?op)"),
    rewrite!("neq-commute"; "(neq ?x ?y ?op)" => "(neq ?y ?x ?op)"),
    rewrite!("lt-commute"; "(lt ?x ?y ?op)" => "(gt ?y ?x ?op)"),
    rewrite!("lte-commute"; "(lte ?x ?y ?op)" => "(gte ?y ?x ?op)"),
    rewrite!("gt-commute"; "(gt ?x ?y ?op)" => "(lt ?y ?x ?op)"),
    rewrite!("gte-commute"; "(gte ?x ?y ?op)" => "(lte ?y ?x ?op)"),
    rewrite!("eq-reflexive"; "(eq ?x ?x ?op)" => { Literal::new(LiteralValue::True, "?op") }),
    rewrite!("neq-reflexive"; "(neq ?x ?x ?op)" => { Literal::new(LiteralValue::False, "?op") }),
    rewrite!("lt-reflexive"; "(lt ?x ?x ?op)" => { Literal::new(LiteralValue::False, "?op") }),
    rewrite!("lte-reflexive"; "(lte ?x ?x ?op)" => { Literal::new(LiteralValue::True, "?op") }),
    rewrite!("gt-reflexive"; "(gt ?x ?x ?op)" => { Literal::new(LiteralValue::False, "?op") }),
    rewrite!("gte-reflexive"; "(gte ?x ?x ?op)" => { Literal::new(LiteralValue::True, "?op") }),
    // Basic Rules for null_if
    rewrite!("null_if-is_valid"; "(null_if (is_valid ?x ?op) ?x ?op)" => { Literal::new(LiteralValue::Null, "?op") }),
    rewrite!("null_if-not_is_valid"; "(null_if (not (is_valid ?x ?op) ?op) ?x ?op)" => "?x"),
    rewrite!("null_if-false"; "(null_if ?false ?x ?op)" => "?x" if is_literal_satisfying("?false", |s| s.is_false())),
    rewrite!("null_if-true"; "(null_if ?true ?x ?op)" => { Literal::new(LiteralValue::Null, "?op") } if is_literal_satisfying("?true", |s| s.is_true())),
    rewrite!("null_if-repeated"; "(null_if ?a (null_if ?a ?x ?op) ?op)" => "(null_if ?a ?x ?op)"),
    rewrite!("null_if-nested"; "(null_if ?b (null_if ?a ?x ?op) ?op)" => "(null_if (logical_or ?a ?b ?op) ?x ?op)"),
    // We'd like to write `(null_if ?a ?x) => (null_if (logical_or (not (is_valid ?x)) ?a) ?x)`,
    // but that rule is misbehaved since it *introduces* more complexity. So instead, we rely
    // on the logical rewrites that may be applied to boolean operations, and go the other
    // direction, recognizing the four cases that of interest.
    rewrite!("null_if-or_not_is_valid"; "(null_if (logical_or (not (is_valid ?x ?op) ?op) ?a ?op) ?x ?op)" => "(null_if ?a ?x ?op)"),
    rewrite!("null_if-or_is_valid"; "(null_if (logical_or (is_valid ?x ?op) ?a ?op) ?x ?op)" => { Literal::new(LiteralValue::Null, "?op") }),
    rewrite!("null_if-and_not_is_valid"; "(null_if (logical_and (not (is_valid ?x ?op) ?op) ?a ?op) ?x ?op)" => "?x"),
    rewrite!("null_if-and_is_valid"; "(null_if (logical_and (is_valid ?x ?op) ?a ?op) ?x ?op)" => "(null_if ?a ?x ?op)"),
    rewrite!("null_if-lift-field_ref"; "(null_if ?cond (field_ref ?record ?field ?op))" => "(field_ref (null_if ?cond ?record ?op) ?field ?op)" if is_not_literal(vec!["?cond"])),
    // Rewrite if to null-if. This rewrite should be preferred.
    rewrite!("if-to-null_if"; "(if ?cond ?value ?op)" => "(null_if (not ?cond ?op) ?value ?op)"),
    // A field ref is null when the "base" is null, so we can eliminate
    // any `if` or `null_if` that uses the validity of the base.
    rewrite!("if-field_ref"; "(if (is_valid ?e ?op) (field_ref ?e ?field ?op))" => "(field_ref ?e ?field ?op)"),
    rewrite!("null_if-field-ref"; "(null_if (not (is_valid ?e ?op) ?op) (field_ref ?e ?field ?op) ?op)" => "(field_ref ?e ?field ?op)"),
    // Basic rules for is_valid
    // TODO: There are probably a bunch of rules here we could add. Eg., `(is_valid (plus ?a ?b))`
    // can be pushed down to each side, etc. This would only be helpful if it discovers more
    // sharing or allows additional simplifications.
    //
    // TODO: Re-enable these rules if necessary for pushdown/etc.
    //
    // rewrite!("is_valid-if"; "(is_valid (if ?cond ?value ?op) ?op)" => "(logical_and ?cond
    // (is_valid ?value ?op) ?op)"),
    //
    // rewrite!("is_valid-null_if"; "(is_valid (null_if ?cond ?value ?op) ?op)" => "(logical_and
    // (not ?cond ?op) (is_valid ?value ?op) ?op)"),
    // HACK: This simplifies away the `json(str) -> json` instruction into the internal
    // `json_field(str, field) -> str` instruction.
    rewrite!("json-to-json_field"; "(field_ref (json ?value ?op) ?field ?op)" => "(json_field ?value ?field ?op)"),
    //
    //--------------------------------------
    // Rewrite rules for operations (merge join, transform, etc.)
    rewrite!("merge-commutes"; "(merge_join ?a ?b)" => "(merge_join ?b ?a)"),
    rewrite!("merge-reflexive"; "(merge_join ?a ?a)" => "?a"),
    rewrite!("merge-idempotent"; "(merge_join ?a (merge_join ?a ?b))" => "(merge_join ?a ?b)"),
    // TODO: These merge equivalences are correct but lead to issues since we transform values from
    // operations that don't exist. We should come up with some way to eliminate the unnecessary /
    // transforms.
    //
    // rewrite!("merge-associates"; "(merge_join (merge_join ?a ?b) ?c)" => "(merge_join ?a
    // (merge_join ?b ?c))"),
    //
    // rewrite!("merge-undistribute"; "(merge_join (merge_join ?a ?b) (merge_join ?a ?c))" =>
    // "(merge_join ?a (merge_join ?b ?c))"),
    //
    // Eliminate a transform if the expression is already in the requested operation.
    rewrite!("transform-elimination"; "(transform ?e ?op)" => "?e" if is_expression_in_op("?e", "?op")),
    // It would be useful to have a rule like this, but it is generally only valid
    // if the `transform` are guaranteed to be widening (such as if they were added
    // by implicit join/merge).
    // rewrite!("transform-combine"; "(transform (transform ?e ?op1) ?op2)" => "(transform ?e
    // ?op2)" if is_not_literal(vec!["?e"])),
    //
    // Literals and late bound values are effectively the same in every domain.
    // So we rewrite them to eliminate the transform. This avoids problems
    // where a literal/latebound is "narrowed" to a select domain and then
    // unexpectedly null. That said, we may be able to eliminate some of
    // these rules once the operation based compilation / execution is complete.
    rewrite!("transform-literal"; "(transform ?literal ?op)" => { TransformLiteral::new("?literal", "?op") } if is_literal(vec!["?literal"])),
    // TODO: This hard-codes the only valid late bound value for simplicity.
    // If we want to support multiple, we probably want to either detect
    // them during analysis, or create a late-bound function that takes a
    // string to get the late bound value.
    rewrite!("transform-latebound"; "(transform (late_bound:ChangedSinceTime ?any) ?op)" => "(late_bound:ChangedSinceTime ?op)" ),
];

struct Literal {
    value: LiteralValue,
    op_var: egg::Var,
}

impl Literal {
    fn new(value: LiteralValue, op_var: &str) -> Literal {
        let op_var = op_var.parse().unwrap();
        Self { value, op_var }
    }
}

impl Applier<DfgLang, DfgAnalysis> for Literal {
    fn apply_one(
        &self,
        egraph: &mut egg::EGraph<DfgLang, DfgAnalysis>,
        eclass: Id,
        subst: &Subst,
        _searcher_ast: Option<&egg::PatternAst<DfgLang>>,
        _rule_name: egg::Symbol,
    ) -> Vec<Id> {
        let op = subst[self.op_var];
        let value = self.value.to_scalar().expect("Unable to create result");
        let literal_node = egraph.add(DfgLang::new(
            StepKind::Expression(Expression::Literal(value)),
            smallvec![op],
        ));
        if egraph.union(eclass, literal_node) {
            vec![eclass]
        } else {
            vec![]
        }
    }
}

struct TransformLiteral {
    literal_var: egg::Var,
    op_var: egg::Var,
}

impl TransformLiteral {
    fn new(literal_var: &str, op_var: &str) -> Self {
        let literal_var = literal_var.parse().unwrap();
        let op_var = op_var.parse().unwrap();
        Self {
            literal_var,
            op_var,
        }
    }
}

impl Applier<DfgLang, DfgAnalysis> for TransformLiteral {
    fn apply_one(
        &self,
        egraph: &mut egg::EGraph<DfgLang, DfgAnalysis>,
        eclass: Id,
        subst: &Subst,
        _searcher_ast: Option<&egg::PatternAst<DfgLang>>,
        _rule_name: egg::Symbol,
    ) -> Vec<Id> {
        let literal = subst[self.literal_var];
        let op = subst[self.op_var];
        // TODO: Cloning the literal is potentially expensive (if we're cloning
        // strings). We should probably "intern" the string for real, if
        // possible.
        let value = egraph[literal]
            .data
            .literal_opt()
            .cloned()
            .expect("Unable to transform non-literal. Should guard rule with `is_literal`");
        let literal_node = egraph.add(DfgLang::new(
            StepKind::Expression(Expression::Literal(value)),
            smallvec![op],
        ));
        if egraph.union(eclass, literal_node) {
            vec![eclass]
        } else {
            vec![]
        }
    }
}

// Return a function that requires none of the variables are literals.
fn is_not_literal(vars: Vec<&str>) -> impl Fn(&mut DfgGraph, Id, &Subst) -> bool {
    let vars: Vec<_> = vars.into_iter().map(|var| var.parse().unwrap()).collect();
    move |egraph, _, subst| {
        vars.iter()
            .all(|var| egraph[subst[*var]].data.literal_opt().is_none())
    }
}

// Return a function that is true if all of the variables are literals.
fn is_literal(vars: Vec<&str>) -> impl Fn(&mut DfgGraph, Id, &Subst) -> bool {
    let vars: Vec<_> = vars.into_iter().map(|var| var.parse().unwrap()).collect();
    move |egraph, _, subst| {
        vars.iter()
            .all(|var| egraph[subst[*var]].data.literal_opt().is_some())
    }
}

fn is_literal_satisfying(
    var: &str,
    predicate: impl Fn(&ScalarValue) -> bool,
) -> impl Fn(&mut DfgGraph, Id, &Subst) -> bool {
    let var = var.parse().unwrap();
    move |egraph, _, subst| {
        if let Some(literal) = egraph[subst[var]].data.literal_opt() {
            predicate(literal)
        } else {
            false
        }
    }
}

fn is_expression_in_op(expr_var: &str, op_var: &str) -> impl Fn(&mut DfgGraph, Id, &Subst) -> bool {
    let expr_var = expr_var.parse().unwrap();
    let op_var = op_var.parse().unwrap();
    move |egraph, _, subst| {
        let expr_id = subst[expr_var];
        let expr_op = egraph[expr_id].data.operation(expr_id);
        let op_id = subst[op_var];
        egraph.find(expr_op) == op_id
    }
}

pub(super) fn run_simplifications(
    graph: DfgGraph,
    options: &CompilerOptions,
) -> anyhow::Result<DfgGraph> {
    let span = info_span!(
        "Running simplifications on the DFG",
        initial_nodes = graph.total_number_of_nodes(),
        initial_classes = graph.number_of_classes(),
        final_nodes = tracing::field::Empty,
        final_classes = tracing::field::Empty,
    );
    let _enter = span.enter();

    let runner = Runner::default()
        .with_egraph(graph)
        .with_iter_limit(options.internal.simplifier_iteration_limit)
        .with_node_limit(options.internal.simplifier_node_limit)
        .with_time_limit(Duration::from_secs_f64(
            options.internal.simplifier_time_limit_seconds,
        ))
        // Leaving this, since it is useful for debugging simplification.
        // .with_hook(|runner| {
        //     println!("DBG Previous Iteration: {:?}", runner.iterations.last());
        //     Ok(())
        // })
        .run(RULES.iter());

    let graph = runner.egraph;

    // Leaving this, since it is useful for debugging simplification.
    // println!("DBG Final Iteration: {:?}", runner.iterations.last());

    match runner.stop_reason {
        Some(egg::StopReason::Saturated) => info!("Simplification stopped due to saturation"),
        reason => warn!(
            "Simplification didn't saturate. Stopped due to {:?}",
            reason
        ),
    }

    span.record("final_nodes", graph.total_number_of_nodes());
    span.record("final_classes", graph.number_of_classes());
    Ok(graph)
}

#[cfg(test)]
mod tests {
    use sparrow_api::kaskada::v1alpha::FeatureSet;
    use sparrow_syntax::{Expr, FeatureSetPart};

    use super::*;
    use crate::ast_to_dfg::ast_to_dfg;
    use crate::dfg::Dfg;
    use crate::resolve_arguments::resolve_recursive;
    use crate::{DataContext, DiagnosticCollector};

    /// Create a DFG with the given expression.
    fn dfg_with(a: &'static str) -> (Id, Dfg, DataContext) {
        let feature_set = FeatureSet::default();
        let mut diagnostics = DiagnosticCollector::new(&feature_set);

        let mut data_context = DataContext::for_test();

        let mut dfg = data_context.create_dfg().unwrap();

        let a_expr = Expr::try_from_str(FeatureSetPart::Internal(a), a).unwrap();
        let mut diagnostic_builder = Vec::new();
        let a_expr = resolve_recursive(&a_expr, &mut diagnostic_builder).unwrap();
        let a_id = ast_to_dfg(&mut data_context, &mut dfg, &mut diagnostics, &a_expr)
            .unwrap()
            .value();
        (a_id, dfg, data_context)
    }

    /// Returns true if `a` simplifies to `b`.
    fn assert_simplifies_to(a: &'static str, b: &'static str) {
        let (a_id, mut dfg, mut data_context) = dfg_with(a);
        dfg.run_simplifications(&CompilerOptions::default())
            .unwrap();

        let feature_set = FeatureSet::default();
        let mut diagnostics = DiagnosticCollector::new(&feature_set);

        let b_expr = Expr::try_from_str(FeatureSetPart::Internal(b), b).unwrap();
        let mut diagnostic_builder = Vec::new();
        let b_expr = resolve_recursive(&b_expr, &mut diagnostic_builder).unwrap();
        let b_id = ast_to_dfg(&mut data_context, &mut dfg, &mut diagnostics, &b_expr)
            .unwrap()
            .value();
        let a_id = dfg.find(a_id);

        if a_id != b_id {
            panic!(
                "Expected '{}' ({:?}) and '{}' ({:?}) to produce the same DFG node. Classes:\n{:?}",
                a,
                a_id,
                b,
                b_id,
                dfg.dump_graph(),
            );
        }

        let a_data = dfg.data(a_id);
        let b_data = dfg.data(b_id);
        assert_eq!(a_data, b_data);
    }

    /// Passes if and only if the given expression simplifies to `null`.
    fn assert_simplifies_to_null(a: &'static str) {
        let (a_id, mut dfg, _) = dfg_with(a);
        dfg.run_simplifications(&CompilerOptions::default())
            .unwrap();

        assert!(
            // implementation of dfg.literal(a_id).is_some_with(ScalarValue::is_null)`.
            matches!(dfg.literal(a_id), Some(x) if x.is_null()),
            "Expected '{}'({:?}) to produce the literal null, but was {:?}. Classes:\n{:?}",
            a,
            a_id,
            dfg.data(a_id),
            dfg.dump_graph(),
        )
    }

    /// Passes if and only if the given expression simplifies to `null`.
    fn assert_simplifies_to_literal(a: &'static str, value: ScalarValue) {
        let (a_id, mut dfg, _) = dfg_with(a);
        dfg.run_simplifications(&CompilerOptions::default())
            .unwrap();

        let data = dfg.data(a_id);
        assert_eq!(
            data.literal_opt(),
            Some(&value),
            "Expected '{a}'({a_id:?}) to produce the literal value {value:?}, but was {data:?}."
        );
    }

    #[test]
    fn test_simplify_arithmetic() {
        assert_simplifies_to("Table1.x_i64 + 0", "Table1.x_i64");
        assert_simplifies_to("0 + Table1.x_i64", "Table1.x_i64");

        assert_simplifies_to_literal("-(- 1)", ScalarValue::Int64(Some(1)));
        assert_simplifies_to_literal("-(1)", ScalarValue::Int64(Some(-1)));

        assert_simplifies_to("-1 * Table1.x_i64", "-Table1.x_i64");
        assert_simplifies_to_literal("Table1.x_i64 - Table1.x_i64", ScalarValue::Int64(Some(0)));
        assert_simplifies_to_literal(
            "Table1.x_i64 + (-Table1.x_i64)",
            ScalarValue::Int64(Some(0)),
        );
        assert_simplifies_to_literal(
            "(-Table1.x_i64) + Table1.x_i64",
            ScalarValue::Int64(Some(0)),
        );
        assert_simplifies_to_literal(
            "Table1.x_i64 + (-1 * Table1.x_i64)",
            ScalarValue::Int64(Some(0)),
        );

        assert_simplifies_to_literal("-2 * -2", ScalarValue::Int64(Some(4)));
        assert_simplifies_to_literal("-1.0 * 3.0", ScalarValue::from_f64(-3.0));

        assert_simplifies_to_literal("Table1.x_i64 * 0", ScalarValue::Int64(Some(0)));
        assert_simplifies_to_literal("0 * Table1.x_i64", ScalarValue::Int64(Some(0)));
        assert_simplifies_to("Table1.x_i64 * 1", "Table1.x_i64");
        assert_simplifies_to("1 * Table1.x_i64", "Table1.x_i64");

        assert_simplifies_to("Table1.x_i64 / 1", "Table1.x_i64");
    }

    #[test]
    fn test_simplify_logic() {
        // And with constant
        assert_simplifies_to("Table1.a_bool and true", "Table1.a_bool");
        assert_simplifies_to_literal("Table1.a_bool and false", ScalarValue::Boolean(Some(false)));
        assert_simplifies_to("true and Table1.a_bool", "Table1.a_bool");
        assert_simplifies_to_literal("false and Table1.a_bool", ScalarValue::Boolean(Some(false)));

        // Or with constant
        assert_simplifies_to_literal("Table1.a_bool or true", ScalarValue::Boolean(Some(true)));
        assert_simplifies_to("Table1.a_bool or false", "Table1.a_bool");
        assert_simplifies_to_literal("true or Table1.a_bool", ScalarValue::Boolean(Some(true)));
        assert_simplifies_to("false or Table1.a_bool", "Table1.a_bool");

        // Negation with constants (and cancelling)
        assert_simplifies_to_literal("!true", ScalarValue::Boolean(Some(false)));
        assert_simplifies_to_literal("!false", ScalarValue::Boolean(Some(true)));
        assert_simplifies_to("!!Table1.a_bool", "Table1.a_bool");

        // And identities with same and negation
        assert_simplifies_to("Table1.a_bool and Table1.a_bool", "Table1.a_bool");
        assert_simplifies_to(
            "Table1.a_bool and !Table1.a_bool",
            "if(is_valid(Table1.a_bool), false)",
        );
        assert_simplifies_to(
            "!Table1.a_bool and Table1.a_bool",
            "if(is_valid(Table1.a_bool), false)",
        );

        // Or identities with same and negation
        assert_simplifies_to("Table1.a_bool or Table1.a_bool", "Table1.a_bool");
        assert_simplifies_to(
            "Table1.a_bool or !Table1.a_bool",
            "if(is_valid(Table1.a_bool), true)",
        );
        assert_simplifies_to(
            "!Table1.a_bool or Table1.a_bool",
            "if(is_valid(Table1.a_bool), true)",
        );
    }

    #[test]
    fn test_simplify_nullif() {
        // If x is null, then is_valid is false, and the result is x (null).
        // If x is not null, then is_valid is true, and the result is null.
        assert_simplifies_to_null("null_if(is_valid(Table1.x_i64), Table1.x_i64)");

        assert_simplifies_to(
            "null_if(!is_valid(Table1.x_i64), Table1.x_i64)",
            "Table1.x_i64",
        );
        assert_simplifies_to("null_if(!is_valid(Table1), Table1.x_i64)", "Table1.x_i64");

        assert_simplifies_to("null_if(false, Table1.x_i64)", "Table1.x_i64");
        assert_simplifies_to_null("null_if(true, Table1.x_i64)");
        assert_simplifies_to(
            "null_if(Table1.b_bool, null_if(Table1.a_bool, Table1.x_i64))",
            "null_if(Table1.a_bool or Table1.b_bool, Table1.x_i64)",
        );
        assert_simplifies_to(
            "null_if(Table1.b_bool, null_if(Table1.a_bool, Table1.x_i64))",
            "null_if(Table1.b_bool or Table1.a_bool, Table1.x_i64)",
        );

        // Whenever `x` is valid we null it out.
        assert_simplifies_to_null("null_if(is_valid(Table1.x_i64) or Table1.a_bool, Table1.x_i64)");
        assert_simplifies_to(
            "null_if(!is_valid(Table1.x_i64) or Table1.a_bool, Table1.x_i64)",
            "null_if(Table1.a_bool, Table1.x_i64)",
        );
        assert_simplifies_to(
            "null_if(is_valid(Table1.x_i64) and Table1.a_bool, Table1.x_i64, )",
            "null_if(Table1.a_bool, Table1.x_i64)",
        );
        assert_simplifies_to(
            "null_if(!is_valid(Table1.x_i64) and Table1.a_bool, Table1.x_i64)",
            "Table1.x_i64",
        );

        // Make sure we can rewrite inside of null if.
        assert_simplifies_to_null(
            "null_if(is_valid(Table1.x_i64), null_if(Table1.a_bool, Table1.x_i64))",
        );
        assert_simplifies_to(
            "null_if(!is_valid(Table1.x_i64), null_if(Table1.a_bool, Table1.x_i64))",
            "null_if(Table1.a_bool, Table1.x_i64)",
        );
    }

    #[test]
    #[ignore = "Revisit \"known not null\" and \"is new\" simplifications"]
    fn test_simplify_is_valid() {
        // assert_simplifies_to(
        //     "is_valid(null_if(Table1.a_bool, Table1.x_i64))",
        //     "is_valid(Table1.x_i64) and !Table1.a_bool",
        // );
        // assert_simplifies_to(
        //     "is_valid(if(Table1.a_bool, Table1.x_i64))",
        //     "is_valid(Table1.x_i64) and Table1.a_bool",
        // );
        assert_simplifies_to_literal(
            "is_valid({ a: Table1.a_bool })",
            ScalarValue::Boolean(Some(true)),
        );
    }

    #[test]
    fn test_pipe_equivalents() {
        // While technically not a test of simplification, this falls under a similar
        // category -- verifying that two expressions are treated as equivalent.
        assert_simplifies_to("Table1.x_i64 | sum()", "sum(Table1.x_i64)")
    }

    #[test]
    fn test_simplify_field_ref() {
        assert_simplifies_to("{a: Table1.a_bool }.a", "Table1.a_bool");
        assert_simplifies_to("{a: Table1.x_i64, b: Table1.a_bool }.a", "Table1.x_i64");
        assert_simplifies_to(
            "let record = {a: Table1.x_i64, b: Table1.a_bool, c: Table1.b_bool}
                let a = record.a
                let b = record.b
                in { a, b }",
            "{ a: Table1.x_i64, b: Table1.a_bool }",
        );
        assert_simplifies_to(
            "null_if(Table1.b, Table1).x_i64",
            "null_if(Table1.b, Table1.x_i64)",
        );
        assert_simplifies_to("if(Table1.b, Table1).x_i64", "if(Table1.b, Table1.x_i64)");
    }
}
