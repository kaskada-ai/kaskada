use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use anyhow::Context;
use bit_set::BitSet;
use hashbrown::{HashMap, HashSet};
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::FeatureSet;
use sparrow_syntax::{Expr, ExprOp, FeatureSetPart, Resolved, ResolvedExpr};
use tracing::error;

use super::parse_expr::parse_expr;
use super::resolve_arguments::resolve_recursive;
use crate::frontend::first_reference::first_reference;
use crate::frontend::free_variable::free_variables;
use crate::{DiagnosticBuilder, DiagnosticCode, DiagnosticCollector};

/// The parsed and resolved [FeatureSet].
///
/// Contains information used to construct the dfg.
pub(super) struct ParsedFeatureSet<'a> {
    pub formulas: Vec<ParsedFormula<'a>>,
    pub query_expr: ResolvedExpr,
    /// The set of free names in the feature set.
    ///
    /// Used to identify places in expressions where substitution may take
    /// place.
    pub free_names: BTreeSet<String>,
}

/// The parsed and resolved [Formula].
pub(super) struct ParsedFormula<'a> {
    pub name: &'a str,
    pub expr: ResolvedExpr,
}

impl<'a> ParsedFeatureSet<'a> {
    /// Constructs a [ParsedFeatureSet].
    ///
    /// Parses, resolves, and topologically sorts expressions in the
    /// `FeatureSet`.
    pub fn try_new(
        feature_set: &'a FeatureSet,
        diagnostics: &mut DiagnosticCollector<'a>,
    ) -> anyhow::Result<Self> {
        let query_info = parse_and_resolve(feature_set, diagnostics)
            .context("Parsing and Resolving feature set")?;

        Ok(
            order_feature_set(query_info, diagnostics).unwrap_or_else(|| ParsedFeatureSet {
                formulas: vec![],
                query_expr: ResolvedExpr {
                    op: ExprOp::Error,
                    args: Resolved::empty(),
                },
                free_names: BTreeSet::new(),
            }),
        )
    }
}

// Holds information about the parsed query and formulas. Used to perform
// analysis and ordering of resolved expressions.
struct QueryInfo<'a> {
    expr: ResolvedExpr,
    free_names: HashSet<String>,
    dependencies: BitSet,
    pub formula_infos: Vec<FormulaInfo<'a>>,
}

/// Intermediate information used during the topologic sort.
///
/// This struct is the intermediate form between a [FeatureSet]
/// and a [ParsedFeatureSet] (with [ParsedFormula]s).
struct FormulaInfo<'a> {
    name: &'a str,
    formula: &'a str,
    /// The index of this formula in the original order.
    index: usize,
    /// The indices of formulas this formula depends on.
    dependencies: BitSet,
    /// The resolved expression for this formula.
    expr: ResolvedExpr,
    /// The assigned index in the topologic order.
    ///
    /// Initialized as 0, and set appropriately when doing
    /// topological sort.
    topo_order: usize,
    /// Free names in the formula.
    free_names: HashSet<String>,
    /// Diagnostics for this formula.
    ///
    /// If this formula encountered errors during parsing or resolving, the
    /// errors should be populated here.
    ///
    /// These are stored here rather than reported immediately, as we only want
    /// to report diagnostics for features referenced by the query.
    diagnostics: Vec<DiagnosticBuilder>,
}

fn parse_and_resolve<'a>(
    feature_set: &'a FeatureSet,
    diagnostics: &mut DiagnosticCollector<'_>,
) -> anyhow::Result<QueryInfo<'a>> {
    let name_to_index = formula_name_to_index(feature_set, diagnostics)?;

    // Parse and resolve all formulas
    let mut formula_infos: Vec<_> = feature_set
        .formulas
        .iter()
        .enumerate()
        .map(|(index, formula)| -> anyhow::Result<_> {
            let mut formula_diagnostics = Vec::new();

            // Note: Could split up `parse` and `resolve`
            let part_id = FeatureSetPart::Formula(index as u32);
            let expr = match parse_expr(part_id, &formula.formula) {
                Ok(expr) => expr,
                Err(mut parse_diagnostics) => {
                    formula_diagnostics.append(&mut parse_diagnostics);
                    Arc::new(Expr::error())
                }
            };

            // Recursively resolve the expression
            //
            // TODO: Can use trait that implements `extend_one` for diagnostic
            // once it is stable. https://github.com/rust-lang/rust/issues/72631
            let resolved_expr = resolve_recursive(&expr, &mut formula_diagnostics)?;
            let (free_names, dependencies) =
                direct_dependencies(&resolved_expr, &name_to_index, diagnostics);
            Ok(FormulaInfo {
                name: &formula.name,
                formula: &formula.formula,
                index,
                dependencies,
                expr: resolved_expr,
                // Initialize all with a default topo_order of 0. Will be re-set during ordering.
                topo_order: 0,
                free_names,
                diagnostics: formula_diagnostics,
            })
        })
        .try_collect()?;

    // Parse the top-level query
    let query_expr = match parse_expr(FeatureSetPart::Query, &feature_set.query) {
        Ok(expr) => expr,
        Err(parse_diagnostics) => {
            diagnostics.collect_all(parse_diagnostics);
            Arc::new(Expr::error())
        }
    };

    // Recursively resolve the query expression
    //
    // TODO: Can use trait that implements `extend_one` for diagnostic
    // once it is stable. https://github.com/rust-lang/rust/issues/72631
    let mut query_diagnostics = Vec::new();
    let resolved_query_expr = resolve_recursive(&query_expr, &mut query_diagnostics)?;

    // Collect any diagnostics from resolving the query
    if !query_diagnostics.is_empty() {
        diagnostics.collect_all(query_diagnostics);
    }

    let (query_free_names, query_dependencies) =
        direct_dependencies(&resolved_query_expr, &name_to_index, diagnostics);
    if query_dependencies
        .iter()
        .any(|dep| !formula_infos[dep].diagnostics.is_empty())
    {
        // If any of the query dependencies have errors, report them.
        formula_infos
            .iter_mut()
            .enumerate()
            // Only include the formulas that were referenced by the query.
            .filter(|(index, _)| query_dependencies.contains(*index))
            .for_each(|(_, f)| {
                if !f.diagnostics.is_empty() {
                    diagnostics.collect_all(f.diagnostics.drain(..));
                }
            });
    }

    let query_info = QueryInfo {
        expr: resolved_query_expr,
        free_names: query_free_names,
        dependencies: query_dependencies,
        formula_infos,
    };
    Ok(query_info)
}

/// Parse and topologically sort the formulas.
///
/// On success, this returns the parsed formulas and query.
/// The formulas are only those necessary, in topologic order.
///
/// On failure, returns the diagnostics that should be reported.
/// This includes parsing errors and dependency cycles.
///
/// This uses Kahn's algorithm and attempts to preserve the order
/// the formulas originally appeared in when possible.
fn order_feature_set<'a>(
    query_info: QueryInfo<'a>,
    diagnostics: &mut DiagnosticCollector<'_>,
) -> Option<ParsedFeatureSet<'a>> {
    // If diagnostics have been reported, return immediately with no additional
    // diagnostics.
    if diagnostics.num_errors() > 0 {
        return None;
    }

    let QueryInfo {
        mut formula_infos,
        dependencies,
        ..
    } = query_info;

    // Topologically sort things. This sorts in place (if possible),
    // and returns an error if there is a cycle.
    if let Err(cycle) = topologic_sort(&mut formula_infos, dependencies) {
        cycle_to_diagnostic(&formula_infos, cycle).emit(diagnostics);
        return None;
    };

    let mut free_names = BTreeSet::new();
    free_names.extend(query_info.free_names);
    for used_formula in formula_infos.iter_mut() {
        free_names.extend(used_formula.free_names.drain());
    }

    let ordered_formulas = formula_infos
        .into_iter()
        .map(|info| ParsedFormula {
            name: info.name,
            expr: info.expr,
        })
        .collect();

    Some(ParsedFeatureSet {
        formulas: ordered_formulas,
        query_expr: query_info.expr,
        free_names,
    })
}

fn formula_name_to_index<'a>(
    feature_set: &'a FeatureSet,
    diagnostics: &'_ mut DiagnosticCollector<'_>,
) -> anyhow::Result<HashMap<&'a String, usize>> {
    let mut name_to_index = HashMap::with_capacity(feature_set.formulas.len());
    let mut has_errors = false;

    for (index, formula) in feature_set.formulas.iter().enumerate() {
        if !sparrow_syntax::is_valid_ident(&formula.name) {
            DiagnosticCode::IllegalIdentifier
                .builder()
                .with_note(format!(
                    "Formula name '{}' is not a legal identifier.",
                    formula.name
                ))
                .emit(diagnostics);
            has_errors = true;
        }

        if let Err(prev) = name_to_index.try_insert(&formula.name, index) {
            DiagnosticCode::FormulaAlreadyDefined
                .builder()
                .with_note(format!(
                    "Formula name '{}' is defined at index {} and again at {}.",
                    formula.name,
                    prev.entry.get(),
                    index
                ))
                .emit(diagnostics);
            has_errors = true;
        }
    }

    if !has_errors {
        Ok(name_to_index)
    } else {
        Ok(HashMap::new())
    }
}

/// Returns all referenced names and referenced formulas.
///
/// All referenced names are in a `HashSet<String>`, while
/// referenced formulas are placed in a `BitSet` keyed by the
/// `name_to_index` map.
pub(super) fn direct_dependencies(
    expr: &ResolvedExpr,
    name_to_index: &HashMap<&String, usize>,
    diagnostics: &mut DiagnosticCollector<'_>,
) -> (HashSet<String>, BitSet) {
    if matches!(expr.op(), ExprOp::Error) {
        (HashSet::new(), BitSet::new())
    } else {
        // Add the dependencies from free variables in the expression.
        let mut dependencies = BitSet::with_capacity(name_to_index.len());
        let free_variables = free_variables(expr, diagnostics);

        free_variables
            .iter()
            .filter_map(|name| name_to_index.get(name))
            .for_each(|index| {
                dependencies.insert(*index);
            });
        (free_variables, dependencies)
    }
}

/// Convert a cycle to a diagnostic reporting the cycle.
fn cycle_to_diagnostic(formula_infos: &[FormulaInfo<'_>], cycle: Vec<usize>) -> DiagnosticBuilder {
    let mut diagnostic = DiagnosticCode::CyclicReference.builder();
    for (src_index, dst_index) in cycle.into_iter().tuple_windows() {
        let src = &formula_infos[src_index];
        let dst = &formula_infos[dst_index];
        if let Some(dst_reference) = first_reference(&formula_infos[src_index].expr, dst.name) {
            diagnostic =
                diagnostic.with_label(dst_reference.location().primary_label().with_message(
                    format!("Formula '{}' referenced here in '{}'", dst.name, src.name),
                ));
        } else {
            error!(
                "Missing reference to '{}' in dependency cycle from expr '{}'",
                dst.name, src.formula
            );

            diagnostic = diagnostic.with_note(format!("'{}' references '{}'", src.name, dst.name));
        }
    }

    diagnostic
}

// Compute the union over an iterator of `&BitSet`.
//
// Only the first item is cloned. The rest are added to that bitset.
// Returns `None` if the iterator is empty.
fn union_all<'a>(mut iter: impl Iterator<Item = &'a BitSet>) -> Option<BitSet> {
    if let Some(first) = iter.next() {
        let mut result = first.clone();
        iter.for_each(|next| result.union_with(next));
        Some(result)
    } else {
        None
    }
}

/// Computes the transitive closure of the referenced direct dependencies.
///
/// Specifically, `direct_deps` should contain the indices of directly
/// referenced formulas. Returns a bit set containing all, all formulas directly
/// or indirectly referenced will be returned.
fn transitive_closure(to_sort: &[FormulaInfo<'_>], direct_deps: BitSet) -> BitSet {
    let mut transitive = BitSet::with_capacity(to_sort.len());

    // We're going to compute the transitive closure in "layers".
    // We start with those things directly referenced.
    let mut new_deps = direct_deps;

    while !new_deps.is_empty() {
        // We add the new dependencies to the transitive closure.
        transitive.union_with(&new_deps);

        // Compute the dependencies referenced by anything in `new_deps`.
        // These may or may not already be in the transitive closure.
        new_deps = union_all(new_deps.iter().map(|index| &to_sort[index].dependencies))
            .expect("new_deps is non-empty");

        // Remove the already discovered dependencies.
        // This also guarantees that we eventually terminate, when all formulas
        // are part of the transitive set.
        new_deps.difference_with(&transitive);
    }

    transitive
}

/// This method actually performs the topologic sort.
///
/// We keep it separate from the logic for creating formula info for easier
/// testing.
///
/// We don't bother with a heap because we expect that the number of formulas in
/// any given query will be tractable and the O(n^2) shouldn't be a problem. If
/// profiling suggests this is otherwise, we can easily switch to a more
/// sophisticated structure.
fn topologic_sort(
    to_sort: &mut Vec<FormulaInfo<'_>>,
    direct_deps: BitSet,
) -> Result<(), Vec<usize>> {
    // Indices of formulas that need to be processed.
    let referenced = transitive_closure(to_sort, direct_deps);
    let mut pending: BitSet = referenced.clone();
    while !pending.is_empty() {
        // Find the first pending formula that doesn't depend on any pending formulas.
        // If the graph is acyclic, then every iteration we can find a formula that
        // depends only on formulas that have already been scheduled.
        if let Some(ready) = pending
            .iter()
            .find(|index| to_sort[*index].dependencies.is_disjoint(&pending))
        {
            to_sort[ready].topo_order = to_sort.len() - pending.len();
            pending.remove(ready);
        } else {
            return Err(find_shortest_cycle(pending, to_sort));
        }
    }

    // Re-order the vector by topologic order.
    to_sort.retain(|f| referenced.contains(f.index));
    to_sort.sort_by_key(|f| f.topo_order);

    Ok(())
}

/// Function called when the topologic sorting of formulas detects a cycle.
///
/// This keeps this code out of the main path for readability.
///
/// This analyzes the remaining dependencies and finds the cycle.
fn find_shortest_cycle(pending: BitSet, formulas: &[FormulaInfo<'_>]) -> Vec<usize> {
    // The indices involved in the shortest cycle found so far.
    let mut shortest_cycle: Option<Vec<usize>> = None;
    for start in pending.iter() {
        if let Some(shorter_cycle) = find_shortest_cycle_from(
            start,
            shortest_cycle
                .as_ref()
                .map(|cycle| cycle.len() - 1)
                .unwrap_or(usize::MAX),
            &pending,
            formulas,
        ) {
            debug_assert!(
                shortest_cycle
                    .iter()
                    .all(|prev| prev.len() > shorter_cycle.len()),
                "Previous: {shortest_cycle:?}, Shorter: {shorter_cycle:?}"
            );

            shortest_cycle = Some(shorter_cycle)
        };
    }

    shortest_cycle.unwrap_or_else(|| panic!("Illegal: Unable to find cycle in graph."))
}

/// Find the shortest cycle starting at `start`.
///
/// Returns `None` if no cycle has been with depth less than or equal to
/// `max_len`.
///
/// `pending` indicates the formulas that have already been visited. All nodes
/// in a cycle must be in the pending set.
fn find_shortest_cycle_from(
    start: usize,
    max_len: usize,
    pending: &BitSet,
    formulas: &[FormulaInfo<'_>],
) -> Option<Vec<usize>> {
    // It *may* be faster to re-use these queues by allocating in
    // `report_cyclic_dependency`. But we do it here for clarity until profiling
    // indicates it *actually matters*. This method is only called after
    // determining a cycle is present, so we're not too worried about time.

    // Records how each node was found.
    // This is `None` if the node is unvisited.
    // This is `Some(index)` for the start node.
    let mut parents = vec![None; formulas.len()];
    parents[start] = Some(start);

    let mut to_visit = VecDeque::with_capacity(formulas.len());
    to_visit.push_back(start);

    let mut curr_level = 0;
    let mut nodes_in_level = to_visit.len();

    while let Some(curr) = to_visit.pop_front() {
        if formulas[curr].dependencies.contains(start) {
            // We have found the last edge that loops back to the start. The nature
            // of the BFS ensures this is the shortest such loop.
            let mut cycle = Vec::with_capacity(curr_level + 1);
            cycle.push(start);
            let mut curr = curr;
            while curr != start {
                cycle.push(curr);
                curr = parents[curr].unwrap();
            }
            cycle.push(start);

            debug_assert_eq!(
                cycle.len(),
                curr_level + 2,
                "Expected cycle at level {} to have length {}, but was {:?}",
                curr_level,
                curr_level + 2,
                cycle
            );

            return Some(cycle);
        } else {
            // For each index in (dependendencies - pending) that haven't been
            // reached, add them to the queue.
            formulas[curr]
                .dependencies
                .intersection(pending)
                .for_each(|next| {
                    if parents[next].is_none() {
                        parents[next] = Some(curr);
                        to_visit.push_back(next);
                    }
                })
        }

        nodes_in_level -= 1;
        if nodes_in_level == 0 {
            if curr_level + 2 == max_len {
                // The number of nodes in the cycle depends on the depth (level) of the BFS.
                // Level 0 = [Start Node], Level 1 = [Nodes from Start Node], ...
                //
                // So a cycle found at level 0 has two nodes `[Start, Start]`, a cycle at level
                // 1 has three nodes `[Start, Node (from level 1), Start]`, etc.
                //
                // To produce a cycle with at most `max_len` steps, we need `curr_level + 2` to
                // be less than `max_len`. If that won't be true once we increment, we stop.
                return None;
            }
            curr_level += 1;
            nodes_in_level = to_visit.len();
        }
    }

    None
}

#[cfg(test)]
mod tests {

    use sparrow_api::kaskada::v1alpha::Formula;
    use sparrow_syntax::Resolved;

    use super::*;
    use crate::DiagnosticCollector;

    fn ordering_for(
        formulas: Vec<Vec<usize>>,
        initial: Vec<usize>,
    ) -> Result<Vec<usize>, Vec<usize>> {
        // For the test we don't really care about the expression, so we use
        // `error` for all
        let mut formulas: Vec<_> = formulas
            .into_iter()
            .enumerate()
            .map(|(index, dependencies)| {
                let dependencies: BitSet = dependencies.into_iter().collect();
                FormulaInfo {
                    name: "formula",
                    formula: "formula",
                    index,
                    dependencies,
                    expr: ResolvedExpr {
                        op: ExprOp::Error,
                        args: Resolved::empty(),
                    },
                    topo_order: 0,
                    free_names: HashSet::new(),
                    diagnostics: Vec::new(),
                }
            })
            .collect();

        let initial: BitSet = initial.into_iter().collect();

        topologic_sort(&mut formulas, initial)?;
        Ok(formulas.into_iter().map(|f| f.index).collect())
    }

    #[test]
    fn test_ordered() {
        assert_eq!(
            ordering_for(vec![vec![], vec![0], vec![0], vec![1, 2], vec![3]], vec![4]),
            Ok(vec![0, 1, 2, 3, 4])
        );
    }

    #[test]
    fn test_unordered() {
        assert_eq!(
            ordering_for(vec![vec![1], vec![], vec![0], vec![2, 0], vec![3]], vec![4]),
            Ok(vec![1, 0, 2, 3, 4])
        );
    }

    #[test]
    fn test_unused() {
        assert_eq!(
            ordering_for(vec![vec![], vec![], vec![0], vec![1]], vec![2]),
            Ok(vec![0, 2])
        )
    }

    #[test]
    fn test_cycle() {
        // Cycle 2 -> 3 -> 2
        assert_eq!(
            ordering_for(vec![vec![1], vec![], vec![3], vec![2, 0], vec![3]], vec![4]),
            Err(vec![2, 3, 2])
        );
    }

    #[test]
    fn test_shortest_cycle() {
        // Two cycles: 0 -> 1 -> 2 -> 0 (length 3) and 2 -> 3 -> 2 (length 2).
        // This verifies that we'll prefer the shorter when we find it.
        assert_eq!(
            ordering_for(vec![vec![1], vec![2], vec![0, 3], vec![2]], vec![3]),
            Err(vec![2, 3, 2])
        );

        // Two cycles: 0 -> 1 -> 2 -> 0 (length 3) and 3 -> 4 -> 3 (length 2).
        // This verifies that we'll prefer the shorter when we find it.
        assert_eq!(
            ordering_for(vec![vec![1], vec![2], vec![0], vec![4], vec![3]], vec![3]),
            Err(vec![3, 4, 3])
        );
    }

    // Run the parse and order function.
    //
    // If it returns `Ok(ordering)`, this returns the string `Ordered: f1, f2, ...`
    // containing names of the successfully parsed formulas, in the order they are
    // needed.
    //
    // If it returns `Err(diagnostics)`, this returns the string `Errors\n...`
    // formatted diagnostics.
    fn run_parse_and_order(feature_set: FeatureSet) -> String {
        let mut diagnostics = DiagnosticCollector::new(&feature_set);
        let ordered = ParsedFeatureSet::try_new(&feature_set, &mut diagnostics).unwrap();

        let diagnostics = diagnostics.finish();
        if !diagnostics.is_empty() {
            format!("Diagnostics\n{}", diagnostics.into_iter().format("\n"))
        } else {
            format!(
                "Ordered: {}",
                ordered
                    .formulas
                    .into_iter()
                    .map(|f| f.name.to_owned())
                    .format(", ")
            )
        }
    }

    #[test]
    fn test_parse_error_in_expr() {
        insta::assert_snapshot!(run_parse_and_order(FeatureSet {
            formulas: vec![],
            query: "Foo + $$".to_owned(),
        }), @r###"
        Diagnostics
        error[E0011]: Invalid syntax
          --> Query:1:7
          |
        1 | Foo + $$
          |       ^ Invalid token '$'
          |
          = Expected "!", "$input", "(", "-", "{", ident, literal

        "###);
    }

    #[test]
    fn test_parse_error_in_referenced_formula() {
        insta::assert_snapshot!(run_parse_and_order(FeatureSet {
            formulas: vec![Formula {
                name: "FormulaFoo".to_owned(),
                formula: "Foo + $$".to_owned(),
                source_location: "FormulaFoo view".to_owned(),
            }],
            query: "FormulaFoo".to_owned(),
        }), @r###"
        Diagnostics
        error[E0011]: Invalid syntax
          --> 'FormulaFoo view':1:7
          |
        1 | Foo + $$
          |       ^ Invalid token '$'
          |
          = Expected "!", "$input", "(", "-", "{", ident, literal

        "###);
    }

    #[test]
    fn test_parse_error_in_unreferenced_formula() {
        insta::assert_snapshot!(run_parse_and_order(FeatureSet {
            formulas: vec![
                Formula {
                    name: "FormulaFoo".to_owned(),
                    formula: "Foo + $$".to_owned(),
                    source_location: "FormulaFoo view".to_owned(),
                },
                Formula {
                    name: "FormulaBar".to_owned(),
                    formula: "TableFoo + 1".to_owned(),
                    source_location: "FormulaBar view".to_owned(),
                },
            ],
            query: "FormulaBar".to_owned(),
        }), @"Ordered: FormulaBar");
    }

    #[test]
    fn test_duplicate_formula_name() {
        insta::assert_snapshot!(run_parse_and_order(FeatureSet {
            formulas: vec![
                Formula {
                    name: "FormulaFoo".to_owned(),
                    formula: "Foo + $$".to_owned(),
                    source_location: "FormulaFoo view".to_owned(),
                },
                Formula {
                    name: "FormulaFoo".to_owned(),
                    formula: "TableFoo + 1".to_owned(),
                    source_location: "FormulaFoo view".to_owned(),
                },
            ],
            query: "FormulaFoo".to_owned(),
        }), @r###"
        Diagnostics
        error[E0004]: Formula already defined
         = Formula name 'FormulaFoo' is defined at index 0 and again at 1.

        "###);
    }

    #[test]
    fn test_invalid_formula_name() {
        insta::assert_snapshot!(run_parse_and_order(FeatureSet {
            formulas: vec![Formula {
                name: "InvalidFormula Name$".to_owned(),
                formula: "Foo + $$".to_owned(),
                source_location: "FormulaFoo view".to_owned(),
            }],
            query: "FormulaBar".to_owned(),
        }), @r###"
        Diagnostics
        error[E0003]: Illegal identifier
         = Formula name 'InvalidFormula Name$' is not a legal identifier.

        "###);
    }

    #[test]
    fn test_unused_binding() {
        insta::assert_snapshot!(run_parse_and_order(FeatureSet {
            formulas: vec![Formula {
                name: "foo".to_owned(),
                formula: "Foo | sum(Foo)".to_owned(),
                source_location: "FormulaFoo view".to_owned(),
            }],
            query: "foo".to_owned(),
        }), @r###"
        Diagnostics
        warning[W2001]: Unused binding
          --> 'FormulaFoo view':1:5
          |
        1 | Foo | sum(Foo)
          |     ^ Left-hand side of pipe not used

        "###);
    }

    #[test]
    fn test_implicit_input_binding() {
        insta::assert_snapshot!(run_parse_and_order(FeatureSet {
            formulas: vec![Formula {
                name: "foo".to_owned(),
                formula: "Foo | sum()".to_owned(),
                source_location: "FormulaFoo view".to_owned(),
            }],
            query: "foo".to_owned(),
        }), @"Ordered: foo");
    }
}
