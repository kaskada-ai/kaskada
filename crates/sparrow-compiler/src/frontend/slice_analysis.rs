//! Analysis of which tables are needed and in which configurations.

use anyhow::Context;
use egg::Id;
use hashbrown::HashMap;
use itertools::Itertools;
use smallvec::smallvec;
use sparrow_api::kaskada::v1alpha::{slice_plan, SlicePlan};
use sparrow_api::kaskada::v1alpha::{slice_request, SliceRequest};
use sparrow_core::debug_println;

use crate::dfg::{ChildrenVec, Dfg, DfgExpr, Operation, StepKind};
use crate::DataContext;

const DEBUG_SLICE_ANALYSIS: bool = false;

/// Return the `expr` rewritten to use the given slice.
pub(super) fn rewrite_slices(
    original: DfgExpr,
    slice_request: &Option<SliceRequest>,
) -> anyhow::Result<DfgExpr> {
    debug_println!(DEBUG_SLICE_ANALYSIS, "Slicing:\n{}", original.dot_string()?);

    // The last node in the expression is the "result".
    let original_output_id: Id = (original.len() - 1).into();

    let mut rewriter = SliceRewriter::new(&original, slice_request);
    rewriter.rewrite(&original, original_output_id, 0)?;
    rewriter.finish(original_output_id)
}

/// Returns the table slices used within `expr`.
pub(super) fn slice_plans(
    expr: &DfgExpr,
    data_context: &DataContext,
) -> anyhow::Result<Vec<SlicePlan>> {
    let mut slice_plans = Vec::new();
    for id in expr.ids() {
        if let StepKind::Operation(Operation::Scan { table_id, slice }) = expr.kind(id) {
            let table_name = data_context
                .table_info(*table_id)
                .context("missing table for ID")?
                .name()
                .to_owned();
            let slice = slice.clone();
            slice_plans.push(SlicePlan { table_name, slice })
        }
    }

    // Sort so we have a deterministic ordering.
    slice_plans.sort();
    Ok(slice_plans)
}

struct SliceRewriter {
    primary_slice: Option<slice_plan::Slice>,
    foreign_slice: Option<slice_plan::Slice>,
    /// Map from `(original_id, lookup_count) -> rewritten_id`.
    rewritten_ids: HashMap<(Id, usize), Id>,
    /// The (mutable) DFG being produced.
    rewritten_dfg: Dfg,
}

impl SliceRewriter {
    fn new(original: &DfgExpr, slice_request: &Option<SliceRequest>) -> Self {
        // Determine the slice needed for the primary tables.
        // Currently, we don't push any predicates or projections down, so this
        // is computed directly from the slice request. This is likely to evolve,
        // and may (at some point) be per-table.
        let primary_slice = slice_request.as_ref().and_then(|request| {
            request.slice.as_ref().map(|slice| match slice {
                slice_request::Slice::Percent(slice_request::PercentSlice { percent }) => {
                    slice_plan::Slice::Percent(slice_plan::PercentSlice { percent: *percent })
                }
                slice_request::Slice::EntityKeys(e) => {
                    slice_plan::Slice::EntityKeys(slice_plan::EntityKeysSlice {
                        entity_keys: e.entity_keys.clone(),
                    })
                }
            })
        });

        let foreign_slice = None;

        let rewritten_ids = HashMap::with_capacity(original.len());
        Self {
            primary_slice,
            foreign_slice,
            rewritten_ids,
            rewritten_dfg: Dfg::default(),
        }
    }

    /// Return the ID of a rewritten node.
    ///
    /// The original node is `original_id` in `original`.
    ///
    /// The `lookup_count` records how many nested lookups we are in. It is
    /// incremented when recursing into a part of the DFG within a lookup
    /// (generally, the `value` of a `transform` to a `lookup_response`) and
    /// decremented when recursing out of the lookup (passing through the
    /// `lookup_request`).
    fn rewrite(
        &mut self,
        original: &DfgExpr,
        original_id: Id,
        lookup_count: usize,
    ) -> anyhow::Result<Id> {
        if let Some(rewritten_id) = self.get_rewritten_id(original_id, lookup_count) {
            return Ok(rewritten_id);
        }

        let (kind, args) = original.node(original_id);

        let rewritten_id = match kind {
            StepKind::Operation(Operation::Scan { table_id, .. }) => {
                let slice_plan = if lookup_count == 0 {
                    self.primary_slice.clone()
                } else {
                    self.foreign_slice.clone()
                };

                let rewritten_kind = StepKind::Operation(Operation::Scan {
                    table_id: *table_id,
                    slice: slice_plan,
                });
                self.add_rewritten_node(original_id, lookup_count, rewritten_kind, smallvec![])
            }
            StepKind::Operation(operation) => {
                let rewritten_args = match operation {
                    Operation::LookupResponse => {
                        let original_foreign_value = args[0];
                        let original_slice_request = args[1];
                        smallvec![
                            self.rewrite(original, original_foreign_value, lookup_count + 1)?,
                            self.rewrite(original, original_slice_request, lookup_count + 1)?
                        ]
                    }
                    Operation::LookupRequest => args
                        .iter()
                        .map(|arg| self.rewrite(original, *arg, lookup_count - 1))
                        .try_collect()?,
                    _ => args
                        .iter()
                        .map(|arg| self.rewrite(original, *arg, lookup_count))
                        .try_collect()?,
                };

                self.add_rewritten_node(original_id, lookup_count, kind.clone(), rewritten_args)
            }
            StepKind::Expression(_) | StepKind::Window(_) | StepKind::Error => {
                let rewritten_args = args
                    .iter()
                    .map(|arg| self.rewrite(original, *arg, lookup_count))
                    .try_collect()?;

                self.add_rewritten_node(original_id, lookup_count, kind.clone(), rewritten_args)
            }
            StepKind::Transform => {
                let original_value_id = args[0];
                let original_operation_id = args[1];

                let operation = match original.kind(original_operation_id) {
                    StepKind::Operation(operation) => operation,
                    unexpected => anyhow::bail!("Expected operation, but was {unexpected:?}"),
                };

                let rewritten_args = match operation {
                    Operation::LookupResponse => {
                        smallvec![
                            self.rewrite(original, original_value_id, lookup_count + 1)?,
                            self.rewrite(original, original_operation_id, lookup_count)?
                        ]
                    }
                    _ => args
                        .iter()
                        .map(|arg| self.rewrite(original, *arg, lookup_count))
                        .try_collect()?,
                };
                self.add_rewritten_node(
                    original_id,
                    lookup_count,
                    StepKind::Transform,
                    rewritten_args,
                )
            }
        }?;

        debug_println!(
            DEBUG_SLICE_ANALYSIS,
            "Rewriting {original_id:?}={kind:?}({args:?}) in {lookup_count:?} => {rewritten_id:?}"
        );
        Ok(rewritten_id)
    }

    fn get_rewritten_id(&self, original_id: Id, lookup_count: usize) -> Option<Id> {
        self.rewritten_ids
            .get(&(original_id, lookup_count))
            .copied()
    }

    fn add_rewritten_node(
        &mut self,
        original_id: Id,
        lookup_count: usize,
        kind: StepKind,
        children: ChildrenVec,
    ) -> anyhow::Result<Id> {
        let rewritten_id = self.rewritten_dfg.add_node(kind, children)?;
        self.rewritten_ids
            .insert((original_id, lookup_count), rewritten_id);
        Ok(rewritten_id)
    }

    fn finish(self, original_output_id: Id) -> anyhow::Result<DfgExpr> {
        let rewritten_output = self
            .get_rewritten_id(original_output_id, 0)
            .context("missing rewritten ID for {original_output_id}")?;
        Ok(self.rewritten_dfg.extract_simplest(rewritten_output))
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use itertools::Itertools;
    use sparrow_api::kaskada::v1alpha::compile_request::ExpressionKind;
    use sparrow_api::kaskada::v1alpha::{ComputeTable, FeatureSet, TableConfig, TableMetadata};
    use uuid::Uuid;

    use super::*;
    use crate::{CompilerOptions, DataContext, FrontendOutput};

    fn data_context() -> DataContext {
        let mut data_context = DataContext::default();
        let table_a_uuid = Uuid::parse_str("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        let table_b_uuid = Uuid::parse_str("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB").unwrap();
        let table_c_uuid = Uuid::parse_str("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC").unwrap();

        // Register tables (all have the same schema with a single `x` value).
        let schema = Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("x", DataType::Int64, true),
        ]);
        let schema = sparrow_api::kaskada::v1alpha::Schema::try_from(&schema).unwrap();
        data_context
            .add_table(ComputeTable {
                config: Some(TableConfig::new_with_table_source(
                    "A",
                    &table_a_uuid,
                    "",
                    Some(""),
                    "key",
                    "grouping",
                )),
                metadata: Some(TableMetadata {
                    schema: Some(schema.clone()),
                    file_count: 0,
                }),
                file_sets: vec![],
            })
            .unwrap();
        data_context
            .add_table(ComputeTable {
                config: Some(TableConfig::new_with_table_source(
                    "B",
                    &table_b_uuid,
                    "",
                    Some(""),
                    "key",
                    "grouping",
                )),
                metadata: Some(TableMetadata {
                    schema: Some(schema.clone()),
                    file_count: 0,
                }),
                file_sets: vec![],
            })
            .unwrap();

        data_context
            .add_table(ComputeTable {
                config: Some(TableConfig::new_with_table_source(
                    "C",
                    &table_c_uuid,
                    "",
                    Some(""),
                    "key",
                    "grouping",
                )),
                metadata: Some(TableMetadata {
                    schema: Some(schema),
                    file_count: 0,
                }),
                file_sets: vec![],
            })
            .unwrap();

        data_context
    }

    fn apply_slice_analysis_get_plans(
        expr: &str,
        slice_request: Option<SliceRequest>,
    ) -> Vec<SlicePlan> {
        // Create a compiler
        let feature_set = FeatureSet {
            formulas: vec![],
            query: expr.to_string(),
        };

        let options = CompilerOptions {
            slice_request,
            ..CompilerOptions::default()
        };

        let mut data_context = data_context();
        let analysis = FrontendOutput::try_compile(
            &mut data_context,
            &feature_set,
            &options,
            ExpressionKind::Formula,
        )
        .unwrap()
        .analysis;

        if analysis.has_errors() {
            panic!(
                "Expression '{}' had errors:\n{}",
                expr,
                analysis.take_diagnostics().iter().format("\n")
            )
        } else {
            analysis.slice_plans
        }
    }

    fn apply_slice_analysis_rewrite_expr(
        expr: &str,
        slice_request: Option<SliceRequest>,
    ) -> String {
        // Create a compiler
        let feature_set = FeatureSet {
            formulas: vec![],
            query: expr.to_string(),
        };

        let options = CompilerOptions {
            slice_request,
            ..CompilerOptions::default()
        };

        let mut data_context = data_context();
        let FrontendOutput {
            analysis,
            expr: dfg,
        } = FrontendOutput::try_compile(
            &mut data_context,
            &feature_set,
            &options,
            ExpressionKind::Formula,
        )
        .unwrap();

        if analysis.has_errors() {
            panic!(
                "Expression '{}' had errors:\n{}",
                expr,
                analysis.take_diagnostics().iter().format("\n")
            )
        } else {
            // Leave room for formatting (indentation) before the strings.
            dfg.pretty(70)
        }
    }

    const FIFTY_PERCENT_SLICE: SliceRequest = SliceRequest {
        slice: Some(slice_request::Slice::Percent(slice_request::PercentSlice {
            percent: 0.5,
        })),
    };

    #[test]
    fn test_slice_plans_simple() {
        let plans = apply_slice_analysis_get_plans("A.x + C.x", Some(FIFTY_PERCENT_SLICE.clone()));
        insta::assert_yaml_snapshot!(plans, @r###"
        ---
        - table_name: A
          slice:
            Percent:
              percent: 0.5
        - table_name: C
          slice:
            Percent:
              percent: 0.5
        "###);
    }

    #[test]
    fn test_slice_plans_lookup() {
        let plans = apply_slice_analysis_get_plans(
            "lookup(A.x, sum(B.x))",
            Some(FIFTY_PERCENT_SLICE.clone()),
        );
        insta::assert_yaml_snapshot!(plans, @r###"
        ---
        - table_name: A
          slice:
            Percent:
              percent: 0.5
        - table_name: B
          slice: ~
        "###);
    }

    #[test]
    fn test_slice_plans_self_lookup_with_slicing() {
        let plans = apply_slice_analysis_get_plans(
            "lookup(A.x, sum(A.x + C.x))",
            Some(FIFTY_PERCENT_SLICE.clone()),
        );
        insta::assert_yaml_snapshot!(plans, @r###"
        ---
        - table_name: A
          slice: ~
        - table_name: A
          slice:
            Percent:
              percent: 0.5
        - table_name: C
          slice: ~
        "###);
    }

    #[test]
    fn test_slice_plans_self_lookup_without_slicing() {
        // This test demonstrates the need to deduplicate between the primary and
        // foreign. Specifically, the table `A` is used in both the primary and
        // foreign position. Because there is no slicing requested, these are
        // identical requests, which we wish to de-duplicate.
        let plans = apply_slice_analysis_get_plans("lookup(A.x, sum(A.x + C.x))", None);
        insta::assert_yaml_snapshot!(plans, @r###"
        ---
        - table_name: A
          slice: ~
        - table_name: C
          slice: ~
        "###);
    }

    #[test]
    fn test_slice_plans_nested_lookup_same_table_with_slicing() {
        // This test has a lookup within a lookup, leading to the same
        // table being necessary sliced and unsliced.
        let plans = apply_slice_analysis_get_plans(
            "lookup(A.x, lookup(A.x, sum(C.x)))",
            Some(FIFTY_PERCENT_SLICE.clone()),
        );
        insta::assert_yaml_snapshot!(plans, @r###"
        ---
        - table_name: A
          slice: ~
        - table_name: A
          slice:
            Percent:
              percent: 0.5
        - table_name: C
          slice: ~
        "###);
    }

    #[test]
    fn test_slice_plans_nested_lookup_with_slicing() {
        // This test has a lookup within a lookup.
        let plans = apply_slice_analysis_get_plans(
            "lookup(A.x, lookup(B.x, sum(C.x)))",
            Some(FIFTY_PERCENT_SLICE.clone()),
        );
        insta::assert_yaml_snapshot!(plans, @r###"
      ---
      - table_name: A
        slice:
          Percent:
            percent: 0.5
      - table_name: B
        slice: ~
      - table_name: C
        slice: ~
      "###);
    }

    #[test]
    fn test_rewrite_query_simple_none() {
        insta::assert_snapshot!(apply_slice_analysis_rewrite_expr("A.x + C.x", None), @r###"
        (add
          (transform
            (field
              scan:cccccccc-cccc-cccc-cccc-cccccccccccc
              (\"x\ scan:cccccccc-cccc-cccc-cccc-cccccccccccc)
              scan:cccccccc-cccc-cccc-cccc-cccccccccccc)
            (merge_join
              scan:cccccccc-cccc-cccc-cccc-cccccccccccc
              scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa))
          (transform
            (field
              scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
              (\"x\ scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
              scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
            (merge_join
              scan:cccccccc-cccc-cccc-cccc-cccccccccccc
              scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa))
          (merge_join
            scan:cccccccc-cccc-cccc-cccc-cccccccccccc
            scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa))
        "###);
    }

    #[test]
    fn test_rewrite_query_simple_fifty_percent() {
        insta::assert_snapshot!(apply_slice_analysis_rewrite_expr("A.x + C.x", Some(FIFTY_PERCENT_SLICE.clone())), @r###"
        (add
          (transform
            (field
              scan:cccccccc-cccc-cccc-cccc-cccccccccccc:Percent(PercentSlice { percent: 0.5 })
              (\"x\
                scan:cccccccc-cccc-cccc-cccc-cccccccccccc:Percent(PercentSlice { percent: 0.5 }))
              scan:cccccccc-cccc-cccc-cccc-cccccccccccc:Percent(PercentSlice { percent: 0.5 }))
            (merge_join
              scan:cccccccc-cccc-cccc-cccc-cccccccccccc:Percent(PercentSlice { percent: 0.5 })
              scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })))
          (transform
            (field
              scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
              (\"x\
                scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 }))
              scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 }))
            (merge_join
              scan:cccccccc-cccc-cccc-cccc-cccccccccccc:Percent(PercentSlice { percent: 0.5 })
              scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })))
          (merge_join
            scan:cccccccc-cccc-cccc-cccc-cccccccccccc:Percent(PercentSlice { percent: 0.5 })
            scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })))
        "###);
    }

    #[test]
    fn test_rewrite_query_lookup_none() {
        insta::assert_snapshot!(apply_slice_analysis_rewrite_expr("lookup(A.x, sum(B.x))", None), @r###"
        (transform
          (transform
            (transform
              (sum
                (field
                  scan:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb
                  (\"x\ scan:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb)
                  scan:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb)
                (null scan:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb)
                (null scan:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb)
                scan:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb)
              (merge_join
                (lookup_request
                  (field
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                    (\"x\ scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa))
                scan:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb))
            (lookup_response
              (lookup_request
                (field
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                  (\"x\ scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa))
              (merge_join
                (lookup_request
                  (field
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                    (\"x\ scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa))
                scan:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb)))
          (merge_join
            scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
            (lookup_response
              (lookup_request
                (field
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                  (\"x\ scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa))
              (merge_join
                (lookup_request
                  (field
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                    (\"x\ scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa))
                scan:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb))))
        "###);
    }

    #[test]
    fn test_rewrite_query_lookup_fifty_percent() {
        insta::assert_snapshot!(apply_slice_analysis_rewrite_expr("lookup(A.x, sum(B.x))", Some(FIFTY_PERCENT_SLICE.clone())), @r###"
        (transform
          (transform
            (transform
              (sum
                (field
                  scan:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb
                  (\"x\ scan:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb)
                  scan:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb)
                (null scan:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb)
                (null scan:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb)
                scan:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb)
              (merge_join
                (lookup_request
                  (field
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
                    (\"x\
                      scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 }))
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })))
                scan:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb))
            (lookup_response
              (lookup_request
                (field
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
                  (\"x\
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 }))
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })))
              (merge_join
                (lookup_request
                  (field
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
                    (\"x\
                      scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 }))
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })))
                scan:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb)))
          (merge_join
            scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
            (lookup_response
              (lookup_request
                (field
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
                  (\"x\
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 }))
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })))
              (merge_join
                (lookup_request
                  (field
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
                    (\"x\
                      scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 }))
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })))
                scan:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb))))
        "###);
    }

    #[test]
    fn test_rewrite_query_self_lookup_none() {
        insta::assert_snapshot!(apply_slice_analysis_rewrite_expr("lookup(A.x, sum(A.x + C.x))", None), @r###"
        (transform
          (transform
            (transform
              (sum
                (if
                  (logical_or
                    (transform
                      (is_valid
                        scan:cccccccc-cccc-cccc-cccc-cccccccccccc
                        scan:cccccccc-cccc-cccc-cccc-cccccccccccc)
                      (merge_join
                        scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                        scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
                    (transform
                      (is_valid
                        scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                        scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                      (merge_join
                        scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                        scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
                    (merge_join
                      scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                      scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
                  (add
                    (transform
                      (field
                        scan:cccccccc-cccc-cccc-cccc-cccccccccccc
                        (\"x\ scan:cccccccc-cccc-cccc-cccc-cccccccccccc)
                        scan:cccccccc-cccc-cccc-cccc-cccccccccccc)
                      (merge_join
                        scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                        scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
                    (transform
                      (field
                        scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                        (\"x\ scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                        scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                      (merge_join
                        scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                        scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
                    (merge_join
                      scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                      scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
                  (merge_join
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                    scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
                (null
                  (merge_join
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                    scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
                (null
                  (merge_join
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                    scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
                (merge_join
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                  scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
              (merge_join
                (lookup_request
                  (field
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                    (\"x\ scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa))
                (merge_join
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                  scan:cccccccc-cccc-cccc-cccc-cccccccccccc)))
            (lookup_response
              (lookup_request
                (field
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                  (\"x\ scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa))
              (merge_join
                (lookup_request
                  (field
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                    (\"x\ scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa))
                (merge_join
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                  scan:cccccccc-cccc-cccc-cccc-cccccccccccc))))
          (merge_join
            scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
            (lookup_response
              (lookup_request
                (field
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                  (\"x\ scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa))
              (merge_join
                (lookup_request
                  (field
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                    (\"x\ scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa))
                (merge_join
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                  scan:cccccccc-cccc-cccc-cccc-cccccccccccc)))))
        "###);
    }

    #[test]
    fn test_rewrite_query_self_lookup_fifty_percent() {
        insta::assert_snapshot!(apply_slice_analysis_rewrite_expr("lookup(A.x, sum(A.x + C.x))", Some(FIFTY_PERCENT_SLICE.clone())), @r###"
        (transform
          (transform
            (transform
              (sum
                (if
                  (logical_or
                    (transform
                      (is_valid
                        scan:cccccccc-cccc-cccc-cccc-cccccccccccc
                        scan:cccccccc-cccc-cccc-cccc-cccccccccccc)
                      (merge_join
                        scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                        scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
                    (transform
                      (is_valid
                        scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                        scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                      (merge_join
                        scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                        scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
                    (merge_join
                      scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                      scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
                  (add
                    (transform
                      (field
                        scan:cccccccc-cccc-cccc-cccc-cccccccccccc
                        (\"x\ scan:cccccccc-cccc-cccc-cccc-cccccccccccc)
                        scan:cccccccc-cccc-cccc-cccc-cccccccccccc)
                      (merge_join
                        scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                        scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
                    (transform
                      (field
                        scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                        (\"x\ scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                        scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                      (merge_join
                        scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                        scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
                    (merge_join
                      scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                      scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
                  (merge_join
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                    scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
                (null
                  (merge_join
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                    scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
                (null
                  (merge_join
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                    scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
                (merge_join
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                  scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
              (merge_join
                (lookup_request
                  (field
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
                    (\"x\
                      scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 }))
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })))
                (merge_join
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                  scan:cccccccc-cccc-cccc-cccc-cccccccccccc)))
            (lookup_response
              (lookup_request
                (field
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
                  (\"x\
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 }))
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })))
              (merge_join
                (lookup_request
                  (field
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
                    (\"x\
                      scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 }))
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })))
                (merge_join
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                  scan:cccccccc-cccc-cccc-cccc-cccccccccccc))))
          (merge_join
            scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
            (lookup_response
              (lookup_request
                (field
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
                  (\"x\
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 }))
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })))
              (merge_join
                (lookup_request
                  (field
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
                    (\"x\
                      scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 }))
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })))
                (merge_join
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                  scan:cccccccc-cccc-cccc-cccc-cccccccccccc)))))
        "###);
    }

    #[test]
    fn test_rewrite_query_unused_let_none() {
        insta::assert_snapshot!(apply_slice_analysis_rewrite_expr("let unused = B.x in lookup(A.x, sum(C.x))", None), @r###"
        (transform
          (transform
            (transform
              (sum
                (field
                  scan:cccccccc-cccc-cccc-cccc-cccccccccccc
                  (\"x\ scan:cccccccc-cccc-cccc-cccc-cccccccccccc)
                  scan:cccccccc-cccc-cccc-cccc-cccccccccccc)
                (null scan:cccccccc-cccc-cccc-cccc-cccccccccccc)
                (null scan:cccccccc-cccc-cccc-cccc-cccccccccccc)
                scan:cccccccc-cccc-cccc-cccc-cccccccccccc)
              (merge_join
                (lookup_request
                  (field
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                    (\"x\ scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa))
                scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
            (lookup_response
              (lookup_request
                (field
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                  (\"x\ scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa))
              (merge_join
                (lookup_request
                  (field
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                    (\"x\ scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa))
                scan:cccccccc-cccc-cccc-cccc-cccccccccccc)))
          (merge_join
            scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
            (lookup_response
              (lookup_request
                (field
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                  (\"x\ scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa))
              (merge_join
                (lookup_request
                  (field
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                    (\"x\ scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa))
                scan:cccccccc-cccc-cccc-cccc-cccccccccccc))))
        "###);
    }

    #[test]
    fn test_rewrite_query_unused_let_fifty_percent() {
        insta::assert_snapshot!(apply_slice_analysis_rewrite_expr("let unused = B.x in lookup(A.x, sum(C.x))", Some(FIFTY_PERCENT_SLICE.clone())), @r###"
        (transform
          (transform
            (transform
              (sum
                (field
                  scan:cccccccc-cccc-cccc-cccc-cccccccccccc
                  (\"x\ scan:cccccccc-cccc-cccc-cccc-cccccccccccc)
                  scan:cccccccc-cccc-cccc-cccc-cccccccccccc)
                (null scan:cccccccc-cccc-cccc-cccc-cccccccccccc)
                (null scan:cccccccc-cccc-cccc-cccc-cccccccccccc)
                scan:cccccccc-cccc-cccc-cccc-cccccccccccc)
              (merge_join
                (lookup_request
                  (field
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
                    (\"x\
                      scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 }))
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })))
                scan:cccccccc-cccc-cccc-cccc-cccccccccccc))
            (lookup_response
              (lookup_request
                (field
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
                  (\"x\
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 }))
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })))
              (merge_join
                (lookup_request
                  (field
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
                    (\"x\
                      scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 }))
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })))
                scan:cccccccc-cccc-cccc-cccc-cccccccccccc)))
          (merge_join
            scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
            (lookup_response
              (lookup_request
                (field
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
                  (\"x\
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 }))
                  scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })))
              (merge_join
                (lookup_request
                  (field
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })
                    (\"x\
                      scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 }))
                    scan:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:Percent(PercentSlice { percent: 0.5 })))
                scan:cccccccc-cccc-cccc-cccc-cccccccccccc))))
        "###);
    }
}
