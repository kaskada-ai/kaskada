//! Functionality for analyzing the AST before conversion to the DFG.

mod first_reference;
mod free_variable;
mod incremental_enabled;
mod output_types;
mod parse_expr;
mod parse_feature_set;
pub(crate) mod resolve_arguments;
mod slice_analysis;

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::anyhow;
use arrow::datatypes::{DataType, TimeUnit};
use smallvec::smallvec;
use sparrow_api::kaskada::v1alpha::compile_request::ExpressionKind;
use sparrow_api::kaskada::v1alpha::{FeatureSet, LateBoundValue, PerEntityBehavior, SlicePlan};
use sparrow_instructions::GroupId;
use sparrow_syntax::{FeatureSetPart, FenlType, Location};
use tracing::error;

use self::resolve_arguments::resolve_recursive;
use crate::dfg::{Dfg, DfgExpr, Expression};
use crate::frontend::parse_feature_set::ParsedFeatureSet;
use crate::time_domain::TimeDomain;
use crate::{
    ast_to_dfg, AstDfg, AstDfgRef, CollectedDiagnostic, CompilerOptions, DataContext,
    DiagnosticCode, DiagnosticCollector,
};

/// The results of "frontend" compilation.
///
/// The frontend performs parsing, type-checking, some simplification, and
/// various analyses that are part of understanding and validating the
/// user-provided query.
#[derive(Debug)]
#[non_exhaustive]
pub struct FrontendAnalysis {
    pub num_errors: usize,
    pub diagnostics: Vec<CollectedDiagnostic>,
    pub result_type: FenlType,
    pub slice_plans: Vec<SlicePlan>,
    pub free_names: BTreeSet<String>,
    pub defined_names: BTreeSet<String>,
    /// If true, then the snapshot for this query must have a `max event time`
    /// less than or equal to the `changed_since_time`.
    ///
    /// If true and there is no `changed_since_time`, then no snapshot is
    /// possible.
    pub must_start_before_changed_since_time: bool,
    /// Whether incremental is enabled.
    pub incremental_enabled: bool,
    pub primary_grouping: Option<GroupId>,
}

impl FrontendAnalysis {
    pub fn has_errors(&self) -> bool {
        self.num_errors > 0
    }

    pub fn num_errors(&self) -> usize {
        self.num_errors
    }

    pub fn diagnostics(&self) -> &[CollectedDiagnostic] {
        &self.diagnostics
    }

    pub fn result_type(&self) -> &FenlType {
        &self.result_type
    }

    pub fn take_diagnostics(self) -> Vec<CollectedDiagnostic> {
        self.diagnostics
    }

    pub fn slice_plans(&self) -> &[SlicePlan] {
        &self.slice_plans
    }

    pub fn free_names(&self) -> &BTreeSet<String> {
        &self.free_names
    }

    pub fn defined_names(&self) -> &BTreeSet<String> {
        &self.defined_names
    }
}

pub(super) struct FrontendOutput {
    /// The analysis information produced by the frontend compilation.
    pub(super) analysis: FrontendAnalysis,
    /// The DFG produced by the frontend compilation.
    pub(super) expr: DfgExpr,
}

const CHANGED_SINCE_DECORATION: &str = "result | when(time_of($input) >= __changed_since_time__)";
const FINAL_QUERY_AT_TIME_DECORATION: &str = "result | when(time_of($input) <= __final_at_time__ \
                                              ) | last() | when(last(time_of($input)) >= \
                                              __changed_since_time__ and finished())";
const FINAL_QUERY_DECORATION: &str =
    "result | last() | when(last(time_of(result)) >= __changed_since_time__ and finished())";

impl FrontendOutput {
    /// Perform frontend-compilation of the given feature set.
    ///
    /// Returns the results of frontend compilation as well as the resulting
    /// DFG.
    pub(crate) fn try_compile(
        data_context: &mut DataContext,
        feature_set: &FeatureSet,
        options: &CompilerOptions,
        expression_kind: ExpressionKind,
    ) -> anyhow::Result<Self> {
        let mut dfg = data_context.create_dfg()?;
        let mut diagnostics = DiagnosticCollector::new(feature_set);

        let parsed = ParsedFeatureSet::try_new(feature_set, &mut diagnostics)?;
        for formula in parsed.formulas.into_iter() {
            debug_assert!(
                !dfg.is_bound(formula.name),
                "Unexpected: Formula '{}' is already bound",
                formula.name
            );
            let expr_dfg = ast_to_dfg(data_context, &mut dfg, &mut diagnostics, &formula.expr)?;
            dfg.bind(formula.name, expr_dfg);
        }

        // Add the query.
        let query = ast_to_dfg(data_context, &mut dfg, &mut diagnostics, &parsed.query_expr)?;
        let result_type = query.value_type().clone();
        let primary_grouping = query.grouping();

        // Verify the output type is a `DataType::Struct`.
        let executable = if expression_kind == ExpressionKind::Complete {
            match &result_type {
                FenlType::Concrete(DataType::Struct(fields)) => {
                    output_types::emit_diagnostic_for_unencodable_fields(
                        &query,
                        fields.as_ref(),
                        &mut diagnostics,
                    )?;
                    true
                }
                FenlType::Error => {
                    // We can't determine whether the inner expression would
                    // have produced a record, had it been a
                    // valid inner expression, so
                    // we shouldn't report an unnecessary error regarding the
                    // output type until the inner
                    // expression errors are corrected.
                    false
                }
                invalid => {
                    DiagnosticCode::InvalidOutputType
                        .builder()
                        .with_note(format!("Output type must be a record, but was {invalid}"))
                        .emit(&mut diagnostics);
                    false
                }
            }
        } else {
            false
        };

        let result_node = decorate(
            data_context,
            &mut dfg,
            &mut diagnostics,
            executable,
            query,
            options.per_entity_behavior,
        )?;

        // Create the basic analysis information.
        let num_errors = diagnostics.num_errors();
        let diagnostics = diagnostics.finish();

        // TODO: We should be able to use dependency information computed by
        // formula_ordering to determine the free names.
        let defined_names = data_context
            .table_infos()
            .map(|info| info.name().to_owned())
            .chain(feature_set.formulas.iter().map(|f| f.name.clone()))
            .collect();

        // Prune the DFG.
        //
        // We do this before dumping it so that any unneeded nodes are removed.
        let result_node = dfg.prune(result_node)?;

        // Dump the DFG dot file if requested.
        if let Some(dot_path) = &options.internal.store_initial_dfg {
            let expr = dfg.extract_simplest(result_node);
            if let Err(e) = expr.write_dot(dot_path) {
                error!("Failed to write DFG to '{:?}': {}", dot_path, e)
            }
        }

        // Create the simplified DFG
        dfg.run_simplifications(options)?;
        let dfg = dfg.extract_simplest(result_node);
        let dfg = crate::dfg::remove_useless_transforms(dfg)?;

        let incremental_enabled = incremental_enabled::is_incremental_enabled(&dfg, options);
        tracing::info!("Incremental compute is enabled: {:?}", incremental_enabled);

        // Perform the slice analysis and use it to rewrite the DFG.
        let dfg = slice_analysis::rewrite_slices(dfg, &options.slice_request)?;
        let slice_plans = slice_analysis::slice_plans(&dfg, data_context)?;

        // Dump the final (post-optimization and rewriting) DFG.
        if let Some(dot_path) = &options.internal.store_final_dfg {
            if let Err(e) = dfg.write_dot(dot_path) {
                error!("Failed to write final DFG to '{:?}': {}", dot_path, e)
            }
        }

        // Determine max allowed event time (for the purposes of resume).
        let must_start_before_changed_since_time = match options.per_entity_behavior {
            PerEntityBehavior::Unspecified => {
                anyhow::bail!(
                    "Unsupported query behavior: {:?}",
                    options.per_entity_behavior
                )
            }
            PerEntityBehavior::All => {
                // For `all` queries, we need to replay events since the changed-since time.
                true
            }
            PerEntityBehavior::Final => {
                // For final queries, we can replay from any time as long as
                // we incorporate all events before we output results.
                false
            }
            PerEntityBehavior::FinalAtTime => {
                // For final queries, we can replay from any time as long as
                // we incorporate all events before we output results.
                //
                // TODO: Verify the logic of this variable with change since + queries final at
                false
            }
        };

        let analysis = FrontendAnalysis {
            num_errors,
            diagnostics,
            result_type,
            slice_plans,
            free_names: parsed.free_names,
            defined_names,
            must_start_before_changed_since_time,
            incremental_enabled,
            primary_grouping,
        };
        Ok(Self {
            analysis,
            expr: dfg,
        })
    }
}

pub fn decorate(
    data_context: &mut DataContext,
    dfg: &mut Dfg,
    diagnostics: &mut DiagnosticCollector<'_>,
    executable: bool,
    query: AstDfgRef,
    per_entity_behavior: PerEntityBehavior,
) -> anyhow::Result<egg::Id> {
    match per_entity_behavior {
        _ if !executable => {
            // Don't decorate incomplete or otherwise non-executable expressions.
            // We don't produce executable plans for incomplete expressions.
            Ok(query.value())
        }
        PerEntityBehavior::All => {
            dfg.enter_env();
            dfg.bind("result", query);
            let time_node = create_changed_since_time_node(dfg)?;
            dfg.bind("__changed_since_time__", time_node);
            let decorated =
                add_decoration(data_context, diagnostics, dfg, CHANGED_SINCE_DECORATION)?;
            dfg.exit_env();
            Ok(decorated.value())
        }
        PerEntityBehavior::Final => {
            dfg.enter_env();
            dfg.bind("result", query);

            // Treat FINAL queries as changed_since_time of 0
            let time_node = create_changed_since_time_node(dfg)?;
            dfg.bind("__changed_since_time__", time_node);

            let decorated = add_decoration(data_context, diagnostics, dfg, FINAL_QUERY_DECORATION)?;
            dfg.exit_env();
            Ok(decorated.value())
        }
        PerEntityBehavior::FinalAtTime => {
            dfg.enter_env();
            dfg.bind("result", query);

            // Treat FINAL queries as changed_since_time of 0
            let time_node = create_changed_since_time_node(dfg)?;
            dfg.bind("__changed_since_time__", time_node);

            let time_node = create_final_at_time_time_node(dfg)?;
            dfg.bind("__final_at_time__", time_node);

            // 1. If the final query time is provided then use it as the query final time in
            // the special decorator 2. Use the same per entity behavior
            // final for all of them
            let decorated = add_decoration(
                data_context,
                diagnostics,
                dfg,
                FINAL_QUERY_AT_TIME_DECORATION,
            )?;
            dfg.exit_env();
            Ok(decorated.value())
        }
        PerEntityBehavior::Unspecified => {
            anyhow::bail!("Unspecified per entity behavior")
        }
    }
}

/// Adds the given decoration to the dfg.
fn add_decoration(
    data_context: &mut DataContext,
    diagnostics: &mut DiagnosticCollector<'_>,
    dfg: &mut Dfg,
    decoration: &'static str,
) -> anyhow::Result<AstDfgRef> {
    let decorated = parse_expr::parse_expr(FeatureSetPart::Internal(decoration), decoration)
        .map_err(|err| anyhow!("Failed to parse {:?}: {:?}", decoration, err))?;

    let mut diagnostic_builder = Vec::new();
    let resolved = resolve_recursive(&decorated, &mut diagnostic_builder)?;

    if !diagnostic_builder.is_empty() {
        diagnostics.collect_all(diagnostic_builder)
    };

    // TODO: Can use trait that implements `extend_one` for diagnostic
    ast_to_dfg(data_context, dfg, diagnostics, &resolved)
}

/// Creates a changed_since_time node in the dfg.
///
/// This is a dynamically injectible node, where the value will be inserted
/// during runtime.
fn create_changed_since_time_node(dfg: &mut Dfg) -> anyhow::Result<AstDfgRef> {
    let value = dfg.add_expression(
        Expression::LateBound(LateBoundValue::ChangedSinceTime),
        smallvec![],
    )?;
    let value_type = FenlType::Concrete(DataType::Timestamp(TimeUnit::Nanosecond, None));
    let is_new = dfg.add_literal(false)?;
    Ok(Arc::new(AstDfg::new(
        value,
        is_new,
        value_type,
        None,
        TimeDomain::literal(),
        // TODO: https://gitlab.com/kaskada/kaskada/-/issues/503
        Location::internal_str("changed_since_time"),
        None,
    )))
}

/// Creates a final_at_time node in the dfg.
///
/// This is a dynamically injectible node, where the value will be inserted
/// during runtime.
fn create_final_at_time_time_node(dfg: &mut Dfg) -> anyhow::Result<AstDfgRef> {
    let value = dfg.add_expression(
        Expression::LateBound(LateBoundValue::FinalAtTime),
        smallvec![],
    )?;
    let value_type = FenlType::Concrete(DataType::Timestamp(TimeUnit::Nanosecond, None));
    let is_new = dfg.add_literal(false)?;
    Ok(Arc::new(AstDfg::new(
        value,
        is_new,
        value_type,
        None,
        TimeDomain::literal(),
        Location::internal_str("final_at_time"),
        None,
    )))
}
