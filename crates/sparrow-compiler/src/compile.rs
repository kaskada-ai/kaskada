use std::io::Write;

use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use prost::Message;
use sparrow_api::kaskada::v1alpha::compile_request::ExpressionKind;
use sparrow_api::kaskada::v1alpha::FenlDiagnostics;
use sparrow_api::kaskada::v1alpha::{CompileRequest, CompileResponse, FeatureSet, PlanHash};
use tracing::{error, info, info_span};

use crate::{
    CompilerOptions, DataContext, Error, FrontendAnalysis, FrontendOutput, InternalCompileOptions,
};

/// Compile the query in the `request` and return the `CompileResponse` proto.
///
/// The `internal` options may be used to configure other properties of the
/// execution, such as the limit of simplification. These are not available
/// as part of the query API, so they are passed in separately.
pub async fn compile_proto(
    request: CompileRequest,
    internal: InternalCompileOptions,
) -> error_stack::Result<CompileResponse, Error> {
    let expression_kind = request.expression_kind();
    let per_entity_behavior = request.per_entity_behavior();

    let feature_set = request
        .feature_set
        .ok_or(Error::MissingField("feature_set"))?;

    let compiler_options = CompilerOptions {
        slice_request: request.slice_request,
        experimental: request.experimental,
        per_entity_behavior,
        internal,
    };

    let mut data_context = DataContext::try_from_tables(request.tables)
        .into_report()
        .change_context(Error::CompileError)?;

    compile(
        &compiler_options,
        &mut data_context,
        &feature_set,
        expression_kind,
    )
}

/// Compile a feature set and return the corresponding compile response.
///
/// This is currently only used by the batch CLI execption. It can probably
/// be changed to use `compile_proto`, and then this (and a bunch of the code
/// in this file) can be simplified.
fn compile(
    options: &CompilerOptions,
    data_context: &mut DataContext,
    feature_set: &FeatureSet,
    expression_kind: ExpressionKind,
) -> error_stack::Result<CompileResponse, Error> {
    let span = info_span!("Compiling query");
    let _enter = span.enter();

    // 1. Do the frontend analysis / compilation.
    let FrontendOutput { analysis, expr } =
        FrontendOutput::try_compile(data_context, feature_set, options, expression_kind)
            .into_report()
            .change_context(Error::CompileError)?;

    // 2. Produce the plan (assuming there were no diagnostic errors).
    let (plan, plan_hash) = if analysis.has_errors() {
        info!("Not producing plan due to Fenl errors");
        (None, None)
    } else if !matches!(expression_kind, ExpressionKind::Complete) {
        info!(
            "Not producing plan for incomplete expression kind {:?}",
            expression_kind
        );
        (None, None)
    } else {
        let primary_grouping = analysis
            .primary_grouping
            .ok_or(Error::Internal("missing primary_grouping"))?;
        let primary_grouping_info = data_context
            .group_info(primary_grouping)
            .ok_or(Error::Internal("missing primary group_info"))?;

        let primary_grouping = primary_grouping_info.name().to_owned();
        let primary_grouping_key_type = primary_grouping_info.key_type();

        let plan = crate::plan::extract_plan_proto(
            data_context,
            expr,
            options.per_entity_behavior,
            primary_grouping,
            primary_grouping_key_type,
        )
        .into_report()
        .change_context(Error::ExtractPlanProto)?;

        let plan_hash = hash_compute_plan_proto(&plan);

        if let Some(graph_path) = &options.internal.store_plan_graph {
            if let Err(err) = plan.write_to_graphviz_path(graph_path) {
                error!(
                    "Failed to write plan to graphviz file {graph_path:?}: {err:?}.\nPlan: \
                     {plan:?}"
                );
            }
        }

        (Some(plan), Some(plan_hash))
    };

    // 3. Create the CompileResponse proto.
    let FrontendAnalysis {
        num_errors,
        diagnostics,
        result_type,
        slice_plans,
        free_names,
        defined_names,
        incremental_enabled,
        ..
    } = analysis;

    let missing_names = free_names.difference(&defined_names).cloned().collect();
    let fenl_diagnostics = diagnostics.into_iter().map(|d| d.into()).collect();
    let result_type = Some(
        sparrow_api::kaskada::v1alpha::DataType::try_from(&result_type)
            .into_report()
            .change_context(Error::Internal("failed to encode result_type"))?,
    );

    Ok(CompileResponse {
        missing_names,
        fenl_diagnostics: Some(FenlDiagnostics {
            fenl_diagnostics,
            num_errors: num_errors as i64,
        }),
        plan,
        result_type,
        free_names: free_names.into_iter().collect(),
        table_slices: slice_plans,
        incremental_enabled,
        plan_hash,
    })
}

pub fn hash_compute_plan_proto(plan: &sparrow_api::kaskada::v1alpha::ComputePlan) -> PlanHash {
    use sha2::Digest;

    // Not all of the compute plan components are hashable, so we
    // instead serialize the proto and hash the resulting bytes.
    let mut hasher = sha2::Sha224::new();

    hasher
        .write_all(&plan.encode_to_vec())
        .expect("writing to hasher");

    let hash = hasher.finalize().to_vec();
    PlanHash { hash }
}

#[cfg(test)]
mod tests {
    use sparrow_api::kaskada::v1alpha::{data_type, schema, DataType, Schema};
    use sparrow_api::kaskada::v1alpha::{
        ComputeTable, Formula, PerEntityBehavior, TableConfig, TableMetadata,
    };
    use uuid::Uuid;

    use super::*;
    use crate::{CompilerOptions, DataContext};

    fn analyze_input_schema() -> Schema {
        Schema {
            fields: vec![
                schema::Field {
                    name: "time".to_owned(),
                    data_type: Some(DataType {
                        kind: Some(data_type::Kind::Primitive(
                            data_type::PrimitiveType::TimestampNanosecond as i32,
                        )),
                    }),
                    nullable: false,
                },
                schema::Field {
                    name: "subsort".to_owned(),
                    data_type: Some(DataType {
                        kind: Some(data_type::Kind::Primitive(
                            data_type::PrimitiveType::I32 as i32,
                        )),
                    }),
                    nullable: false,
                },
                schema::Field {
                    name: "entity".to_owned(),
                    data_type: Some(DataType {
                        kind: Some(data_type::Kind::Primitive(
                            data_type::PrimitiveType::String as i32,
                        )),
                    }),
                    nullable: false,
                },
                schema::Field {
                    name: "str".to_owned(),
                    data_type: Some(DataType {
                        kind: Some(data_type::Kind::Primitive(
                            data_type::PrimitiveType::String as i32,
                        )),
                    }),
                    nullable: false,
                },
            ],
        }
    }

    fn get_plan_hash(tables: Vec<ComputeTable>, feature_set: &FeatureSet) -> Option<PlanHash> {
        let options = CompilerOptions::default();
        get_plan_hash_with_options(tables, feature_set, &options)
    }

    fn get_plan_hash_with_options(
        tables: Vec<ComputeTable>,
        feature_set: &FeatureSet,
        options: &CompilerOptions,
    ) -> Option<PlanHash> {
        let mut data_context = DataContext::try_from_tables(tables).unwrap();
        compile(
            options,
            &mut data_context,
            feature_set,
            ExpressionKind::Complete,
        )
        .unwrap()
        .plan_hash
    }

    #[test]
    fn test_hash_plan_table_order_one_used() {
        let table1 = ComputeTable {
            config: Some(TableConfig::new_with_table_source(
                "Table1",
                &Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap(),
                "time",
                Some("subsort"),
                "entity",
                "grouping",
            )),
            file_sets: vec![],
            metadata: Some(TableMetadata {
                schema: Some(analyze_input_schema()),
                file_count: 0,
            }),
        };
        let table2 = ComputeTable {
            config: Some(TableConfig::new_with_table_source(
                "Table2",
                &Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A9").unwrap(),
                "time",
                Some("subsort"),
                "entity",
                "grouping",
            )),
            file_sets: vec![],
            metadata: Some(TableMetadata {
                schema: Some(analyze_input_schema()),
                file_count: 0,
            }),
        };

        let feature_set = FeatureSet {
            formulas: vec![],
            query: "{x: Table1.str as i64, y: Table1.str }".to_owned(),
        };

        let result1 = get_plan_hash(vec![table1.clone(), table2.clone()], &feature_set);
        let result2 = get_plan_hash(vec![table2, table1], &feature_set);
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_hash_plan_extra_views_unused() {
        let table1 = ComputeTable {
            config: Some(TableConfig::new_with_table_source(
                "Table1",
                &Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap(),
                "time",
                Some("subsort"),
                "entity",
                "grouping",
            )),
            file_sets: vec![],
            metadata: Some(TableMetadata {
                schema: Some(analyze_input_schema()),
                file_count: 0,
            }),
        };

        let feature_set = FeatureSet {
            formulas: vec![],
            query: "{x: Table1.str as i64, y: Table1.str }".to_owned(),
        };
        let feature_set_unused_bound = FeatureSet {
            formulas: vec![],
            query: "let foo = last(Table1.str) in {x: Table1.str as i64, y: Table1.str }"
                .to_owned(),
        };

        let result1 = get_plan_hash(vec![table1.clone()], &feature_set);
        let result2 = get_plan_hash(vec![table1], &feature_set_unused_bound);
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_hash_plan_extra_views_unordered() {
        let table1 = ComputeTable {
            config: Some(TableConfig::new_with_table_source(
                "Table1",
                &Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap(),
                "time",
                Some("subsort"),
                "entity",
                "grouping",
            )),
            file_sets: vec![],
            metadata: Some(TableMetadata {
                schema: Some(analyze_input_schema()),
                file_count: 0,
            }),
        };

        let feature_set_1 = FeatureSet {
            formulas: vec![
                Formula {
                    name: "bar".to_owned(),
                    formula: "first(Table1.str)".to_owned(),
                    source_location: "bar".to_owned(),
                },
                Formula {
                    name: "foo".to_owned(),
                    formula: "last(Table1.str)".to_owned(),
                    source_location: "foo".to_owned(),
                },
            ],
            query: "{x: foo, y: bar }".to_owned(),
        };
        let feature_set_2 = FeatureSet {
            formulas: vec![
                Formula {
                    name: "foo".to_owned(),
                    formula: "last(Table1.str)".to_owned(),
                    source_location: "foo".to_owned(),
                },
                Formula {
                    name: "bar".to_owned(),
                    formula: "first(Table1.str)".to_owned(),
                    source_location: "bar".to_owned(),
                },
            ],
            query: "{x: foo, y: bar }".to_owned(),
        };

        let result1 = get_plan_hash(vec![table1.clone()], &feature_set_1);
        let result2 = get_plan_hash(vec![table1], &feature_set_2);
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_hash_plan_extra_tables_one_used() {
        let table1 = ComputeTable {
            config: Some(TableConfig::new_with_table_source(
                "Table1",
                &Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap(),
                "time",
                Some("subsort"),
                "entity",
                "grouping",
            )),
            file_sets: vec![],
            metadata: Some(TableMetadata {
                schema: Some(analyze_input_schema()),
                file_count: 0,
            }),
        };
        let table2 = ComputeTable {
            config: Some(TableConfig::new_with_table_source(
                "Table2",
                &Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A9").unwrap(),
                "time",
                Some("subsort"),
                "entity",
                "grouping",
            )),
            file_sets: vec![],
            metadata: Some(TableMetadata {
                schema: Some(analyze_input_schema()),
                file_count: 0,
            }),
        };

        let feature_set = FeatureSet {
            formulas: vec![],
            query: "{x: Table1.str as i64, y: Table1.str }".to_owned(),
        };

        let result1 = get_plan_hash(vec![table1.clone()], &feature_set);
        let result2 = get_plan_hash(vec![table2, table1], &feature_set);
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_hash_plan_final_independent_of_grouping_order() {
        let table1 = ComputeTable {
            config: Some(TableConfig::new_with_table_source(
                "Table1",
                &Uuid::parse_str("e4511dc4-5e96-47b9-9e8f-fb792f289d49").unwrap(),
                "time",
                Some("subsort"),
                "entity",
                "grouping1",
            )),
            file_sets: vec![],
            metadata: Some(TableMetadata {
                schema: Some(analyze_input_schema()),
                file_count: 0,
            }),
        };

        let table2 = ComputeTable {
            config: Some(TableConfig::new_with_table_source(
                "Table2",
                &Uuid::parse_str("9c48f082-66fd-4c23-bdb4-99d9a993bd18").unwrap(),
                "time",
                Some("subsort"),
                "entity",
                "grouping2",
            )),
            file_sets: vec![],
            metadata: Some(TableMetadata {
                schema: Some(analyze_input_schema()),
                file_count: 0,
            }),
        };

        let feature_set = FeatureSet {
            formulas: vec![],
            query: "{x: first(Table1.str), y: last(Table1.str) }".to_owned(),
        };

        let options = CompilerOptions {
            per_entity_behavior: PerEntityBehavior::Final,
            ..Default::default()
        };

        let result1 =
            get_plan_hash_with_options(vec![table2, table1.clone()], &feature_set, &options);
        let result2 = get_plan_hash_with_options(vec![table1], &feature_set, &options);
        assert_eq!(result1, result2);
    }
}
