use hashbrown::HashMap;
use std::borrow::Cow;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::execute_request::Limits;
use sparrow_api::kaskada::v1alpha::{
    ComputeTable, FeatureSet, PerEntityBehavior, TableConfig, TableMetadata,
};
use sparrow_compiler::{AstDfgRef, CompilerOptions, DataContext, Dfg, DiagnosticCollector};
use sparrow_instructions::{GroupId, Udf};
use sparrow_runtime::execute::output::Destination;
use sparrow_runtime::key_hash_inverse::ThreadSafeKeyHashInverse;
use sparrow_syntax::{ExprOp, FenlType, LiteralValue, Located, Location, Resolved};
use uuid::Uuid;

use crate::execution::Execution;
use crate::{Error, Expr, Literal, Table};

#[derive(Default)]
pub struct Session {
    data_context: DataContext,
    dfg: Dfg,
    key_hash_inverse: HashMap<GroupId, Arc<ThreadSafeKeyHashInverse>>,
    /// Keeps track of the uuid mapping.
    ///
    /// We currently do not serialize the `dyn Udf` into the plan, and instead
    /// directly use this local mapping to look up the udf from the serialized
    /// uuid. Once we run on multiple machines, we'll have to serialize/pickle the
    /// udf as well.
    udfs: HashMap<Uuid, Arc<dyn Udf>>,
}

#[derive(Default)]
pub enum Results {
    #[default]
    History,
    Snapshot,
}

#[derive(Default)]
pub struct ExecutionOptions {
    /// The maximum number of rows to return.
    pub row_limit: Option<usize>,
    /// The maximum number of rows to return in a single batch.
    pub max_batch_size: Option<usize>,
    /// Whether to run execute as a materialization or not.
    pub materialize: bool,
    /// History or Snapshot results.
    pub results: Results,
    /// The changed since time. This is the minimum timestamp of changes to events.
    /// For historic queries, this limits the output points.
    /// For snapshot queries, this limits the set of entities that are considered changed.
    pub changed_since_time_s: Option<i64>,
    /// The final at time. This is the maximum timestamp output.
    /// For historic queries, this limits the output points.
    /// For snapshot queries, this determines the time at which the snapshot is produced.
    pub final_at_time_s: Option<i64>,
}

/// Adds a table to the session.
impl Session {
    pub fn add_literal(&mut self, literal: Literal) -> error_stack::Result<Expr, Error> {
        let literal_value = match literal {
            Literal::Null => LiteralValue::Null,
            Literal::Bool(true) => LiteralValue::True,
            Literal::Bool(false) => LiteralValue::False,
            Literal::String(s) => LiteralValue::String(s),
            Literal::Int64(n) => LiteralValue::Number(n.to_string()),
            Literal::UInt64(n) => LiteralValue::Number(n.to_string()),
            Literal::Float64(n) => LiteralValue::Number(n.to_string()),
            Literal::Timedelta { seconds, nanos } => LiteralValue::Timedelta { seconds, nanos },
        };
        self.add_to_dfg(
            ExprOp::Literal(Located::builder(literal_value)),
            Resolved::default(),
        )
        .map(Expr)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_table(
        &mut self,
        name: &str,
        schema: SchemaRef,
        time_column_name: &str,
        retained: bool,
        subsort_column_name: Option<&str>,
        key_column_name: &str,
        grouping_name: Option<&str>,
        time_unit: Option<&str>,
    ) -> error_stack::Result<Table, Error> {
        let uuid = Uuid::new_v4();
        let schema_proto = sparrow_api::kaskada::v1alpha::Schema::try_from(schema.as_ref())
            .into_report()
            .change_context_lazy(|| Error::SchemaForTable(name.to_owned()))?;
        let table = ComputeTable {
            config: Some(TableConfig {
                name: name.to_owned(),
                uuid: uuid.to_string(),
                time_column_name: time_column_name.to_owned(),
                subsort_column_name: subsort_column_name.map(|s| s.to_owned()),
                group_column_name: key_column_name.to_owned(),
                grouping: grouping_name.unwrap_or("").to_owned(),
                source: None,
            }),
            metadata: Some(TableMetadata {
                schema: Some(schema_proto),
                file_count: 0,
            }),
            file_sets: vec![],
        };

        let (key_column, key_field) = schema
            .column_with_name(key_column_name)
            .expect("expected key column");

        let table_info = self
            .data_context
            .add_table(table)
            .into_report()
            .change_context_lazy(|| Error::CreateTable {
                name: name.to_owned(),
            })?;

        let dfg_node = table_info
            .dfg_node(&mut self.dfg)
            .into_report()
            .change_context(Error::CreateTable {
                name: name.to_owned(),
            })?;

        let expr = Expr(dfg_node);

        let key_hash_inverse = self
            .key_hash_inverse
            .entry(table_info.group_id())
            .or_insert_with(|| {
                Arc::new(ThreadSafeKeyHashInverse::from_data_type(
                    key_field.data_type(),
                ))
            })
            .clone();

        Table::new(
            table_info,
            key_hash_inverse,
            key_column,
            expr,
            retained,
            time_unit,
        )
    }

    pub fn add_cast(
        &mut self,
        arg: Expr,
        data_type: arrow_schema::DataType,
    ) -> error_stack::Result<Expr, Error> {
        let op = ExprOp::Cast(
            Located::builder(FenlType::Concrete(data_type)),
            Location::builder(),
        );
        let args = Resolved::new(
            Cow::Borrowed(&*CAST_ARGUMENTS),
            smallvec::smallvec![Located::builder(arg.0)],
            false,
        );
        self.add_to_dfg(op, args).map(Expr)
    }

    pub fn add_expr(
        &mut self,
        function: &str,
        args: Vec<Expr>,
    ) -> error_stack::Result<Expr, Error> {
        let (op, args) = match function {
            "fieldref" => {
                assert_eq!(args.len(), 2);
                let (base, name) = args.into_iter().collect_tuple().unwrap();

                let name = self
                    .dfg
                    .string_literal(name.0.value())
                    .expect("literal name");

                let op = ExprOp::FieldRef(Located::builder(name.to_owned()), Location::builder());
                let args = Resolved::new(
                    Cow::Borrowed(&*FIELD_REF_ARGUMENTS),
                    smallvec::smallvec![Located::builder(base.0)],
                    false,
                );
                (op, args)
            }
            "record" => {
                assert!(args.len() % 2 == 0);
                let (names, values): (Vec<_>, _) = args
                    .into_iter()
                    .map(|e| Located::builder(e.0))
                    .tuples()
                    .unzip();

                let names: smallvec::SmallVec<_> = names
                    .into_iter()
                    .map(|name| {
                        name.transform(|name| {
                            self.dfg
                                .string_literal(name.value())
                                .expect("literal name")
                                .to_owned()
                        })
                    })
                    .collect();

                let args = Resolved::new(Cow::Owned(names.to_vec()), values, false);
                let op = ExprOp::Record(names, Location::builder());
                (op, args)
            }
            "remove_fields" => {
                let values = args.into_iter().map(|e| Located::builder(e.0)).collect();
                let op = ExprOp::RemoveFields(Location::builder());
                let args = Resolved::new(
                    Cow::Borrowed(&*SELECT_REMOVE_FIELDS_ARGUMENTS),
                    values,
                    true,
                );
                (op, args)
            }
            "select_fields" => {
                let values = args.into_iter().map(|e| Located::builder(e.0)).collect();
                let op = ExprOp::SelectFields(Location::builder());
                let args = Resolved::new(
                    Cow::Borrowed(&*SELECT_REMOVE_FIELDS_ARGUMENTS),
                    values,
                    true,
                );
                (op, args)
            }
            "extend_record" => {
                let values = args.into_iter().map(|e| Located::builder(e.0)).collect();
                let op = ExprOp::ExtendRecord(Location::builder());
                let args =
                    Resolved::new(Cow::Borrowed(&*RECORD_EXTENSION_ARGUMENTS), values, false);
                (op, args)
            }
            function => {
                let op = ExprOp::Call(Located::builder(function.to_owned()));

                let function = match sparrow_compiler::get_function(function) {
                    Ok(function) => function,
                    Err(matches) => {
                        error_stack::bail!(Error::NoSuchFunction {
                            name: function.to_owned(),
                            nearest: matches.map(|s| s.to_owned())
                        });
                    }
                };

                if function.is_tick() {
                    assert_eq!(args.len(), 1);
                    let input = &args[0].0;
                    let tick_behavior = function.tick_behavior().expect("tick behavior");
                    let tick = sparrow_compiler::add_tick(&mut self.dfg, tick_behavior, input)
                        .into_report()
                        .change_context(Error::Compile)?;
                    return Ok(Expr(tick));
                }

                // TODO: Make this a proper error (not an assertion).
                let signature = function.internal_signature();
                signature.assert_valid_argument_count(args.len());

                let has_vararg =
                    signature.parameters().has_vararg && args.len() > signature.arg_names().len();
                let args = Resolved::new(
                    signature.arg_names().into(),
                    args.into_iter()
                        .map(|arg| Located::builder(arg.0))
                        .collect(),
                    has_vararg,
                );
                (op, args)
            }
        };

        self.add_to_dfg(op, args).map(Expr)
    }

    fn add_to_dfg(
        &mut self,
        op: ExprOp,
        args: Resolved<Located<AstDfgRef>>,
    ) -> error_stack::Result<AstDfgRef, Error> {
        let feature_set = FeatureSet::default();
        let mut diagnostics = DiagnosticCollector::new(&feature_set);
        let result = sparrow_compiler::add_to_dfg(
            &mut self.data_context,
            &mut self.dfg,
            &mut diagnostics,
            &op,
            args,
            None,
        )
        .into_report()
        .change_context(Error::Invalid)?;

        if diagnostics.num_errors() > 0 {
            let errors = diagnostics
                .finish()
                .into_iter()
                .filter(|diagnostic| diagnostic.is_error())
                .map(|diagnostic| diagnostic.formatted)
                .collect();
            Err(Error::Errors(errors))?
        } else {
            Ok(result)
        }
    }

    /// The [Expr] will call this to add a user-defined-function to the DFG directly.
    ///
    /// This bypasses much of the plumbing of the [ExprOp] required due to our construction
    /// of the AST.
    pub fn add_udf_to_dfg(
        &mut self,
        udf: Arc<dyn Udf>,
        args: Vec<Expr>,
    ) -> error_stack::Result<Expr, Error> {
        let signature = udf.signature();
        let arg_names = signature.arg_names().to_owned();
        signature.assert_valid_argument_count(args.len());

        let has_vararg =
            signature.parameters().has_vararg && args.len() > signature.arg_names().len();
        let args = Resolved::new(
            arg_names.into(),
            args.into_iter()
                .map(|arg| Located::builder(arg.0))
                .collect(),
            has_vararg,
        );
        let feature_set = FeatureSet::default();
        let mut diagnostics = DiagnosticCollector::new(&feature_set);

        let location = Located::builder("udf".to_owned());
        let result = sparrow_compiler::add_udf_to_dfg(
            &location,
            udf.clone(),
            &mut self.dfg,
            args,
            &mut self.data_context,
            &mut diagnostics,
        )
        .into_report()
        .change_context(Error::Invalid)?;

        if diagnostics.num_errors() > 0 {
            let errors = diagnostics
                .finish()
                .into_iter()
                .filter(|diagnostic| diagnostic.is_error())
                .map(|diagnostic| diagnostic.formatted)
                .collect();
            Err(Error::Errors(errors))?
        } else {
            self.udfs.insert(*udf.uuid(), udf.clone());
            Ok(Expr(result))
        }
    }

    /// Execute the query.
    ///
    /// It is unnfortunate this requires `&mut self` instead of `&self`. It relates to the
    /// fact that the decorations may require mutating the DFG, which in turn requires
    /// mutability. In practice, the decorations shouldn't mutate the DFG and/or that
    /// shouldn't require mutating the session.
    pub fn execute(
        &mut self,
        query: &Expr,
        options: ExecutionOptions,
    ) -> error_stack::Result<Execution, Error> {
        // TODO: Decorations?
        let group_id = query
            .0
            .grouping()
            .expect("query to be grouped (non-literal)");

        let per_entity_behavior = match options.results {
            Results::History => PerEntityBehavior::All,
            Results::Snapshot if options.final_at_time_s.is_some() => {
                PerEntityBehavior::FinalAtTime
            }
            Results::Snapshot => PerEntityBehavior::Final,
        };

        // Apply decorations as necessary for the per-entity behavior.
        let feature_set = FeatureSet::default();
        let mut diagnostics = DiagnosticCollector::new(&feature_set);
        let expr = sparrow_compiler::decorate(
            &mut self.data_context,
            &mut self.dfg,
            &mut diagnostics,
            true,
            query.0.clone(),
            per_entity_behavior,
        )
        .into_report()
        .change_context(Error::Compile)?;
        error_stack::ensure!(diagnostics.num_errors() == 0, Error::Internal);

        // Extract the necessary subset of the DFG as an expression.
        // This will allow us to operate without mutating things.
        let expr = self.dfg.extract_simplest(expr);
        let expr = expr
            .simplify(&CompilerOptions {
                ..CompilerOptions::default()
            })
            .into_report()
            .change_context(Error::Compile)?;
        let expr = sparrow_compiler::remove_useless_transforms(expr)
            .into_report()
            .change_context(Error::Compile)?;

        let primary_group_info = self
            .data_context
            .group_info(group_id)
            .expect("missing group info");
        let primary_grouping = primary_group_info.name().to_owned();
        let primary_grouping_key_type = primary_group_info.key_type();

        // Hacky. Ideally, we'd determine the schema from the created execution plan.
        // Currently, this isn't easily available. Instead, we create this from the
        // columns we know we're producing.
        let schema = result_schema(query, primary_grouping_key_type)?;

        // TODO: Incremental?
        // TODO: Slicing?
        let plan = sparrow_compiler::plan::extract_plan_proto(
            &self.data_context,
            expr,
            per_entity_behavior,
            primary_grouping,
            primary_grouping_key_type,
        )
        .into_report()
        .change_context(Error::Compile)?;

        // Switch to the Tokio async pool. This seems gross.
        // Create the runtime.
        //
        // TODO: Figure out how to asynchronously pass results back to Python?
        let rt = tokio::runtime::Runtime::new()
            .into_report()
            .change_context(Error::Execute)?;
        let (output_tx, output_rx) = tokio::sync::mpsc::channel(10);

        let destination = Destination::Channel(output_tx);
        let data_context = self.data_context.clone();

        let (stop_signal_tx, stop_signal_rx) = tokio::sync::watch::channel(false);
        let mut options = options.to_sparrow_options();
        options.stop_signal_rx = Some(stop_signal_rx);

        let key_hash_inverse = self
            .key_hash_inverse
            .get(&group_id)
            .cloned()
            .unwrap_or_else(|| {
                Arc::new(ThreadSafeKeyHashInverse::from_data_type(
                    primary_grouping_key_type,
                ))
            });

        // Hacky. Use the existing execution logic. This weird things with downloading checkpoints, etc.
        let progress = rt
            .block_on(sparrow_runtime::execute::execute_new(
                plan,
                destination,
                data_context,
                options,
                Some(key_hash_inverse),
                self.udfs.clone(),
            ))
            .change_context(Error::Execute)?
            .map_err(|e| e.change_context(Error::Execute))
            .boxed();

        Ok(Execution::new(
            rt,
            output_rx,
            progress,
            stop_signal_tx,
            schema,
        ))
    }
}

#[static_init::dynamic]
pub(crate) static FIELD_REF_ARGUMENTS: [Located<String>; 1] = [Located::internal_string("record")];

#[static_init::dynamic]
static SELECT_REMOVE_FIELDS_ARGUMENTS: [Located<String>; 2] = [
    Located::internal_string("record"),
    Located::internal_string("fields"),
];

#[static_init::dynamic]
static RECORD_EXTENSION_ARGUMENTS: [Located<String>; 2] = [
    Located::internal_string("extension"),
    Located::internal_string("base"),
];

#[static_init::dynamic]
static CAST_ARGUMENTS: [Located<String>; 1] = [Located::internal_string("input")];

impl ExecutionOptions {
    fn to_sparrow_options(&self) -> sparrow_runtime::execute::ExecutionOptions {
        let mut options = sparrow_runtime::execute::ExecutionOptions {
            max_batch_size: self.max_batch_size,
            materialize: self.materialize,
            ..Default::default()
        };

        if let Some(row_limit) = self.row_limit {
            options.limits = Some(Limits {
                preview_rows: row_limit as i64,
            });
        }

        if let Some(changed_since) = self.changed_since_time_s {
            options.set_changed_since_s(changed_since);
        }
        if let Some(final_at_time) = self.final_at_time_s {
            options.set_final_at_time_s(final_at_time);
        }

        options
    }
}

fn result_schema(expr: &Expr, key_type: &DataType) -> error_stack::Result<SchemaRef, Error> {
    let DataType::Struct(fields) = expr.0.value_type().arrow_type().ok_or(Error::Internal)? else {
        error_stack::bail!(Error::Internal)
    };

    let fields: Fields = super::table::KEY_FIELDS
        .iter()
        .cloned()
        .chain(std::iter::once(Arc::new(Field::new(
            "_key",
            key_type.clone(),
            true,
        ))))
        .chain(fields.iter().cloned())
        .collect();
    let schema = Schema::new(fields);
    Ok(Arc::new(schema))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::*;

    #[test]
    fn session_compilation_test() {
        let mut session = Session::default();

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("key", DataType::UInt64, false),
            Field::new("a", DataType::UInt64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let table = session
            .add_table(
                "table",
                schema,
                "time",
                true,
                None,
                "key",
                Some("user"),
                None,
            )
            .unwrap();

        let field_name = session
            .add_literal(Literal::String("a".to_owned()))
            .unwrap();
        let field_ref = session
            .add_expr("fieldref", vec![table.expr.clone(), field_name])
            .unwrap();

        assert_eq!(field_ref.data_type(), Some(&DataType::UInt64));
    }
}
