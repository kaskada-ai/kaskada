use std::borrow::Cow;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::{
    ComputeTable, FeatureSet, PerEntityBehavior, TableConfig, TableMetadata,
};
use sparrow_compiler::{AstDfgRef, DataContext, Dfg, DiagnosticCollector};
use sparrow_plan::TableId;
use sparrow_runtime::execute::output::Destination;
use sparrow_runtime::execute::ExecutionOptions;
use sparrow_syntax::{ExprOp, LiteralValue, Located, Location, Resolved};
use uuid::Uuid;

use crate::{Error, Expr, Literal, Table};

#[derive(Default)]
pub struct Session {
    data_context: DataContext,
    dfg: Dfg,
}

/// Adds a table to the session.
impl Session {
    pub fn add_literal(&mut self, literal: Literal) -> error_stack::Result<Expr, Error> {
        let literal_value = match literal {
            Literal::Null => LiteralValue::Null,
            Literal::String(s) => LiteralValue::String(s),
            Literal::Int64(n) => LiteralValue::Number(n.to_string()),
            Literal::UInt64(n) => LiteralValue::Number(n.to_string()),
            Literal::Float64(n) => LiteralValue::Number(n.to_string()),
        };
        self.add_to_dfg(
            ExprOp::Literal(Located::builder(literal_value)),
            Resolved::default(),
        )
        .map(Expr)
    }

    pub fn add_table(
        &mut self,
        name: &str,
        schema: SchemaRef,
        time_column_name: &str,
        subsort_column_name: Option<&str>,
        key_column_name: &str,
        grouping_name: Option<&str>,
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

        Ok(Table::new(expr, table_info))
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
            "cast" => {
                todo!()
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

                // TODO: Make this a proper error (not an assertion).

                function.signature().assert_valid_argument_count(args.len());

                let has_vararg = args.len() > function.signature().arg_names().len();
                let args = Resolved::new(
                    function.signature().arg_names().into(),
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

    pub fn execute(&self, expr: &Expr) -> error_stack::Result<RecordBatch, Error> {
        // TODO: Decorations?
        let primary_group_info = self
            .data_context
            .group_info(
                expr.0
                    .grouping()
                    .expect("query to be grouped (non-literal)"),
            )
            .expect("missing group info");
        let primary_grouping = primary_group_info.name().to_owned();
        let primary_grouping_key_type = primary_group_info.key_type();

        // First, extract the necessary subset of the DFG as an expression.
        // This will allow us to operate without mutating things.
        let expr = self.dfg.extract_simplest(expr.0.value());
        let expr = sparrow_compiler::remove_useless_transforms(expr)
            .into_report()
            .change_context(Error::Compile)?;

        // TODO: Run the egraph simplifier.
        // TODO: Incremental?
        // TODO: Slicing?
        let plan = sparrow_compiler::plan::extract_plan_proto(
            &self.data_context,
            expr,
            // TODO: Configure per-entity behavior.
            PerEntityBehavior::Final,
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

        // Spawn the root task
        rt.block_on(async move {
            let (output_tx, output_rx) = tokio::sync::mpsc::channel(10);

            let destination = Destination::Channel(output_tx);
            let data_context = self.data_context.clone();
            let options = ExecutionOptions::default();

            // Hacky. Use the existing execution logic. This weird things with downloading checkpoints, etc.
            let mut results =
                sparrow_runtime::execute::execute_new(plan, destination, data_context, options)
                    .await
                    .change_context(Error::Execute)?
                    .boxed();

            // Hacky. Try to get the last response so we can see if there are any errors, etc.
            let mut _last = None;
            while let Some(response) = results.try_next().await.change_context(Error::Execute)? {
                _last = Some(response);
            }

            let batches: Vec<_> = tokio_stream::wrappers::ReceiverStream::new(output_rx)
                .collect()
                .await;

            // Hacky: Assume we produce at least one batch.
            // New execution plans contain the schema ref which cleans this up.
            let schema = batches[0].schema();
            let batch = arrow_select::concat::concat_batches(&schema, &batches)
                .into_report()
                .change_context(Error::Execute)?;
            Ok(batch)
        })
    }

    pub(super) fn hacky_table_mut(
        &mut self,
        table_id: TableId,
    ) -> &mut sparrow_compiler::TableInfo {
        self.data_context.table_info_mut(table_id).unwrap()
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
            .add_table("table", schema, "time", None, "key", Some("user"))
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
