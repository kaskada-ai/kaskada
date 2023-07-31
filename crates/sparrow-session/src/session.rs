use std::borrow::Cow;

use arrow_schema::SchemaRef;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::{ComputeTable, FeatureSet, TableConfig, TableMetadata};
use sparrow_compiler::{AstDfgRef, DataContext, Dfg, DiagnosticCollector};
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
            Literal::StringLiteral(s) => LiteralValue::String(s),
            Literal::Int64Literal(n) => LiteralValue::Number(n.to_string()),
            Literal::UInt64Literal(n) => LiteralValue::Number(n.to_string()),
            Literal::Float64Literal(n) => LiteralValue::Number(n.to_string()),
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
            "make_record" => {
                todo!()
            }
            "remove_fields" => {
                todo!()
            }
            "select_fields" => {
                todo!()
            }
            "extend_record" => {
                todo!()
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
                .map(|diagnostic| diagnostic.message)
                .collect();
            Err(Error::Errors(errors))?
        } else {
            Ok(result)
        }
    }
}

#[static_init::dynamic]
pub(crate) static FIELD_REF_ARGUMENTS: [Located<String>; 1] = [Located::internal_string("record")];

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::*;

    #[test]
    fn session_test() {
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
            .add_literal(Literal::StringLiteral("a".to_owned()))
            .unwrap();
        let field_ref = session
            .add_expr("fieldref", vec![table.expr.clone(), field_name])
            .unwrap();

        assert_eq!(field_ref.data_type(), Some(&DataType::UInt64));
    }
}
