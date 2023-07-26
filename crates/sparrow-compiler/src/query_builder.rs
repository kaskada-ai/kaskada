use std::borrow::Cow;

use crate::dfg::Dfg;
use crate::resolve_arguments::FIELD_REF_ARGUMENTS;
use crate::{AstDfgRef, DataContext, DiagnosticCollector};
use arrow_schema::SchemaRef;
use error_stack::{IntoReportCompat, ResultExt};
use itertools::Itertools;
use smallvec::smallvec;
use sparrow_api::kaskada::v1alpha::{ComputeTable, FeatureSet, TableConfig, TableMetadata};
use sparrow_syntax::{ExprOp, LiteralValue, Located, Location, Resolved};
use uuid::Uuid;

/// Kaskada query builder.
#[derive(Default)]
pub struct QueryBuilder {
    data_context: DataContext,
    dfg: Dfg,
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "failed to create table '{name}'")]
    CreateTable { name: String },
    #[display(fmt = "failed to encode schema for table '{_0}'")]
    SchemaForTable(String),
    #[display(fmt = "invalid expression")]
    Invalid,
    #[display(fmt = "no function named '{name}': nearest matches are {nearest:?}")]
    NoSuchFunction { name: String, nearest: Vec<String> },
    #[display(fmt = "errors during construction")]
    Errors,
}

impl error_stack::Context for Error {}

pub enum Literal {
    StringLiteral(String),
    Int64Literal(i64),
    UInt64Literal(u64),
    Float64Literal(f64),
}

impl QueryBuilder {
    pub fn add_table(
        &mut self,
        name: &str,
        schema: SchemaRef,
        time_column_name: &str,
        subsort_column_name: Option<&str>,
        key_column_name: &str,
        grouping_name: Option<&str>,
    ) -> error_stack::Result<AstDfgRef, Error> {
        let uuid = Uuid::new_v4();
        let schema_proto: sparrow_api::kaskada::v1alpha::Schema =
            error_stack::IntoReport::into_report(schema.as_ref().try_into())
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
        table_info
            .dfg_node(&mut self.dfg)
            .into_report()
            .change_context(Error::CreateTable {
                name: name.to_owned(),
            })
    }

    fn add_to_dfg(
        &mut self,
        op: ExprOp,
        args: Resolved<Located<AstDfgRef>>,
    ) -> error_stack::Result<AstDfgRef, Error> {
        let feature_set = FeatureSet::default();
        let mut diagnostics = DiagnosticCollector::new(&feature_set);
        let result = crate::add_to_dfg(
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
            Err(Error::Errors)?
        } else {
            Ok(result)
        }
    }

    pub fn add_literal(&mut self, literal: Literal) -> error_stack::Result<AstDfgRef, Error> {
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
    }

    pub fn add_expr(
        &mut self,
        function: &str,
        args: &[AstDfgRef],
    ) -> error_stack::Result<AstDfgRef, Error> {
        let (op, args) = match function {
            "fieldref" => {
                assert_eq!(args.len(), 2);
                let (base, name) = args.iter().cloned().collect_tuple().unwrap();

                let name = self.dfg.string_literal(name.value()).expect("literal name");

                let op = ExprOp::FieldRef(Located::builder(name.to_owned()), Location::builder());
                let args = Resolved::new(
                    Cow::Borrowed(&*FIELD_REF_ARGUMENTS),
                    smallvec![Located::builder(base)],
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

                let function = match crate::get_function(function) {
                    Ok(function) => function,
                    Err(matches) => {
                        error_stack::bail!(Error::NoSuchFunction {
                            name: function.to_owned(),
                            nearest: matches
                                .into_iter()
                                .map(|m| m.to_owned())
                                .collect::<Vec<_>>()
                        });
                    }
                };

                // TODO: Make this a proper error (not an assertion).
                function.signature().assert_valid_argument_count(args.len());

                let has_vararg = args.len() > function.signature().arg_names().len();
                let args = Resolved::new(
                    function.signature().arg_names().into(),
                    args.iter().cloned().map(Located::builder).collect(),
                    has_vararg,
                );
                (op, args)
            }
        };

        self.add_to_dfg(op, args)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use sparrow_syntax::FenlType;

    use super::*;

    #[test]
    fn basic_builder_test() {
        let mut query_builder = QueryBuilder::default();

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
        let table = query_builder
            .add_table("table", schema, "time", None, "key", Some("user"))
            .unwrap();

        let field_name = query_builder
            .add_literal(Literal::StringLiteral("a".to_owned()))
            .unwrap();
        let field_ref = query_builder
            .add_expr("fieldref", &[table, field_name])
            .unwrap();

        assert_eq!(
            field_ref.value_type(),
            &FenlType::Concrete(DataType::UInt64)
        );
    }
}
