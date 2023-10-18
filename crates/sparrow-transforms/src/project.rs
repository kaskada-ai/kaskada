use arrow_schema::DataType;
use error_stack::ResultExt;
use sparrow_batch::Batch;

use sparrow_expr_execution::ExpressionExecutor;
use sparrow_physical::Exprs;

use crate::transform::{Error, Transform};

/// Transform for projection.
pub struct Project {
    evaluators: ExpressionExecutor,
}

impl Project {
    pub fn try_new(exprs: &Exprs, output_type: &DataType) -> error_stack::Result<Self, Error> {
        let evaluators = ExpressionExecutor::try_new(exprs.as_vec())
            .change_context_lazy(|| Error::CreateTransform("project"))?;
        error_stack::ensure!(
            output_type == evaluators.output_type(),
            Error::MismatchedResultType {
                transform: "project",
                expected: output_type.clone(),
                actual: evaluators.output_type().clone()
            }
        );
        Ok(Self { evaluators })
    }
}

impl Transform for Project {
    fn apply(&self, batch: Batch) -> error_stack::Result<Batch, Error> {
        assert!(!batch.is_empty());

        let error = || Error::ExecuteTransform("project");
        let result = self.evaluators.execute(&batch).change_context_lazy(error)?;
        Ok(batch.with_projection(result))
    }

    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow_array::{Int64Array, StructArray, TimestampNanosecondArray, UInt64Array};
    use arrow_schema::{Field, Fields};
    use index_vec::index_vec;
    use sparrow_arrow::scalar_value::ScalarValue;
    use sparrow_batch::RowTime;

    fn test_batch() -> Batch {
        let input_fields = Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]);
        let input_data = Arc::new(StructArray::new(
            input_fields.clone(),
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2, 3])),
                Arc::new(Int64Array::from(vec![4, 7, 10, 11])),
            ],
            None,
        ));
        let time = Arc::new(TimestampNanosecondArray::from(vec![0, 1, 2, 3]));
        let subsort = Arc::new(UInt64Array::from(vec![0, 1, 2, 3]));
        let key_hash = Arc::new(UInt64Array::from(vec![0, 1, 2, 3]));

        Batch::new_with_data(
            input_data,
            time,
            subsort,
            key_hash,
            RowTime::from_timestamp_ns(3),
        )
    }

    #[test]
    fn test_project() {
        let output_fields = Fields::from(vec![
            Field::new("b", DataType::Int64, true),
            Field::new("ab", DataType::Int64, true),
        ]);
        let output_type = DataType::Struct(output_fields.clone());

        let input = test_batch();
        let input_type = input.data().unwrap().data_type();
        let exprs: Exprs = index_vec![
            sparrow_physical::Expr {
                name: "input".into(),
                literal_args: vec![],
                args: vec![],
                result_type: input_type.clone(),
            },
            sparrow_physical::Expr {
                name: "fieldref".into(),
                literal_args: vec![ScalarValue::Utf8(Some("a".to_owned()))],
                args: vec![0.into()],
                result_type: DataType::Int64,
            },
            sparrow_physical::Expr {
                name: "fieldref".into(),
                literal_args: vec![ScalarValue::Utf8(Some("b".to_owned()))],
                args: vec![0.into()],
                result_type: DataType::Int64,
            },
            sparrow_physical::Expr {
                name: "add".into(),
                literal_args: vec![],
                args: vec![1.into(), 2.into()],
                result_type: DataType::Int64,
            },
            sparrow_physical::Expr {
                name: "literal".into(),
                literal_args: vec![ScalarValue::Int64(Some(10))],
                args: vec![],
                result_type: DataType::Int64,
            },
            sparrow_physical::Expr {
                name: "record".into(),
                literal_args: vec![],
                args: vec![2.into(), 3.into()],
                result_type: output_type.clone(),
            },
        ];
        let project = Project::try_new(&exprs, &output_type).unwrap();
        let input = test_batch();
        let actual = project.apply(input).unwrap();

        // Construct the expected output
        let output_data = Arc::new(StructArray::new(
            output_fields.clone(),
            vec![
                Arc::new(Int64Array::from(vec![4, 7, 10, 11])),
                Arc::new(Int64Array::from(vec![4, 8, 12, 14])),
            ],
            None,
        ));
        let time = Arc::new(TimestampNanosecondArray::from(vec![0, 1, 2, 3]));
        let subsort = Arc::new(UInt64Array::from(vec![0, 1, 2, 3]));
        let key_hash = Arc::new(UInt64Array::from(vec![0, 1, 2, 3]));
        let expected = Batch::new_with_data(
            output_data,
            time,
            subsort,
            key_hash,
            RowTime::from_timestamp_ns(3),
        );

        assert_eq!(expected, actual);
    }
}
