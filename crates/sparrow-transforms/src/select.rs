use arrow_array::{cast::AsArray, BooleanArray};
use arrow_schema::DataType;
use error_stack::{IntoReport, ResultExt};
use sparrow_batch::Batch;

use sparrow_expr_execution::ExpressionExecutor;
use sparrow_physical::Exprs;

use crate::transform::{Error, Transform};

/// Transform for select.
pub struct Select {
    evaluators: ExpressionExecutor,
}

impl Select {
    pub fn try_new(exprs: &Exprs, output_type: &DataType) -> error_stack::Result<Self, Error> {
        let evaluators = ExpressionExecutor::try_new(exprs.as_vec())
            .change_context_lazy(|| Error::CreateTransform("select"))?;
        error_stack::ensure!(
            output_type == evaluators.output_type(),
            Error::MismatchedResultType {
                transform: "select",
                expected: output_type.clone(),
                actual: evaluators.output_type().clone()
            }
        );
        Ok(Self { evaluators })
    }
}

impl Transform for Select {
    fn apply(&self, batch: Batch) -> error_stack::Result<Batch, Error> {
        assert!(!batch.is_empty());

        let condition = self
            .evaluators
            .execute(&batch)
            .change_context(Error::ExecuteTransform("select"))?;

        // Expressions must evaluate to a single boolean condition.
        let condition = condition.as_boolean();

        let result = filter(batch, condition)?;
        Ok(result)
    }

    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

// TODO: Use "filter-bits" to avoid eagerly creating new columns.
fn filter(batch: Batch, predicate: &BooleanArray) -> error_stack::Result<Batch, Error> {
    let error = || Error::ExecuteTransform("select");
    let up_to_time = batch.up_to_time;
    match batch.data() {
        Some(data) => {
            let filter = arrow_select::filter::FilterBuilder::new(predicate).build();
            let data = filter
                .filter(data)
                .into_report()
                .change_context_lazy(error)?;

            // TODO: This is unnecessary if `time` and `key_hash` were already in the batch.
            // We should figure out how to avoid the redundant work.
            let time = batch.time().expect("time column");
            let subsort = batch.subsort().expect("subsort column");
            let key_hash = batch.key_hash().expect("key column");
            let time = filter
                .filter(time)
                .into_report()
                .change_context_lazy(error)?;
            let subsort = filter
                .filter(subsort)
                .into_report()
                .change_context_lazy(error)?;
            let key_hash = filter
                .filter(key_hash)
                .into_report()
                .change_context_lazy(error)?;

            Ok(Batch::new_with_data(
                data, time, subsort, key_hash, up_to_time,
            ))
        }
        None => {
            assert_eq!(predicate.len(), 0);
            Ok(batch.clone())
        }
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
    fn test_select() {
        let output_type = DataType::Boolean;
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
                name: "gt_primitive".into(),
                literal_args: vec![],
                args: vec![3.into(), 4.into()],
                result_type: output_type.clone(),
            },
        ];
        let select = Select::try_new(&exprs, &output_type).unwrap();
        let input = test_batch();
        let actual = select.apply(input).unwrap();

        // Construct the expected output
        let output_fields = Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]);
        let output_data = Arc::new(StructArray::new(
            output_fields.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2, 3])),
                Arc::new(Int64Array::from(vec![10, 11])),
            ],
            None,
        ));
        let time = Arc::new(TimestampNanosecondArray::from(vec![2, 3]));
        let subsort = Arc::new(UInt64Array::from(vec![2, 3]));
        let key_hash = Arc::new(UInt64Array::from(vec![2, 3]));
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
