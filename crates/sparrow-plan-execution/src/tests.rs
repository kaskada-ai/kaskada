use arrow_array::cast::AsArray;
use arrow_array::{Int64Array, RecordBatch, TimestampNanosecondArray, UInt64Array};
use sparrow_interfaces::source::{Source, SourceExt};
use sparrow_io::in_memory::InMemorySource;
use sparrow_logical::ExprRef;
use sparrow_session::partitioned::Session;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};

fn query(
    session: &Session,
    source: ExprRef,
) -> error_stack::Result<ExprRef, sparrow_session::Error> {
    let a_str = session.add_literal(sparrow_logical::Literal::new_str("a"))?;
    let a = session.add_expr("fieldref", vec![source.clone(), a_str])?;

    let b_str = session.add_literal(sparrow_logical::Literal::new_str("b"))?;
    let b = session.add_expr("fieldref", vec![source.clone(), b_str])?;

    let c_str = session.add_literal(sparrow_logical::Literal::new_str("c"))?;
    let c = session.add_expr("fieldref", vec![source.clone(), c_str])?;

    let a_plus_b = session.add_expr("add", vec![a, b])?;
    let a_plus_b_plus_c = session.add_expr("add", vec![a_plus_b.clone(), c])?;

    let ab_str = session.add_literal(sparrow_logical::Literal::new_str("ab"))?;
    let abc_str = session.add_literal(sparrow_logical::Literal::new_str("abc"))?;
    session.add_expr("record", vec![ab_str, a_plus_b, abc_str, a_plus_b_plus_c])
}

fn add_input_batch(session: &Session, source: &Arc<dyn Source>) {
    let source_prepared_schema = Arc::new(Schema::new(vec![
        Field::new(
            "_time",
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("_subsort", DataType::UInt64, false),
        Field::new("_key_hash", DataType::UInt64, false),
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Int64, true),
    ]));

    let time = Arc::new(TimestampNanosecondArray::from(vec![0, 1, 2, 3]));
    let subsort = Arc::new(UInt64Array::from(vec![0, 1, 2, 3]));
    let key_hash = Arc::new(UInt64Array::from(vec![0, 1, 2, 3]));
    let a = Arc::new(Int64Array::from(vec![0, 1, 2, 3]));
    let b = Arc::new(Int64Array::from(vec![4, 7, 10, 11]));
    let c = Arc::new(Int64Array::from(vec![Some(21), None, Some(387), Some(87)]));
    let batch = RecordBatch::try_new(
        source_prepared_schema.clone(),
        vec![time, subsort, key_hash, a, b, c],
    )
    .unwrap();

    let source: &InMemorySource = source.downcast_source();
    session.block_on(source.add_batch(batch)).unwrap();
}

#[test]
fn test_logical_query_data_before_execute() {
    sparrow_expressions::ensure_registered();

    sparrow_testing::init_test_logging();
    let mut session = Session::default();

    let source_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Int64, true),
    ]));

    let source: Arc<dyn Source> =
        Arc::new(InMemorySource::new(true, source_schema.clone()).unwrap());
    let source_expr = session.add_source(source.clone()).unwrap();

    add_input_batch(&session, &source);

    let query = query(&session, source_expr).unwrap();
    let execution = session
        .execute(&query, sparrow_interfaces::ExecutionOptions::default())
        .unwrap();

    let output = execution.collect_all_blocking().unwrap();
    assert_eq!(output.len(), 1);
    let output = output.into_iter().next().unwrap();
    let ab = output.column_by_name("ab").unwrap();
    let abc = output.column_by_name("abc").unwrap();
    assert_eq!(ab.as_primitive(), &Int64Array::from(vec![4, 8, 12, 14]));
    assert_eq!(
        abc.as_primitive(),
        &Int64Array::from(vec![Some(25), None, Some(399), Some(101)])
    );
}
