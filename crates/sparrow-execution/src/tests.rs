use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Int64Array, StructArray, TimestampNanosecondArray, UInt64Array};
use arrow_schema::{DataType, Field, Fields};
use error_stack::{IntoReport, ResultExt};
use index_vec::index_vec;
use parking_lot::Mutex;
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_arrow::{Batch, RowTime};
use sparrow_scheduler::{Pipeline, PipelineError, PipelineInput, WorkerPool};
use sparrow_transforms::TransformPipeline;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, TimestampNanosecondArray, UInt64Array};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use error_stack::{IntoReport, ResultExt};
use futures::stream::BoxStream;
use futures::TryStreamExt;
use index_vec::index_vec;
use parking_lot::Mutex;
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_batch::Batch;
use sparrow_interfaces::ReadConfig;
use sparrow_interfaces::Source;
use sparrow_scheduler::{Pipeline, PipelineError, PipelineInput, WorkerPool};
use sparrow_transforms::TransformPipeline;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "error creating executor")]
    Creating,
    #[display(fmt = "error executing")]
    Executing,
}

impl error_stack::Context for Error {}

fn in_memory_source(schema: SchemaRef) -> sparrow_sources::InMemory {
    let source = sparrow_sources::InMemory::new(true, schema.clone()).unwrap();
    source
}

#[tokio::test]
#[ignore]
async fn test_query() {
    sparrow_testing::init_test_logging();

    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(10);

    let input_fields = Fields::from(vec![
        Field::new(
            "_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("_subsort", DataType::UInt64, true),
        Field::new("_key_hash", DataType::UInt64, false),
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Int64, true),
    ]);
    let projected_datatype = DataType::Struct(input_fields.clone());
    let schema = Arc::new(Schema::new(input_fields.clone()));
    let read_config = ReadConfig {
        keep_open: true,
        start_time: None,
        end_time: None,
    };

    let input_source = in_memory_source(schema.clone());
    let input_rx = input_source.read(&projected_datatype, read_config);

    let output_fields = Fields::from(vec![
        Field::new("ab", DataType::Int64, true),
        Field::new("abc", DataType::Int64, true),
    ]);

    let input_columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(vec![0, 1, 2, 3])), // time
        Arc::new(UInt64Array::from(vec![0, 1, 2, 3])),              // subsort
        Arc::new(UInt64Array::from(vec![0, 1, 2, 3])),              // key_hash
        Arc::new(Int64Array::from(vec![0, 1, 2, 3])),               // a
        Arc::new(Int64Array::from(vec![4, 7, 10, 11])),             // b
        Arc::new(Int64Array::from(vec![Some(21), None, Some(387), Some(87)])), // c
    ];
    let input_batch = RecordBatch::try_new(schema.clone(), input_columns).unwrap();
    input_source.add_batch(input_batch).await.unwrap();

    execute(
        "hello".to_owned(),
        DataType::Struct(input_fields),
        input_rx,
        DataType::Struct(output_fields),
        output_tx,
    )
    .await
    .unwrap();

    let output = output_rx.recv().await.unwrap();
    let output = output.data().unwrap().as_struct();
    let ab = output.column_by_name("ab").unwrap();
    let abc = output.column_by_name("abc").unwrap();
    assert_eq!(ab.as_primitive(), &Int64Array::from(vec![4, 8, 12, 14]));
    assert_eq!(
        abc.as_primitive(),
        &Int64Array::from(vec![Some(25), None, Some(399), Some(101)])
    );
}

/// Execute a physical plan.
pub async fn execute(
    query_id: String,
    input_type: DataType,
    mut input: BoxStream<'_, error_stack::Result<Batch, sparrow_interfaces::SourceError>>,
    output_type: DataType,
    output: tokio::sync::mpsc::Sender<Batch>,
) -> error_stack::Result<(), Error> {
    let mut worker_pool = WorkerPool::start(query_id).change_context(Error::Creating)?;

    // This sets up some fake stuff:
    // - We don't have sinks yet, so we use tokio channels.
    // - We create a "hypothetical" scan step (0)
    // - We create a hard-coded "project" step (1)
    // - We output the results to the channel.

    let table_id = uuid::uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8");

    let scan = sparrow_physical::Step {
        id: 0.into(),
        kind: sparrow_physical::StepKind::Read {
            source_id: table_id,
        },
        inputs: vec![],
        result_type: input_type,
        exprs: sparrow_physical::Exprs::new(),
    };

    let project = sparrow_physical::Step {
        id: 1.into(),
        kind: sparrow_physical::StepKind::Project,
        inputs: vec![0.into()],
        result_type: output_type.clone(),
        exprs: index_vec![
            sparrow_physical::Expr {
                name: "column".into(),
                literal_args: vec![ScalarValue::Utf8(Some("a".to_owned()))],
                args: vec![],
                result_type: DataType::Int64
            },
            sparrow_physical::Expr {
                name: "column".into(),
                literal_args: vec![ScalarValue::Utf8(Some("b".to_owned()))],
                args: vec![],
                result_type: DataType::Int64
            },
            sparrow_physical::Expr {
                name: "add".into(),
                literal_args: vec![],
                args: vec![0.into(), 1.into()],
                result_type: DataType::Int64
            },
            sparrow_physical::Expr {
                name: "column".into(),
                literal_args: vec![ScalarValue::Utf8(Some("c".to_owned()))],
                args: vec![],
                result_type: DataType::Int64
            },
            sparrow_physical::Expr {
                name: "add".into(),
                literal_args: vec![],
                args: vec![2.into(), 3.into()],
                result_type: DataType::Int64
            },
            sparrow_physical::Expr {
                name: "record".into(),
                literal_args: vec![],
                args: vec![2.into(), 4.into()],
                result_type: output_type
            }
        ],
    };

    let sink_pipeline = worker_pool.add_pipeline(1, WriteChannelPipeline::new(output));
    let transform_pipeline = worker_pool.add_pipeline(
        1,
        TransformPipeline::try_new(
            &scan,
            [project].iter(),
            PipelineInput::new(sink_pipeline, 0),
        )
        .change_context(Error::Creating)?,
    );
    let transform_pipeline_input = PipelineInput::new(transform_pipeline, 0);

    let mut injector = worker_pool.injector().clone();
    while let Some(batch) = input.try_next().await.unwrap() {
        transform_pipeline_input
            .add_input(0.into(), batch, &mut injector)
            .change_context(Error::Executing)?;
    }
    transform_pipeline_input
        .close_input(0.into(), &mut injector)
        .change_context(Error::Executing)?;
    worker_pool.stop().change_context(Error::Executing)?;

    Ok(())
}

#[derive(Debug)]
struct WriteChannelPipeline(Mutex<Option<tokio::sync::mpsc::Sender<Batch>>>);

impl WriteChannelPipeline {
    fn new(channel: tokio::sync::mpsc::Sender<Batch>) -> Self {
        Self(Mutex::new(Some(channel)))
    }
}

impl Pipeline for WriteChannelPipeline {
    fn initialize(&mut self, _tasks: sparrow_scheduler::Partitioned<sparrow_scheduler::TaskRef>) {}

    fn add_input(
        &self,
        input_partition: sparrow_scheduler::Partition,
        input: usize,
        batch: Batch,
        _scheduler: &mut dyn sparrow_scheduler::Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        let channel = self.0.lock();
        channel
            .as_ref()
            .ok_or(PipelineError::InputClosed {
                input,
                input_partition,
            })?
            .blocking_send(batch)
            .into_report()
            .change_context(PipelineError::Execution)
    }

    fn close_input(
        &self,
        input_partition: sparrow_scheduler::Partition,
        input: usize,
        _scheduler: &mut dyn sparrow_scheduler::Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        let mut channel = self.0.lock();
        error_stack::ensure!(
            channel.is_some(),
            PipelineError::InputClosed {
                input,
                input_partition,
            },
        );
        *channel = None;
        Ok(())
    }

    fn do_work(
        &self,
        _partition: sparrow_scheduler::Partition,
        _scheduler: &mut dyn sparrow_scheduler::Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        Ok(())
    }
}
