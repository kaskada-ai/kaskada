#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

//! Implementations of the pipelines to be executed.

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{Int64Array, RecordBatch, TimestampNanosecondArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use error_stack::{IntoReport, ResultExt};
    use index_vec::index_vec;
    use parking_lot::Mutex;
    use sparrow_arrow::scalar_value::ScalarValue;
    use sparrow_arrow::{Batch, RowTime};
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

    #[tokio::test]
    async fn test_query() {
        sparrow_testing::init_test_logging();

        let (input_tx, input_rx) = tokio::sync::mpsc::channel(10);
        let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(10);

        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("ab", DataType::Int64, true),
            Field::new("abc", DataType::Int64, true),
        ]));

        let input_batch = RecordBatch::try_new(
            input_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2, 3])),
                Arc::new(Int64Array::from(vec![4, 7, 10, 11])),
                Arc::new(Int64Array::from(vec![Some(21), None, Some(387), Some(87)])),
            ],
        )
        .unwrap();
        let time = Arc::new(TimestampNanosecondArray::from(vec![0, 1, 2, 3]));
        let subsort = Arc::new(UInt64Array::from(vec![0, 1, 2, 3]));
        let key_hash = Arc::new(UInt64Array::from(vec![0, 1, 2, 3]));
        let input_batch = Batch::new_with_data(
            input_batch,
            time,
            subsort,
            key_hash,
            RowTime::from_timestamp_ns(3),
        );
        input_tx.send(input_batch).await.unwrap();
        std::mem::drop(input_tx);

        execute(
            "hello".to_owned(),
            input_schema,
            input_rx,
            output_schema,
            output_tx,
        )
        .await
        .unwrap();

        let output = output_rx.recv().await.unwrap();
        let output = output.into_record_batch().unwrap();
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
        input_schema: SchemaRef,
        mut input: tokio::sync::mpsc::Receiver<Batch>,
        output_schema: SchemaRef,
        output: tokio::sync::mpsc::Sender<Batch>,
    ) -> error_stack::Result<(), Error> {
        let mut worker_pool = WorkerPool::start(query_id).change_context(Error::Creating)?;

        // This sets up some fake stuff:
        // - We don't have sources / sinks yet, so we use tokio channels.
        // - We create a "hypothetical" scan step (0)
        // - We create a hard-coded "project" step (1)
        // - We output the results to the channel.

        let scan = sparrow_physical::Step {
            id: 0.into(),
            kind: sparrow_physical::StepKind::Scan {
                table_name: "table".to_owned(),
            },
            inputs: vec![],
            schema: input_schema,
        };

        let project = sparrow_physical::Step {
            id: 1.into(),
            kind: sparrow_physical::StepKind::Project {
                exprs: sparrow_physical::Exprs {
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
                    ],
                    outputs: vec![2.into(), 4.into()],
                },
            },
            inputs: vec![0.into()],
            schema: output_schema,
        };

        let sink_pipeline = worker_pool.add_pipeline(1, WriteChannelPipeline::new(output));
        let transform_pipeline = worker_pool.add_pipeline(
            1,
            TransformPipeline::try_new(
                &scan,
                vec![project].iter(),
                PipelineInput::new(sink_pipeline, 0),
            )
            .change_context(Error::Creating)?,
        );
        let transform_pipeline_input = PipelineInput::new(transform_pipeline, 0);

        let mut injector = worker_pool.injector().clone();
        while let Some(batch) = input.recv().await {
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
        fn initialize(
            &mut self,
            _tasks: sparrow_scheduler::Partitioned<sparrow_scheduler::TaskRef>,
        ) {
        }

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
}
