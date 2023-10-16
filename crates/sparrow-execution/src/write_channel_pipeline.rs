use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use error_stack::{IntoReport, ResultExt};
use parking_lot::Mutex;
use sparrow_batch::Batch;
use sparrow_scheduler::{Pipeline, PipelineError};

/// Pipeline for writing to a Tokio channel.
///
/// This is currently a place holder for a more complete write solution.
#[derive(Debug)]
pub(super) struct WriteChannelPipeline {
    channel: Mutex<Option<tokio::sync::mpsc::Sender<RecordBatch>>>,
    tasks: sparrow_scheduler::Partitioned<sparrow_scheduler::TaskRef>,
    schema: SchemaRef,
}

impl WriteChannelPipeline {
    pub fn new(channel: tokio::sync::mpsc::Sender<RecordBatch>, schema: SchemaRef) -> Self {
        Self {
            channel: Mutex::new(Some(channel)),
            tasks: sparrow_scheduler::Partitioned::new(),
            schema,
        }
    }
}

impl Pipeline for WriteChannelPipeline {
    fn initialize(&mut self, tasks: sparrow_scheduler::Partitioned<sparrow_scheduler::TaskRef>) {
        assert_eq!(tasks.len(), 1);
        self.tasks = tasks;
    }

    fn add_input(
        &self,
        input_partition: sparrow_scheduler::Partition,
        input: usize,
        batch: Batch,
        _scheduler: &mut dyn sparrow_scheduler::Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        // HACK: This converts `Batch` to `RecordBatch` because the current execution logic
        //  expects `RecordBatch` outputs. This should be changed to standardize on `Batch`
        //  which makes it easier to carry a primitive value out.
        if let Some(batch) = batch.into_record_batch(self.schema.clone()) {
            let channel = self.channel.lock();
            channel
                .as_ref()
                .ok_or(PipelineError::InputClosed {
                    input,
                    input_partition,
                })?
                .blocking_send(batch)
                .into_report()
                .change_context(PipelineError::Execution)?
        }
        Ok(())
    }

    fn close_input(
        &self,
        input_partition: sparrow_scheduler::Partition,
        input: usize,
        _scheduler: &mut dyn sparrow_scheduler::Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        let mut channel = self.channel.lock();
        tracing::info!("Closing input of write_channel");
        assert_eq!(input, 0);
        assert_eq!(input_partition, 0);

        error_stack::ensure!(
            channel.is_some(),
            PipelineError::InputClosed {
                input,
                input_partition,
            },
        );
        *channel = None;
        self.tasks[0].complete();
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
