use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use error_stack::ResultExt;
use parking_lot::Mutex;
use sparrow_batch::Batch;
use sparrow_interfaces::destination::{Destination, Writer};
use sparrow_interfaces::types::{Partition, Partitioned};
use sparrow_scheduler::{Pipeline, PipelineError, Scheduler, TaskRef};

/// Pipeline for writing to a destination.
#[derive(Debug)]
pub struct WritePipeline {
    destination: Arc<dyn Destination>,
    #[allow(unused)]
    schema: SchemaRef,
    partitions: Partitioned<WritePartition>,
}

#[derive(Debug)]
struct WritePartition {
    task: TaskRef,
    is_closed: AtomicBool,
    inputs: Mutex<VecDeque<Batch>>,
    writer: WriterState,
}

impl WritePartition {
    /// Close the input. Returns true if the input buffer is empty.
    fn close(&self) -> bool {
        self.is_closed.store(true, Ordering::Release);
        self.inputs.lock().is_empty()
    }

    fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Acquire)
    }

    fn add_input(&self, batch: Batch) {
        self.inputs.lock().push_back(batch);
    }

    fn pop_input(&self) -> Option<Batch> {
        self.inputs.lock().pop_front()
    }
}

#[derive(Debug)]
enum WriterState {
    #[allow(dead_code)]
    NotStarted,
    Writing(Mutex<Box<dyn Writer>>),
    #[allow(dead_code)]
    Complete,
}

impl WriterState {
    fn close(&self) -> error_stack::Result<(), PipelineError> {
        match self {
            WriterState::Writing(writer) => {
                writer
                    .lock()
                    .close()
                    .change_context(PipelineError::Execution)?;
            }
            WriterState::NotStarted => {
                tracing::warn!("called close on uninitialized writer")
            }
            WriterState::Complete => {
                tracing::warn!("called close on already complete writer")
            }
        }
        Ok(())
    }
}

impl WritePipeline {
    pub fn new(destination: Arc<dyn Destination>, schema: SchemaRef) -> Self {
        Self {
            destination,
            schema,
            partitions: Partitioned::new(),
        }
    }
}

impl Pipeline for WritePipeline {
    fn initialize(&mut self, tasks: Partitioned<TaskRef>) {
        self.partitions = tasks
            .into_iter()
            .map(|task| {
                let writer: Box<dyn Writer> = self.destination.new_writer(task.partition).unwrap();
                let writer = WriterState::Writing(Mutex::new(writer));
                let is_closed = AtomicBool::new(false);
                let inputs = Mutex::new(VecDeque::new());
                WritePartition {
                    task,
                    is_closed,
                    inputs,
                    writer,
                }
            })
            .collect();
    }

    fn add_input(
        &self,
        input_partition: Partition,
        input: usize,
        batch: Batch,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        error_stack::ensure!(
            input == 0,
            PipelineError::InvalidInput {
                input,
                input_len: 1
            }
        );
        let partition = &self.partitions[input_partition];
        error_stack::ensure!(
            !partition.is_closed(),
            PipelineError::InputClosed {
                input,
                input_partition
            }
        );
        partition.add_input(batch);
        scheduler.schedule(partition.task.clone());
        Ok(())
    }

    fn close_input(
        &self,
        input_partition: Partition,
        input: usize,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        error_stack::ensure!(
            input == 0,
            PipelineError::InvalidInput {
                input,
                input_len: 1
            }
        );
        let partition = &self.partitions[input_partition];
        tracing::trace!("Closing input for writer {}", partition.task);
        error_stack::ensure!(
            !partition.is_closed(),
            PipelineError::InputClosed {
                input,
                input_partition
            }
        );

        // We may be currently executing a `do_work` loop, in which case we need to allow
        // the writer to write output before it is closed.
        //
        // We do call `close` on the partition to indicate that once the work loop sees
        // that the partition is empty of inputs, it can complete.
        partition.close();
        scheduler.schedule(partition.task.clone());

        Ok(())
    }

    fn do_work(
        &self,
        partition: Partition,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        let partition = &self.partitions[partition];
        let _enter = tracing::trace_span!("WritePipeline::do_work", %partition.task).entered();

        let Some(batch) = partition.pop_input() else {
            error_stack::ensure!(
                partition.is_closed(),
                PipelineError::illegal_state("scheduled without work")
            );

            tracing::info!(
                "Input is closed and empty. Closing writer and finishing partition {:?}.",
                partition
            );
            partition.writer.close()?;
            partition.task.complete();
            return Ok(());
        };

        if !batch.is_empty() {
            match &partition.writer {
                WriterState::Writing(writer) => {
                    writer
                        .lock()
                        .write_batch(batch)
                        .change_context(PipelineError::Execution)?;
                }
                WriterState::NotStarted => {
                    error_stack::bail!(PipelineError::illegal_state(
                        "work scheduled for uninitialized writer"
                    ))
                }
                WriterState::Complete => {
                    error_stack::bail!(PipelineError::illegal_state(
                        "work scheduled for a complete writer"
                    ));
                }
            }
        }

        // Reschedule the transform if there exists more batches to process.
        // See the [ScheduleCount].
        if !partition.inputs.lock().is_empty() {
            scheduler.schedule(partition.task.clone());
        }

        Ok(())
    }
}
