use std::sync::Arc;

use sparrow_batch::Batch;

use crate::{Partition, Pipeline, PipelineError, Scheduler};

#[derive(Debug, Default)]
pub struct InputHandles(smallvec::SmallVec<[InputHandle; 1]>);

/// A struct used for sending batches to a specific input port of a down-stream pipeline.
#[derive(Debug)]
struct InputHandle {
    pipeline: Arc<dyn Pipeline>,
    input: usize,
}

impl InputHandles {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn add_consumer(&mut self, pipeline: Arc<dyn Pipeline>, input: usize) {
        self.0.push(InputHandle { pipeline, input });
    }

    pub fn add_input(
        &self,
        input_partition: Partition,
        batch: Batch,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        debug_assert!(!self.0.is_empty(), "Inputs should be non-empty when used.");

        // TODO: Currently, there is a chance that one pipeline outputting to multiple
        // pipelines schedules all of the consumers on the same task pool. This should be
        // OK due to task-stealing, but we may be able to do better. If we place the
        // first consumer that needs to be woken on the local queue, we could place the
        // others on the global queue. This would cause the batch to move to another
        // core, but would let both consumers run in parallel.
        for handle in self.0.iter() {
            handle
                .pipeline
                .add_input(input_partition, handle.input, batch.clone(), scheduler)?;
        }
        Ok(())
    }

    pub fn close_input(
        &self,
        input_partition: Partition,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        debug_assert!(!self.0.is_empty(), "Inputs should be non-empty when used.");

        for handle in self.0.iter() {
            handle
                .pipeline
                .close_input(input_partition, handle.input, scheduler)?;
        }
        Ok(())
    }
}
