use std::sync::Arc;

use sparrow_arrow::Batch;

use crate::{Partition, Pipeline, PipelineError, Scheduler};

/// A struct used for sending batches to a specific input port of a down-stream pipeline.
#[derive(Debug)]
pub struct PipelineInput {
    pipeline: Arc<dyn Pipeline>,
    input: usize,
}

impl PipelineInput {
    pub fn new(pipeline: Arc<dyn Pipeline>, input: usize) -> Self {
        Self { pipeline, input }
    }

    pub fn add_input(
        &self,
        partition: Partition,
        batch: Batch,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        self.pipeline
            .add_input(partition, self.input, batch, scheduler)
    }

    pub fn close_input(
        &self,
        input_partition: Partition,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        self.pipeline
            .close_input(input_partition, self.input, scheduler)
    }
}
