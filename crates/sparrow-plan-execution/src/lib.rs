#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

//! Execution of physical plans.

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use hashbrown::HashMap;
use sparrow_interfaces::source::Source;
use sparrow_interfaces::ExecutionOptions;
use sparrow_merge::MergePipeline;
use sparrow_physical::StepId;
use sparrow_transforms::TransformPipeline;
use std::sync::Arc;

use error_stack::ResultExt;
use itertools::Itertools;
use sparrow_scheduler::{InputHandles, WorkerPoolBuilder};
use uuid::Uuid;

mod error;
mod source_tasks;
mod write_channel_pipeline;

#[cfg(test)]
mod tests;

pub use error::*;

use crate::source_tasks::SourceTasks;
use crate::write_channel_pipeline::WriteChannelPipeline;

pub struct PlanExecutor {
    worker_pool: WorkerPoolBuilder,
    source_tasks: SourceTasks,
    execution_options: Arc<ExecutionOptions>,
}

fn result_type_to_output_schema(result_type: &DataType) -> SchemaRef {
    let mut output_fields = vec![
        Arc::new(Field::new(
            "_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )),
        Arc::new(Field::new("_subsort", DataType::UInt64, false)),
        // TODO: Convert key hash to key.
        Arc::new(Field::new("_key_hash", DataType::UInt64, false)),
    ];
    match result_type {
        DataType::Struct(fields) => {
            output_fields.extend(fields.iter().cloned());
        }
        other => output_fields.push(Arc::new(Field::new("output", other.clone(), true))),
    };
    let output_schema = Schema::new(output_fields);
    Arc::new(output_schema)
}

impl PlanExecutor {
    /// Create the plan executor for the given plan.
    ///
    /// This creates the worker threads but does not inject inputs, meaning
    /// the threads won't have anything to do until `run` is called.
    pub fn try_new(
        query_id: String,
        plan: sparrow_physical::Plan,
        sources: &HashMap<Uuid, Arc<dyn Source>>,
        output: tokio::sync::mpsc::Sender<RecordBatch>,
        execution_options: Arc<ExecutionOptions>,
    ) -> error_stack::Result<Self, Error> {
        let mut executor = PlanExecutor {
            worker_pool: WorkerPoolBuilder::new(query_id).change_context(Error::Creating)?,
            source_tasks: SourceTasks::default(),
            execution_options: execution_options.clone(),
        };

        let last_step = plan.steps.last().expect("at least one step");
        let output_schema = result_type_to_output_schema(&last_step.result_type);

        let sink_pipeline = executor
            .worker_pool
            .add_pipeline(1, WriteChannelPipeline::new(output, output_schema));

        // Map from the producing step ID to the consumers.
        let mut step_consumers: HashMap<StepId, InputHandles> = HashMap::new();
        step_consumers
            .entry(plan.steps.last_idx())
            .or_default()
            .add_consumer(sink_pipeline, 0);

        // Iterate in reverse so the receivers (consumers) are created before the senders (producers).
        for pipeline in plan.pipelines.iter().rev() {
            let first_step_id = *pipeline.steps.first().expect("at least one step");
            let last_step_id = *pipeline.steps.last().expect("at least one step");

            // Get the consumer(s) for the last step in the pipeline.
            // While we schedule and execute pipelines, the structure of the individual
            // steps identify which *step* they receive from. So, to determine where
            // this pipeline "pushes output" we need to determine where the last step
            // in this pipeline "pushes output".
            let consumers = step_consumers
                .remove(&last_step_id)
                .expect("at least one consumer for step");

            let first_step_inputs = &plan.steps[first_step_id].inputs;

            // Create a transform pipeline (if possible), othrewise create the
            // appropriate non-transform pipeline.
            let pipeline = if pipeline
                .steps
                .iter()
                .all(|id| plan.steps[*id].kind.is_transform())
            {
                debug_assert_eq!(
                    first_step_inputs.len(),
                    1,
                    "Transform steps should have a single input"
                );

                // If all of the steps are transforms, then we use the transform pipeline.
                executor.add_transform_pipeline(&plan, &pipeline.steps, consumers)?
            } else {
                assert_eq!(
                    pipeline.steps.len(),
                    1,
                    "Non-transform steps should be in separate pipelines"
                );

                let step = &plan.steps[pipeline.steps[0]];

                // `add_non_transform_pipeline` returns `Some(pipeline)` if it adds a
                // pipeline for the step. It returns `None` if the step is a source and
                // thus has no corresponding pipeline.
                if let Some(pipeline) =
                    executor.add_non_transform_pipeline(&plan, step, consumers, sources)?
                {
                    pipeline
                } else {
                    debug_assert_eq!(step.inputs.len(), 0);
                    continue;
                }
            };

            // Add the pipeline as a consumer for it's inputs.
            for (input_index, input_step_id) in first_step_inputs.iter().enumerate() {
                step_consumers
                    .entry(*input_step_id)
                    .or_default()
                    .add_consumer(pipeline.clone(), input_index);
            }
        }

        Ok(executor)
    }

    pub async fn execute(
        self,
        _stop_signal_rx: tokio::sync::watch::Receiver<bool>,
    ) -> error_stack::Result<(), Error> {
        let Self {
            worker_pool,
            source_tasks,
            ..
        } = self;

        let injector = worker_pool.injector().clone();

        let workers = worker_pool.start().change_context(Error::Starting)?;
        source_tasks.run_sources(injector).await?;

        workers.join().await.change_context(Error::Stopping)?;

        Ok(())
    }

    fn add_non_transform_pipeline(
        &mut self,
        plan: &sparrow_physical::Plan,
        step: &sparrow_physical::Step,
        consumers: InputHandles,
        sources: &HashMap<Uuid, Arc<dyn Source>>,
    ) -> error_stack::Result<Option<Arc<dyn sparrow_scheduler::Pipeline>>, Error> {
        match step.kind {
            sparrow_physical::StepKind::Read { source_uuid } => {
                let channel = sources.get(&source_uuid).ok_or(Error::NoSuchSource {
                    source_id: source_uuid,
                })?;
                let stream = channel.read(&step.result_type, self.execution_options.clone());
                self.source_tasks.add_read(source_uuid, stream, consumers);
                Ok(None)
            }
            sparrow_physical::StepKind::Merge => {
                // TODO: n-way input merges
                assert_eq!(step.inputs.len(), 2, "expected 2 inputs for merge");
                let input_l = plan.steps[step.inputs[0]].id;
                let datatype_l = &plan.steps[step.inputs[0]].result_type;
                let input_r = plan.steps[step.inputs[1]].id;
                let datatype_r = &plan.steps[step.inputs[1]].result_type;

                let pipeline = MergePipeline::try_new(
                    input_l,
                    input_r,
                    datatype_l,
                    datatype_r,
                    &step.result_type,
                    consumers,
                )
                .change_context(Error::Creating)?;
                Ok(Some(self.worker_pool.add_pipeline(1, pipeline)))
            }
            other if other.is_transform() => {
                unreachable!("Transforms should use add_transform_pipeline")
            }
            other => {
                todo!("Unsupported step kind: {other}")
            }
        }
    }

    /// Convert a physical plan Pipeline into the executable scheduler Pipeline.
    fn add_transform_pipeline(
        &mut self,
        plan: &sparrow_physical::Plan,
        steps: &[sparrow_physical::StepId],
        consumers: InputHandles,
    ) -> error_stack::Result<Arc<dyn sparrow_scheduler::Pipeline>, Error> {
        tracing::trace!(
            "Creating transform pipeline: {}",
            steps.iter().format_with(",", |step_id, f| {
                let step = &plan.steps[*step_id];
                f(&format_args!("{step}"))
            })
        );

        // Determine the producer of the input to the transform pipeline.
        // This is the input of the first step in the pipeline.
        let first_step_inputs = &plan.steps[*steps.first().expect("at least one step")].inputs;
        debug_assert_eq!(first_step_inputs.len(), 1);
        let producer_id = first_step_inputs[0];

        let steps = steps.iter().map(|id| &plan.steps[*id]);
        let pipeline = TransformPipeline::try_new(producer_id, steps, consumers)
            .change_context(Error::Creating)?;
        Ok(self.worker_pool.add_pipeline(1, pipeline))
    }
}
