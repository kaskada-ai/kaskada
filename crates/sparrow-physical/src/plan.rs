use crate::{Step, StepId};

/// A plan is a directed, acyclic graph of steps.
///
/// The plan is represented as an array of steps, with each step referencing
/// it's children (inputs) by index. The array is topologically sorted so that
/// every step appears after the inputs to that step.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Plan {
    /// The steps in the plan.
    pub steps: Vec<Step>,
    /// The pipelines within the plan.
    pub pipelines: Vec<Pipeline>,
}

/// Information about a specific "pipeline" within the plan.
///
/// Pipelines take a single input through a linear sequence of
/// steps. Identifying pipelines within the plan provides units
/// of work that ideally run on the same core.
///
/// See [Morsel-driven Parallelism](https://dl.acm.org/doi/10.1145/2588555.2610507).
#[derive(Debug, serde::Serialize, serde::Deserialize, Default, PartialEq)]
pub struct Pipeline {
    /// The steps that are part of this pipeline.
    pub steps: Vec<StepId>,
}
