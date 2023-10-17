use index_vec::IndexVec;

use crate::{Step, StepId};

/// A plan is a directed, acyclic graph of steps.
///
/// The plan is represented as an array of steps, with each step referencing
/// it's children (inputs) by index. The array is topologically sorted so that
/// every step appears after the inputs to that step.
#[derive(Debug, serde::Serialize, serde::Deserialize, Default)]
pub struct Plan {
    /// The steps in the plan.
    pub steps: IndexVec<StepId, Step>,
    /// The pipelines within the plan.
    pub pipelines: Vec<Pipeline>,
}

impl Plan {
    pub fn yaml(&self) -> impl std::fmt::Display + '_ {
        PlanYaml(self)
    }
}

/// Prints a plan as Yaml.
struct PlanYaml<'a>(&'a Plan);

impl<'a> std::fmt::Display for PlanYaml<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // If these YAML strings are large, we could potentially implement `std::io::Write`
        // to the `std::fmt::Formatter`, but for now we go via a string.
        let yaml = serde_yaml::to_string(self.0).map_err(|e| {
            tracing::error!("Failed to write plan YAML: {e:?}");
            std::fmt::Error
        })?;
        write!(f, "{yaml}")
    }
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
