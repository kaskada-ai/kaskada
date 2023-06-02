use crate::Step;

/// A plan is a directed, acyclic graph of steps.
///
/// The plan is represented as an array of steps, with each step referencing
/// it's children (inputs) by index. The array is topologically sorted so that
/// every step appears after the inputs to that step.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Plan {
    /// The steps in the plan.
    pub steps: Vec<Step>,
}
