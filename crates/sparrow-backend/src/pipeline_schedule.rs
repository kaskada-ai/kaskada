use index_vec::{IndexSlice, IndexVec};
use sparrow_core::debug_println;
use sparrow_physical::{Pipeline, Step, StepId, StepKind};

const DEBUG_SCHEDULING: bool = false;

/// Determine the pipeline each step should be part of.
///
/// This is computed in a bottom-up manner:
///
/// 1. Each leaf (input) is the start of a separate pipeline.
/// 2. Certain step kinds are "pipeline breaking", which means
///    they start a new pipeline. For instance, `with_key`.
/// 3. Any other operation with a single input is part of the
///    the same pipeline as the input.
/// 4. Any other operation is a separate pipeline.
pub fn pipeline_schedule(steps: &IndexSlice<StepId, [Step]>) -> Vec<Pipeline> {
    // Compute how many times each step is referenced.
    // Any step referenced multiple times must end a pipeline.
    let mut references = index_vec::index_vec![0; steps.len()];
    for step in steps {
        for input in &step.inputs {
            references[*input] += 1;
        }
    }

    // Then compute the assignments for each step.
    let mut assignments = IndexVec::with_capacity(steps.len());

    let mut pipelines = Vec::new();
    for (index, step) in steps.iter_enumerated() {
        let assignment = if is_pipeline_breaker(index, step, &references) {
            // A step with no input (such as a scan) starts a new pipeline.
            // A step with multiple inputs (such as a merge) is a separate pipeline.
            let index = pipelines.len();
            pipelines.push(Pipeline::default());
            index
        } else {
            // The step has exactly 1 input and is not a pipeline breaker.
            assignments[step.inputs[0]]
        };

        // Add this step to the assigned pipeline and record the assignment.
        pipelines[assignment].steps.push(index);
        assignments.push(assignment);
    }

    pipelines
}

/// Return true if the step is "pipeline breaking".
fn is_pipeline_breaker(index: StepId, step: &Step, references: &IndexVec<StepId, usize>) -> bool {
    match &step.kind {
        _ if step.inputs.len() != 1 => {
            debug_println!(
                DEBUG_SCHEDULING,
                "Step {index} is new pipeline since it has multiple inputs"
            );
            true
        }
        _ if references[step.inputs[0]] > 1 => {
            debug_println!(
                DEBUG_SCHEDULING,
                "Step {index} is new pipeline since it's only input ({}) is referenced {} times",
                step.inputs[0],
                references[step.inputs[0]]
            );
            true
        }
        StepKind::Scan { .. } | StepKind::Merge | StepKind::Repartition { .. } => {
            debug_println!(
                DEBUG_SCHEDULING,
                "Step {index} is new pipeline based on kind {:?}",
                step.kind
            );
            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Schema;
    use sparrow_physical::{Exprs, Pipeline, Step, StepKind};

    use crate::pipeline_schedule;

    #[test]
    fn test_schedule_pipeline() {
        let schema = Arc::new(Schema::empty());
        let steps = index_vec::index_vec![
            // 0: scan table1
            Step {
                kind: StepKind::Scan {
                    table_name: "table1".to_owned(),
                },
                inputs: vec![],
                schema: schema.clone(),
            },
            // 1: scan table2
            Step {
                kind: StepKind::Scan {
                    table_name: "table2".to_owned(),
                },
                inputs: vec![],
                schema: schema.clone(),
            },
            // 2: merge 0 and 1
            Step {
                kind: StepKind::Merge,
                inputs: vec![0.into(), 1.into()],
                schema: schema.clone(),
            },
            // 3: project 0 -> separate pipeline since 0 has 2 consumers
            Step {
                kind: StepKind::Project {
                    exprs: Exprs::empty(),
                },
                inputs: vec![0.into()],
                schema: schema.clone(),
            },
            // 4: project 2 -> same pipeline since only consumer
            Step {
                kind: StepKind::Project {
                    exprs: Exprs::empty(),
                },
                inputs: vec![2.into()],
                schema: schema.clone(),
            },
            // 5: merge 3 and 4 -> new pipeline since merge
            Step {
                kind: StepKind::Merge,
                inputs: vec![3.into(), 4.into()],
                schema,
            },
        ];

        assert_eq!(
            pipeline_schedule(&steps),
            vec![
                Pipeline {
                    steps: vec![0.into()]
                },
                Pipeline {
                    steps: vec![1.into()]
                },
                Pipeline {
                    steps: vec![2.into(), 4.into()]
                },
                Pipeline {
                    steps: vec![3.into()]
                },
                Pipeline {
                    steps: vec![5.into()]
                }
            ]
        );
    }
}
