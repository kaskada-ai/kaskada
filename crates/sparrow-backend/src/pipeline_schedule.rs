use index_vec::{IndexSlice, IndexVec};
use sparrow_physical::{Pipeline, Step, StepId};

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
        let assignment = if is_pipeline_breaker(index, step, &references, steps) {
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
///
/// A "pipeline breaker" must start a new pipeline.
fn is_pipeline_breaker(
    index: StepId,
    step: &Step,
    references: &IndexVec<StepId, usize>,
    steps: &IndexSlice<StepId, [Step]>,
) -> bool {
    match &step.kind {
        _ if !step.kind.is_transform() => {
            tracing::trace!("Step {index} is new pipeline since it not a transform");
            true
        }
        _ if step.inputs.len() != 1 => {
            tracing::trace!("Step {index} is new pipeline since it has multiple inputs");
            true
        }
        _ if !steps[step.inputs[0]].kind.is_transform() => {
            tracing::trace!("Step {index} is a new pipeline since it's input is not a transform");
            true
        }
        _ if references[step.inputs[0]] > 1 => {
            tracing::trace!(
                "Step {index} is new pipeline since it's only input ({}) is referenced {} times",
                step.inputs[0],
                references[step.inputs[0]]
            );
            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {

    use arrow_schema::DataType;
    use sparrow_physical::{Exprs, Pipeline, Step, StepKind};

    use crate::pipeline_schedule;

    #[test]
    fn test_schedule_pipeline() {
        let result_type = DataType::Null;
        let table1 = uuid::uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8");
        let table2 = uuid::uuid!("67e55044-10b1-426f-9247-bb680e5fe0c9");
        let steps = index_vec::index_vec![
            // 0: scan table1
            Step {
                id: 0.into(),
                kind: StepKind::Read {
                    source_uuid: table1
                },
                inputs: vec![],
                result_type: result_type.clone(),
                exprs: Exprs::new(),
            },
            // 1: scan table2
            Step {
                id: 1.into(),
                kind: StepKind::Read {
                    source_uuid: table2
                },
                inputs: vec![],
                result_type: result_type.clone(),
                exprs: Exprs::new(),
            },
            // 2: merge 0 and 1
            Step {
                id: 2.into(),
                kind: StepKind::Merge,
                inputs: vec![0.into(), 1.into()],
                result_type: result_type.clone(),
                exprs: Exprs::new(),
            },
            // 3: project 0 -> separate pipeline since 0 has 2 consumers
            Step {
                id: 3.into(),
                kind: StepKind::Project,
                inputs: vec![0.into()],
                result_type: result_type.clone(),
                exprs: Exprs::new(),
            },
            // 4: project 2 -> new pipeline since input (2) is a merge, not a transform
            Step {
                id: 4.into(),
                kind: StepKind::Project,
                inputs: vec![2.into()],
                result_type: result_type.clone(),
                exprs: Exprs::new(),
            },
            // 5: filter 4 -> same pipeline since only consumer
            Step {
                id: 5.into(),
                kind: StepKind::Filter,
                inputs: vec![4.into()],
                result_type: result_type.clone(),
                exprs: Exprs::new(),
            },
            // 6: merge 3 and 5 -> new pipeline since merge
            Step {
                id: 6.into(),
                kind: StepKind::Merge,
                inputs: vec![3.into(), 5.into()],
                result_type,
                exprs: Exprs::new(),
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
                    steps: vec![2.into()]
                },
                Pipeline {
                    steps: vec![3.into()]
                },
                Pipeline {
                    steps: vec![4.into(), 5.into()]
                },
                Pipeline {
                    steps: vec![6.into()]
                }
            ]
        );
    }
}
