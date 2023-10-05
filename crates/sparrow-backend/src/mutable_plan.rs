use std::rc::Rc;

use arrow_schema::DataType;
use bitvec::prelude::BitVec;
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use index_vec::IndexVec;
use sparrow_physical::{StepId, StepKind};

use crate::exprs::ExprVec;
use crate::Error;

/// Physical plan builder.
pub(crate) struct MutablePlan {
    /// The mutable steps within the plan.
    ///
    /// Note that not all steps may end up being necessary in the final plan.
    steps: IndexVec<StepId, Rc<Step>>,

    /// Map from "step key" to step IDs, to avoid creating steps with the
    /// same kind and inputs.
    ///
    /// Note: This doesn't prevent all equivalent steps. For instance,
    /// `(merge a (merge b c))` and `(merge b (merge a c))`. We could
    /// use an `egraph` or more complicated logic to find the minimal
    /// set of steps early. Instead, we plan to do so using a later
    /// pass that attempts to eliminate unnecessary merges and steps.
    step_hashcons: HashMap<Rc<Step>, StepId>,
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub(crate) struct Step {
    /// The step kind.
    pub kind: StepKind,
    /// The IDs of steps used as input to this step builder.
    pub inputs: Vec<StepId>,
    /// The expressions within this step.
    pub exprs: ExprVec,
    pub result_type: DataType,
}

impl MutablePlan {
    /// Create a new empty plan.
    pub fn empty() -> Self {
        Self {
            steps: IndexVec::new(),
            step_hashcons: HashMap::new(),
        }
    }

    pub fn get_or_create_step_id(
        &mut self,
        kind: StepKind,
        inputs: Vec<StepId>,
        exprs: ExprVec,
        result_type: DataType,
    ) -> StepId {
        let step = Rc::new(Step {
            kind,
            inputs,
            exprs,
            result_type,
        });
        match self.step_hashcons.entry(step) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                let id = self.steps.next_idx();
                self.steps.push(entry.key().clone());
                *entry.insert(id)
            }
        }
    }

    pub fn last_step_id(&self) -> StepId {
        self.steps.last_idx()
    }

    /// Create the physical plan for the given output.
    pub fn finish(self, output: StepId) -> error_stack::Result<sparrow_physical::Plan, Error> {
        // First, determine which steps we need.
        // Since we know they were created in topologic order, we can just
        // output them in the original order, but omitting any unnecessary
        // steps.
        let mut needed: BitVec = BitVec::with_capacity(self.steps.len());
        needed.resize(self.steps.len(), false);
        needed.set(output.index(), true);
        for (step_id, step) in self.steps.iter_enumerated().rev() {
            if needed[step_id.index()] {
                for input in step.inputs.iter() {
                    needed.set(input.index(), true);
                }
            }
        }

        let mut step_ids = index_vec::index_vec![None; self.steps.len()];
        let mut steps = IndexVec::with_capacity(needed.count_ones());
        for needed_step in needed.iter_ones() {
            let step = &self.steps[needed_step];

            let id = steps.next_idx();
            let kind = step.kind;
            let inputs = step
                .inputs
                .iter()
                .map(|input| step_ids[*input].expect("needed"))
                .collect();
            let exprs = step.exprs.to_physical_exprs()?;
            let result_type = step.result_type.clone();
            steps.push(sparrow_physical::Step {
                id,
                kind,
                inputs,
                exprs,
                result_type,
            });
            step_ids.insert(needed_step.into(), Some(id));
        }

        let plan = sparrow_physical::Plan {
            steps,
            pipelines: vec![],
        };
        Ok(plan)
    }

    pub(crate) fn step_result_type(&self, step_id: StepId) -> &DataType {
        &self.steps[step_id].result_type
    }
}
