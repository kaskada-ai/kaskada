use anyhow::anyhow;
use sparrow_plan::ValueRef;

use crate::StaticArg;

/// Enum for working with the arguments to an aggregation in plans.
///
/// Specifically, there are 3 arguments -- `input`, `ticks` and `duration`.
pub enum AggregationArgs<T> {
    /// Unwindowed aggregations have null ticks and duration.
    NoWindow { inputs: Vec<T> },
    /// Since windowed aggregations have non-null ticks and null duration.
    ///
    /// TODO: Would it make sense to have this be `duration=1` instead?
    Since { inputs: Vec<T>, ticks: T },
    /// Sliding windowed aggregations have both non-null ticks and duration.
    Sliding {
        inputs: Vec<T>,
        ticks: T,
        duration: T,
    },
}

impl AggregationArgs<ValueRef> {
    /// Returns an `AggregationArgs` indicating what window arguments
    /// should be applied to this aggregation based on the `input`.
    pub fn from_input(input: Vec<StaticArg>) -> anyhow::Result<Self> {
        // With the new operation-based plan, we flatten the arguments in the dfg.
        anyhow::ensure!(
            input.len() >= 3,
            "Aggregations should have 3 or more arguments. Saw {:?}",
            input.len()
        );

        // The final argument is the duration, and the final-1 argument is the tick
        // [input1, ..., inputn, ticks, duration]
        let len = input.len();
        match (
            input[len - 2].is_literal_null(),
            input[len - 1].is_literal_null(),
        ) {
            (true, true) => Ok(AggregationArgs::NoWindow {
                inputs: input
                    .into_iter()
                    .take(len - 2)
                    .map(|i| i.value_ref)
                    .collect(),
            }),
            (false, true) => {
                let tick = input[len - 2].value_ref.clone();
                Ok(AggregationArgs::Since {
                    inputs: input
                        .into_iter()
                        .take(len - 2)
                        .map(|i| i.value_ref)
                        .collect(),
                    ticks: tick,
                })
            }
            (false, false) => {
                let tick = input[len - 2].value_ref.clone();
                let duration = input[len - 1].value_ref.clone();
                Ok(AggregationArgs::Sliding {
                    inputs: input
                        .into_iter()
                        .take(len - 2)
                        .map(|i| i.value_ref)
                        .collect(),
                    ticks: tick,
                    duration,
                })
            }

            invalid => Err(anyhow!(
                "Saw invalid combination of windowed aggregation parameters: {:?}",
                invalid
            )),
        }
    }
}

impl<T> AggregationArgs<T> {
    pub fn to_arg_vec(self) -> Vec<Option<T>> {
        match self {
            AggregationArgs::NoWindow { inputs } => inputs
                .into_iter()
                .map(|i| Some(i))
                .chain(vec![None, None])
                .collect(),
            AggregationArgs::Since { inputs, ticks } => inputs
                .into_iter()
                .map(|i| Some(i))
                .chain(vec![Some(ticks), None])
                .collect(),
            AggregationArgs::Sliding {
                inputs,
                ticks,
                duration,
            } => inputs
                .into_iter()
                .map(|i| Some(i))
                .chain(vec![Some(ticks), Some(duration)])
                .collect(),
        }
    }
}
