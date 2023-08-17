use crate::ValueRef;
use anyhow::anyhow;

use crate::StaticArg;

/// Enum for working with the arguments to an aggregation in plans.
///
/// Specifically, there are 3 arguments -- `input`, `ticks` and `duration`.
pub enum AggregationArgs<T> {
    /// Unwindowed aggregations have null ticks and duration.
    NoWindow { input: T },
    /// Since windowed aggregations have non-null ticks and null duration.
    ///
    /// TODO: Would it make sense to have this be `duration=1` instead?
    Since { input: T, ticks: T },
    /// Sliding windowed aggregations have both non-null ticks and duration.
    Sliding { input: T, ticks: T, duration: T },
}

impl AggregationArgs<ValueRef> {
    /// Returns an `AggregationArgs` indicating what window arguments
    /// should be applied to this aggregation based on the `input`.
    pub fn from_input(input: Vec<StaticArg>) -> anyhow::Result<Self> {
        // With the new operation-based plan, we flatten the arguments in the dfg.
        // [input, tick, duration]
        anyhow::ensure!(
            input.len() == 3,
            "Aggregations should have 3 arguments. Saw {:?}",
            input.len()
        );

        match (input[1].is_literal_null(), input[2].is_literal_null()) {
            (true, true) => Ok(AggregationArgs::NoWindow {
                input: input[0].value_ref.clone(),
            }),
            (false, true) => Ok(AggregationArgs::Since {
                input: input[0].value_ref.clone(),
                ticks: input[1].value_ref.clone(),
            }),
            (false, false) => Ok(AggregationArgs::Sliding {
                input: input[0].value_ref.clone(),
                ticks: input[1].value_ref.clone(),
                duration: input[2].value_ref.clone(),
            }),

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
            AggregationArgs::NoWindow { input } => vec![Some(input), None, None],
            AggregationArgs::Since { input, ticks } => vec![Some(input), Some(ticks), None],
            AggregationArgs::Sliding {
                input,
                ticks,
                duration,
            } => vec![Some(input), Some(ticks), Some(duration)],
        }
    }
}
