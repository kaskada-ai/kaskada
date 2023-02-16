use super::agg_fn::AggFn;

/// Placeholder struct for the implementation of the [[AggFn]] for `count`.
pub struct Count;
impl AggFn for Count {
    type InT = u32;
    type AccT = u32;
    type OutT = u32;

    fn zero() -> Self::AccT {
        0
    }

    fn merge(acc1: &mut Self::AccT, acc2: &Self::AccT) {
        *acc1 += *acc2
    }

    fn extract(acc: &Self::AccT) -> Option<Self::OutT> {
        Some(*acc)
    }

    fn add_one(acc: &mut Self::AccT, input: &Self::InT) {
        *acc += *input
    }

    fn name() -> &'static str {
        "count"
    }
}
