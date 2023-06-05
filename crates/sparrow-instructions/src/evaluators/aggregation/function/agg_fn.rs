use std::fmt::Debug;

use arrow::datatypes::ArrowPrimitiveType;

/// This trait defines the implementation of aggregation functions.
///
/// Note none of the methods actually take a self parameter. The intention is
/// for this trait to be used directly at compile time to get required to
/// perform all basic aggregation operations.
pub trait AggFn {
    type InT: Send + Sync;
    type AccT: Send + Clone + Debug;
    type OutT;

    fn zero() -> Self::AccT;

    fn one(input: &Self::InT) -> Self::AccT {
        let mut acc = Self::zero();
        Self::add_one(&mut acc, input);
        acc
    }

    /// Merge an accumulator into `acc1`.
    fn merge(acc1: &mut Self::AccT, acc2: &Self::AccT);

    fn extract(acc: &Self::AccT) -> Option<Self::OutT>;

    /// Add an input to `acc`.
    fn add_one(acc: &mut Self::AccT, input: &Self::InT);

    fn name() -> &'static str;
}

/// This trait defines types for aggregations using Arrow types.
///
/// Certain aggregations do not require Arrow types, hence this is split from
/// `AggFn`.
pub trait ArrowAggFn: AggFn {
    type InArrowT: ArrowPrimitiveType<Native = <Self as AggFn>::InT>;
    type OutArrowT: ArrowPrimitiveType<Native = <Self as AggFn>::OutT>;
}
