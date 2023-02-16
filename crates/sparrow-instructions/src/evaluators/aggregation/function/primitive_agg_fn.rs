use std::marker::PhantomData;

use arrow::datatypes::{ArrowPrimitiveType, Float64Type};

use super::agg_fn::{AggFn, ArrowAggFn};
use crate::NumericProperties;

/// Placeholder struct for the implementation of the [[AggFn]] for `sum`
/// aggregation.
pub struct Sum<T: ArrowPrimitiveType>
where
    T::Native: Copy + NumericProperties,
{
    // Make the compiler happy by using the type parameter. Conceptually, the aggregation
    // will store values of type T.
    _phantom: PhantomData<T>,
}

impl<T: ArrowPrimitiveType> ArrowAggFn for Sum<T>
where
    T::Native: NumericProperties,
{
    type InArrowT = T;
    type OutArrowT = T;
}

// Note: Notice that in order to implement a "zero" value for numeric
// aggregations (None), we have to make `AccT` an optional. However, this
// sacrifies space - if `T::Native` is i64, it will store 64 bits, with another
// 64 bits for the `Option` to wrap it.
//
// A possible option is to store a `Vec<AccT>`, without the `Option`, alongside
// a `BitVec`, then convert the `AccT` into `Option<AccT>` based on the value of
// the bit.
impl<T: ArrowPrimitiveType> AggFn for Sum<T>
where
    T::Native: NumericProperties,
{
    type InT = T::Native;
    type AccT = Option<T::Native>;
    type OutT = T::Native;

    fn zero() -> Self::AccT {
        None
    }

    fn merge(acc1: &mut Self::AccT, acc2: &Self::AccT) {
        if let Some(a2) = acc2 {
            if let Some(a1) = acc1 {
                *acc1 = Some(NumericProperties::saturating_add(*a1, *a2))
            } else {
                *acc1 = *acc2;
            }
        }
    }

    fn extract(acc: &Self::AccT) -> Option<Self::OutT> {
        *acc
    }

    fn add_one(acc: &mut Self::AccT, input: &Self::InT) {
        *acc = match acc {
            Some(a) => Some(NumericProperties::saturating_add(*a, *input)),
            None => Some(*input),
        }
    }

    fn name() -> &'static str {
        "sum"
    }
}

/// Placeholder struct for the implementation of the [[AggFn]] for
/// `first` aggregation on primitives.
pub struct FirstPrimitive<T: ArrowPrimitiveType>
where
    T::Native: Copy,
{
    // Make the compiler happy by using the type parameter. Conceptually, the aggregation
    // will store values of type T.
    _phantom: PhantomData<T>,
}

impl<T: ArrowPrimitiveType> ArrowAggFn for FirstPrimitive<T> {
    type InArrowT = T;
    type OutArrowT = T;
}

impl<T: ArrowPrimitiveType> AggFn for FirstPrimitive<T> {
    type InT = T::Native;
    type AccT = Option<T::Native>;
    type OutT = T::Native;

    fn zero() -> Self::AccT {
        None
    }

    fn one(input: &Self::InT) -> Self::AccT {
        Some(*input)
    }

    fn merge(acc1: &mut Self::AccT, acc2: &Self::AccT) {
        if acc2.is_some() && acc1.is_none() {
            *acc1 = *acc2;
        }
    }

    fn extract(acc: &Self::AccT) -> Option<Self::OutT> {
        *acc
    }

    fn add_one(acc: &mut Self::AccT, input: &Self::InT) {
        if acc.is_none() {
            *acc = Some(*input)
        }
    }

    fn name() -> &'static str {
        "first"
    }
}

/// Placeholder struct for the implementation of the [[AggFn]] for
/// `last` aggregation on primitives.
pub struct LastPrimitive<T: ArrowPrimitiveType> {
    // Make the compiler happy by using the type parameter. Conceptually, the aggregation
    // will store values of type T.
    _phantom: PhantomData<T>,
}

impl<T: ArrowPrimitiveType> ArrowAggFn for LastPrimitive<T> {
    type InArrowT = T;
    type OutArrowT = T;
}

impl<T: ArrowPrimitiveType> AggFn for LastPrimitive<T> {
    type InT = T::Native;
    type AccT = Option<T::Native>;
    type OutT = T::Native;

    fn zero() -> Self::AccT {
        None
    }

    fn one(input: &Self::InT) -> Self::AccT {
        Some(*input)
    }

    fn merge(acc1: &mut Self::AccT, acc2: &Self::AccT) {
        if acc2.is_some() {
            *acc1 = *acc2
        }
    }

    fn extract(acc: &Self::AccT) -> Option<Self::OutT> {
        *acc
    }

    fn add_one(acc: &mut Self::AccT, input: &Self::InT) {
        *acc = Some(*input)
    }

    fn name() -> &'static str {
        "last"
    }
}

/// Placeholder struct for the implementation of the [[AggFn]] for `max`
/// aggregation.
pub struct Max<T: ArrowPrimitiveType>
where
    T::Native: Copy + NumericProperties,
{
    // Make the compiler happy by using the type parameter. Conceptually, the aggregation
    // will store values of type T.
    _phantom: PhantomData<T>,
}

impl<T: ArrowPrimitiveType> ArrowAggFn for Max<T>
where
    T::Native: NumericProperties,
{
    type InArrowT = T;
    type OutArrowT = T;
}

impl<T: ArrowPrimitiveType> AggFn for Max<T>
where
    T::Native: NumericProperties,
{
    type InT = T::Native;
    type AccT = Option<T::Native>;
    type OutT = T::Native;

    fn zero() -> Self::AccT {
        None
    }

    fn one(input: &Self::InT) -> Self::AccT {
        Some(*input)
    }

    fn merge(acc1: &mut Self::AccT, acc2: &Self::AccT) {
        if let Some(a2) = acc2 {
            if let Some(a1) = acc1 {
                *acc1 = Some(NumericProperties::max(*a1, *a2))
            } else {
                *acc1 = *acc2;
            }
        }
    }

    fn extract(acc: &Self::AccT) -> Option<Self::OutT> {
        *acc
    }

    fn add_one(acc: &mut Self::AccT, input: &Self::InT) {
        *acc = match acc {
            Some(a) => Some(NumericProperties::max(*a, *input)),
            None => Some(*input),
        }
    }

    fn name() -> &'static str {
        "max"
    }
}

/// Placeholder struct for the implementation of the [[AggFn]] for `min`
/// aggregation.
pub struct Min<T: ArrowPrimitiveType>
where
    T::Native: Copy + NumericProperties,
{
    // Make the compiler happy by using the type parameter. Conceptually, the aggregation
    // will store values of type T.
    _phantom: PhantomData<T>,
}

impl<T: ArrowPrimitiveType> ArrowAggFn for Min<T>
where
    T::Native: NumericProperties,
{
    type InArrowT = T;
    type OutArrowT = T;
}

impl<T: ArrowPrimitiveType> AggFn for Min<T>
where
    T::Native: NumericProperties,
{
    type InT = T::Native;
    type AccT = Option<T::Native>;
    type OutT = T::Native;

    fn zero() -> Self::AccT {
        None
    }

    fn one(input: &Self::InT) -> Self::AccT {
        Some(*input)
    }

    fn merge(acc1: &mut Self::AccT, acc2: &Self::AccT) {
        if let Some(a2) = acc2 {
            if let Some(a1) = acc1 {
                *acc1 = Some(NumericProperties::min(*a1, *a2))
            } else {
                *acc1 = *acc2;
            }
        }
    }

    fn extract(acc: &Self::AccT) -> Option<Self::OutT> {
        *acc
    }

    fn add_one(acc: &mut Self::AccT, input: &Self::InT) {
        *acc = match acc {
            Some(a) => Some(NumericProperties::min(*a, *input)),
            None => Some(*input),
        }
    }

    fn name() -> &'static str {
        "min"
    }
}

/// Placeholder struct for the implementation of the [[AggFn]] for `mean`
/// aggregation.
pub struct Mean<T: ArrowPrimitiveType>
where
    T::Native: Copy + NumericProperties,
{
    // Make the compiler happy by using the type parameter. Conceptually, the aggregation
    // will store values of type T.
    _phantom: PhantomData<T>,
}

impl<T: ArrowPrimitiveType> ArrowAggFn for Mean<T>
where
    T::Native: NumericProperties,
{
    type InArrowT = T;
    type OutArrowT = Float64Type;
}

impl<T: ArrowPrimitiveType> AggFn for Mean<T>
where
    T::Native: NumericProperties,
{
    type InT = T::Native;
    type AccT = Option<(f64, usize)>;
    type OutT = f64;

    fn zero() -> Self::AccT {
        None
    }

    fn one(input: &Self::InT) -> Self::AccT {
        Some((input.as_f64(), 1))
    }

    fn merge(acc1: &mut Self::AccT, acc2: &Self::AccT) {
        if let Some(a2) = acc2 {
            if let Some(a1) = acc1 {
                let combined_items = a1.1 + a2.1;
                let combined_mean =
                    ((a1.0 * a1.1 as f64) + (a2.0 * a2.1 as f64)) / combined_items as f64;
                *acc1 = Some((combined_mean, combined_items))
            } else {
                *acc1 = *acc2
            }
        }
    }

    fn add_one(acc: &mut Self::AccT, input: &Self::InT) {
        *acc = match acc {
            Some(a) => {
                let (mean, count) = a;
                *count += 1;
                let delta = input.as_f64() - *mean;
                *mean += delta / (*count as f64);
                Some((*mean, *count))
            }
            None => Self::one(input),
        }
    }

    fn extract(acc: &Self::AccT) -> Option<Self::OutT> {
        acc.map(|a| a.0)
    }

    fn name() -> &'static str {
        "mean"
    }
}

/// Placeholder struct for the implementation of the [[AggFn]] for
/// `variance` aggregation.
///
/// This computes the sample variance `M2 / (count - 1)`.
pub struct SampleVariance<T: ArrowPrimitiveType>
where
    T::Native: Copy + NumericProperties,
{
    // Make the compiler happy by using the type parameter. Conceptually, the aggregation
    // will store values of type T.
    _phantom: PhantomData<T>,
}

impl<T: ArrowPrimitiveType> ArrowAggFn for SampleVariance<T>
where
    T::Native: NumericProperties,
{
    type InArrowT = T;
    type OutArrowT = Float64Type;
}

impl<T: ArrowPrimitiveType> AggFn for SampleVariance<T>
where
    T::Native: NumericProperties,
{
    type InT = T::Native;
    type AccT = Option<(f64, f64, usize)>;
    type OutT = f64;

    fn zero() -> Self::AccT {
        None
    }

    fn one(input: &Self::InT) -> Self::AccT {
        let input_as_f64 = input.as_f64();
        Some((0.0, input_as_f64, 1))
    }

    fn add_one(acc: &mut Self::AccT, input: &Self::InT) {
        *acc = match acc {
            Some(a) => {
                let input_as_f64 = input.as_f64();
                let (m2, mean, count) = a;
                *count += 1;
                let delta = input_as_f64 - *mean;
                let mean = *mean + (delta / (*count as f64));
                let delta2 = input_as_f64 - mean;
                let m2 = *m2 + delta * delta2;
                Some((m2, mean, *count))
            }
            None => Self::one(input),
        }
    }

    fn extract(acc: &Self::AccT) -> Option<Self::OutT> {
        if let Some(a) = acc {
            // Variance does not exist with 1 or less inputs
            if a.2 > 1 {
                return Some(a.0 / (a.2 as f64));
            }
        }

        None
    }

    fn merge(acc1: &mut Self::AccT, acc2: &Self::AccT) {
        if let Some(a2) = acc2 {
            if let Some(a1) = acc1 {
                if a1.2 == 0 {
                    // If acc1 is empty, copy acc2's values over.
                    a1.2 = a2.2;
                    a1.1 = a2.1;
                    a1.0 = a2.0;
                } else if a2.2 > 0 {
                    // If acc2 is non-empty, merge it into acc1.
                    a1.2 += a2.2;

                    // The mean is normally calculated as (totalSum / totalCount). However, it is
                    // possible that (sum = mean * count) can lose precision at large
                    // values, so chan's method for estimating the mean is a viable
                    // option. However, this formula may be unstable when nA ~= nB and
                    // both values are large. https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
                    a1.1 = a1.1 + ((a2.1 - a1.1) * (a2.2 as f64 / (a1.2 + a2.2) as f64));

                    // Parallel Variance, combining m2: https://en.m.wikipedia.org/wiki/Algorithms_for_calculating_variance
                    a1.0 = a1.0
                        + a2.0
                        + ((a2.2 - a1.2).pow(2) as f64
                            * (((a1.2 * a2.2) as f64) / ((a1.2 + a2.2) as f64)));
                }
            }
        }
    }

    fn name() -> &'static str {
        "variance"
    }
}
