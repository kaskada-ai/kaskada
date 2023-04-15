use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BooleanArray, PrimitiveArray, UInt32Array},
    datatypes::ArrowPrimitiveType,
};
use itertools::izip;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sparrow_core::{downcast_primitive_array, downcast_struct_array};
use sparrow_kernels::BitBufferIterator;
use sparrow_plan::ValueRef;

// use crate::evaluators::aggregation::function::agg_fn::MaxByFn;
use crate::{
    AggFn, AggregationArgs, ArrowAggFn, Evaluator, EvaluatorFactory, MaxByAccumToken, RuntimeInfo,
    StateToken, StaticInfo,
};

/// Evaluator for arrow aggregations.
///
/// This evaluator is specialized for aggregation functions `AggF`.
///
/// This evaluator supports both non-windowed aggregations and aggregations
/// with a `since` window, as they have similar aggregation implementations.
pub struct MaxByEvaluator<T1, T2>
where
    T1: ArrowPrimitiveType,
    T2: ArrowPrimitiveType,
{
    token: MaxByAccumToken<T1::Native, T2::Native>,
    args: AggregationArgs<ValueRef>,
}

impl<T1, T2> Evaluator for MaxByEvaluator<T1, T2>
where
    T1: ArrowPrimitiveType,
    T1::Native: Serialize + DeserializeOwned,
    T2: ArrowPrimitiveType,
    T2::Native: Serialize + DeserializeOwned,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        match &self.args {
            AggregationArgs::NoWindow { inputs } => {
                let grouping = info.grouping();
                let measure_vals = info.value(&inputs[0])?.array_ref()?;
                let input_vals = info.value(&inputs[1])?.array_ref()?;
                let result = Self::aggregate(
                    &mut self.token,
                    grouping.num_groups(),
                    grouping.group_indices(),
                    &measure_vals,
                    &input_vals,
                );

                result
            }
            AggregationArgs::Since { ticks, inputs } => {
                let grouping = info.grouping();
                let input_vals = info.value(&inputs[0])?.array_ref()?;
                let ticks = info.value(ticks)?.boolean_array()?;
                // let result = Self::aggregate_since(
                //     &mut self.token,
                //     grouping.num_groups(),
                //     grouping.group_indices(),
                //     &input_vals,
                //     ticks.as_ref(),
                // );

                // result
                todo!()
            }
            AggregationArgs::Sliding { .. } => {
                unreachable!("Expected Non-windowed or Since windowed aggregation, saw Sliding.")
            }
        }
    }

    fn state_token(&self) -> Option<&dyn StateToken> {
        Some(&self.token)
    }

    fn state_token_mut(&mut self) -> Option<&mut dyn StateToken> {
        Some(&mut self.token)
    }
}

impl<T1, T2> EvaluatorFactory for MaxByEvaluator<T1, T2>
where
    T1: ArrowPrimitiveType + Send + 'static,
    T1::Native: Serialize + DeserializeOwned + Send + Sync,
    T2: ArrowPrimitiveType + Send + 'static,
    T2::Native: Serialize + DeserializeOwned + Send + Sync,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let args = AggregationArgs::from_input(info.args)?;
        match args {
            AggregationArgs::NoWindow { .. } | AggregationArgs::Since { .. } => {
                let token = MaxByAccumToken::<T1::Native, T2::Native>::default();
                Ok(Box::new(Self { token, args }))
            }
            AggregationArgs::Sliding { .. } => {
                // let token = TwoStacksPrimitiveAccumToken::new();
                // Ok(Box::new(TwoStacksMaxByEvaluator::<AggF> { token, args }))
                todo!()
            }
        }
    }
}

impl<T1, T2> MaxByEvaluator<T1, T2>
where
    T1: ArrowPrimitiveType,
    T2: ArrowPrimitiveType,
{
    fn ensure_entity_capacity(
        token: &mut MaxByAccumToken<T1::Native, T2::Native>,
        entity_id_len: usize,
    ) {
        token.resize(entity_id_len);
    }

    /// Updates the accumulator based on the given flags.
    ///
    /// Accumulator behavior is to update -> emit -> reset, resulting in
    /// exclusive start bounds and inclusive end bounds.
    ///
    /// Implements a single row of the logic so that we can easily reuse it.
    /// We choose to inline this so that it can be specialized in cases where
    /// the valid bits are always true.
    #[inline]
    fn update_accum(
        token: &mut MaxByAccumToken<T1::Native, T2::Native>,
        entity_index: u32,
        measure_is_valid: bool,
        input_is_valid: bool,
        measure_value: T1::Native,
        input_value: T2::Native,
    ) -> Option<T2::Native> {
        match token.get_measured(entity_index) {
            Some(cur_max) => {
                println!("cur_max: {:?}", cur_max);
                // print the measure value
                println!("measure_value: {:?}", measure_value);
                // print the measure is valid
                println!("measure_is_valid: {:?}", measure_is_valid);
                if measure_is_valid && measure_value >= cur_max {
                    token.set_measured(entity_index, Some(measure_value));
                    // TODO: Same deal, a valid null value will never be set here.
                    // but we can't know whether it's because its non-new or actually null.
                    // if it's not-new, then are there cases where we actually don't
                    // want to set it? In the current implementation pattern, yes, because
                    // we have that if statement. But if we get rid of that...does it work?
                    // 1. Get rid of if statement.
                    // 2. Set input value no matter what, don't check validity.
                    if input_is_valid {
                        token.set_output(entity_index, Some(input_value));
                    }
                }
            }
            None => {
                if measure_is_valid {
                    token.set_measured(entity_index, Some(measure_value));
                }

                // TODO: FRAZ - this means we'll be ignoring `null` inputs, even if it's a valid output.
                // But since we have no way of distinguishing between null and non-new in this current pattern
                // we can't do any better than this, unless we go the other way and set the output even if
                // the input was not new (meaning we set it to null with out `if input_is_new input_value` transform)
                if input_is_valid {
                    token.set_output(entity_index, Some(input_value));
                }
            }
        }

        token.get_output(entity_index)
    }

    /// Updates the accumulator based on the given flags.
    ///
    /// Accumulator behavior is to update -> emit -> reset, resulting in
    /// exclusive start bounds and inclusive end bounds.
    ///
    /// Implements a single row of the logic so that we can easily reuse it.
    /// We choose to inline this so that it can be specialized in cases where
    /// the valid bits are always true.
    // #[inline]
    // fn update_since_accum(
    //     token: &mut MaxByAccumToken<AggF::AccT, T>,
    //     entity_index: u32,
    //     input_is_valid: bool,
    //     tick_is_valid: bool,
    //     input: &AggF::InT,
    //     tick: bool,
    // ) -> Option<AggF::OutT> {
    //     let entity_index = entity_index as usize;
    //     let accum = &mut accum[entity_index];

    //     if input_is_valid {
    //         // Update the existing accumulator if it exists, or just add the single input
    //         if let Some(accum) = accum {
    //             AggF::add_one(accum, input);
    //         } else {
    //             *accum = Some(AggF::one(input));
    //         }
    //     }
    //     let value_to_emit = match accum.as_ref() {
    //         Some(accum) => AggF::extract(accum),
    //         None => None,
    //     };

    //     if tick_is_valid && tick {
    //         // If `since` is true, clear the accumulator
    //         *accum = None;
    //     }

    //     value_to_emit
    // }

    /// Update the aggregation state with the given inputs and return the
    /// aggregation.
    ///
    /// The `key_capacity` must be greater than all values in the
    /// `entity_indices`.
    ///
    /// # Result
    /// The result is an array containing the result of the aggregation for each
    /// input row.
    ///
    /// # Assumptions
    /// This assumes that the input data has been sorted by occurrence time.
    /// Specifically, no checking is done to ensure that elements appear in the
    /// appropriate order.
    fn aggregate(
        token: &mut MaxByAccumToken<T1::Native, T2::Native>,
        key_capacity: usize,
        entity_indices: &UInt32Array,
        measure_values: &ArrayRef,
        input_values: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        assert_eq!(entity_indices.len(), measure_values.len());
        assert_eq!(entity_indices.len(), input_values.len());

        // Make sure the accum vec is large enough for the entity accumulators we want
        // to store.
        Self::ensure_entity_capacity(token, key_capacity);

        let measure_values = downcast_primitive_array::<T1>(measure_values.as_ref())?;
        let input_values = downcast_primitive_array::<T2>(input_values.as_ref())?;

        // TODO: Handle the case where the input is empty (null_count == len) and we
        // don't need to compute anything.

        let measure_valid_bits = BitBufferIterator::array_valid_bits(measure_values);
        let input_valid_bits = BitBufferIterator::array_valid_bits(input_values);
        let result: PrimitiveArray<T2> = match (measure_valid_bits, input_valid_bits) {
            (Some(mvb), Some(ivb)) => {
                let iter = izip!(
                    mvb,
                    ivb,
                    entity_indices.values(),
                    measure_values.values(),
                    input_values.values()
                )
                .map(
                    |(
                        measure_is_valid,
                        input_is_valid,
                        entity_index,
                        measure_value,
                        input_value,
                    )| {
                        Self::update_accum(
                            token,
                            *entity_index,
                            measure_is_valid,
                            input_is_valid,
                            *measure_value,
                            *input_value,
                        )
                    },
                );
                // SAFETY: `izip!` and `map` are trusted length iterators.
                unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
            }

            (Some(mvb), None) => {
                let iter = izip!(
                    mvb,
                    entity_indices.values(),
                    measure_values.values(),
                    input_values.values()
                )
                .map(
                    |(measure_is_valid, entity_index, measure_value, input_value)| {
                        Self::update_accum(
                            token,
                            *entity_index,
                            measure_is_valid,
                            true,
                            *measure_value,
                            *input_value,
                        )
                    },
                );
                // SAFETY: `izip!` and `map` are trusted length iterators.
                unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
            }

            (None, Some(ivb)) => {
                let iter = izip!(
                    ivb,
                    entity_indices.values(),
                    measure_values.values(),
                    input_values.values()
                )
                .map(
                    |(input_is_valid, entity_index, measure_value, input_value)| {
                        Self::update_accum(
                            token,
                            *entity_index,
                            true,
                            input_is_valid,
                            *measure_value,
                            *input_value,
                        )
                    },
                );
                // SAFETY: `izip!` and `map` are trusted length iterators.
                unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
            }
            (None, None) => {
                let iter = izip!(
                    entity_indices.values(),
                    measure_values.values(),
                    input_values.values()
                )
                .map(|(entity_index, measure_value, input_value)| {
                    Self::update_accum(
                        token,
                        *entity_index,
                        true,
                        true,
                        *measure_value,
                        *input_value,
                    )
                });
                // SAFETY: `izip!` and `map` are trusted length iterators.
                unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
            }
        };

        Ok(Arc::new(result))
    }

    /// Update the aggregation state with the given inputs and return the
    /// aggregation.
    ///
    /// The `key_capacity` must be greater than all values in the
    /// `entity_indices`.
    ///
    /// # Window Behavior
    /// This aggregation uses the `since` window behavior, which takes a single
    /// predicate. If the predicate evaluates to true, the accumulated value is
    /// reset. Values are not accumulated until the first time the predicate
    /// evaluates to true.
    ///
    /// # Result
    /// The result is an array containing the result of the aggregation for each
    /// input row.
    ///
    /// # Assumptions
    /// This assumes that the input data has been sorted by occurrence time.
    /// Specifically, no checking is done to ensure that elements appear in the
    /// appropriate order.
    fn aggregate_since(
        token: &mut MaxByAccumToken<T1::Native, T2::Native>,
        key_capacity: usize,
        entity_indices: &UInt32Array,
        input: &ArrayRef,
        ticks: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        assert_eq!(entity_indices.len(), input.len());
        todo!()

        // Make sure the internal buffers are large enough for the accumulators we may
        // want to store.
        // Self::ensure_entity_capacity(accum, key_capacity);

        // let input = downcast_primitive_array::<AggF::InArrowT>(input.as_ref())?;

        // // TODO: Handle the case where the input is empty (null_count == len) and we
        // // don't need to compute anything.

        // let result: PrimitiveArray<AggF::OutArrowT> = match (
        //     BitBufferIterator::array_valid_bits(input),
        //     BitBufferIterator::array_valid_bits(ticks),
        // ) {
        //     (None, None) => {
        //         let iter = izip!(
        //             entity_indices.values(),
        //             input.values(),
        //             BitBufferIterator::boolean_array(ticks)
        //         )
        //         .map(|(entity_index, input, tick)| {
        //             Self::update_accum(accum, *entity_index, true, true, input, tick)
        //         });

        //         // SAFETY: `izip!` and `map` are trusted length iterators.
        //         unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
        //     }
        //     (Some(input_valid_bits), None) => {
        //         let iter = izip!(
        //             entity_indices.values(),
        //             input_valid_bits,
        //             input.values(),
        //             BitBufferIterator::boolean_array(ticks)
        //         )
        //         .map(|(entity_index, input_is_valid, input, since_bool)| {
        //             Self::update_accum(
        //                 accum,
        //                 *entity_index,
        //                 input_is_valid,
        //                 true,
        //                 input,
        //                 since_bool,
        //             )
        //         });
        //         // SAFETY: `izip!` and `map` are trusted length iterators.
        //         unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
        //     }
        //     (None, Some(window_valid_bits)) => {
        //         let iter = izip!(
        //             entity_indices.values(),
        //             window_valid_bits,
        //             input.values(),
        //             BitBufferIterator::boolean_array(ticks)
        //         )
        //         .map(|(entity_index, since_is_valid, input, since_bool)| {
        //             Self::update_accum(
        //                 accum,
        //                 *entity_index,
        //                 true,
        //                 since_is_valid,
        //                 input,
        //                 since_bool,
        //             )
        //         });

        //         // SAFETY: `izip!` and `map` are trusted length iterators.
        //         unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
        //     }
        //     (Some(input_valid_bits), Some(window_valid_bits)) => {
        //         let iter = izip!(
        //             entity_indices.values(),
        //             input_valid_bits,
        //             window_valid_bits,
        //             input.values(),
        //             BitBufferIterator::boolean_array(ticks)
        //         )
        //         .map(
        //             |(entity_index, input_is_valid, since_is_valid, input, since_bool)| {
        //                 Self::update_accum(
        //                     accum,
        //                     *entity_index,
        //                     input_is_valid,
        //                     since_is_valid,
        //                     input,
        //                     since_bool,
        //                 )
        //             },
        //         );

        //         // SAFETY: `izip!` and `map` are trusted length iterators.
        //         unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
        //     }
        // };

        // Ok(Arc::new(result))
    }
}

#[cfg(test)]
mod tests {}
