use std::sync::Arc;

use anyhow::Context;
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBufferBuilder, NullArray, PrimitiveArray,
    PrimitiveBuilder, StringArray, StringBuilder, StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::*;
use itertools::{izip, Itertools};
use sparrow_arrow::downcast::{
    downcast_boolean_array, downcast_primitive_array, downcast_string_array, downcast_struct_array,
};

/// Spread and zip two arrays by some boolean mask.
///
/// The result will have the same length as `mask`:
///
/// * Where `mask` is `null`, the result will be `null`.
/// * Where `mask` is `true`, the next value from `truthy` will be taken.
/// * Where `mask` is `false`, the next value from `falsy` will be taken.
///
/// Unlike the built-in `zip(mask, truthy, falsy)` where all three arrays must
/// have the same length, this only uses elements from `truthy` and `falsy` as
/// directed by the `mask`. Specifically:
///
/// * The length of `truthy` should be the same as the number of `true` values.
/// * The length of `falsy` should be the same as the number of `false` values.
pub(super) fn spread_zip(
    mask: &BooleanArray,
    truthy: &dyn Array,
    falsy: &dyn Array,
) -> anyhow::Result<ArrayRef> {
    // NOTE: If the mask is all `true` or all `false`, we could just return a
    // reference to the input if we took an `ArrayRef`. This would require
    // changing the API (and in some cases code changes) to have the `ArrayRef`
    // available.

    anyhow::ensure!(truthy.data_type() == falsy.data_type());

    match truthy.data_type() {
        arrow::datatypes::DataType::Null => Ok(Arc::new(NullArray::new(mask.len()))),
        arrow::datatypes::DataType::Boolean => boolean_spread_zip(mask, truthy, falsy),
        arrow::datatypes::DataType::Int8 => {
            primitive_spread_zip_inefficient::<Int8Type>(mask, truthy, falsy)
        }
        arrow::datatypes::DataType::Int16 => {
            primitive_spread_zip_inefficient::<Int16Type>(mask, truthy, falsy)
        }
        arrow::datatypes::DataType::Int32 => {
            primitive_spread_zip_inefficient::<Int32Type>(mask, truthy, falsy)
        }
        arrow::datatypes::DataType::Int64 => {
            primitive_spread_zip_inefficient::<Int64Type>(mask, truthy, falsy)
        }
        arrow::datatypes::DataType::UInt8 => {
            primitive_spread_zip_inefficient::<UInt8Type>(mask, truthy, falsy)
        }
        arrow::datatypes::DataType::UInt16 => {
            primitive_spread_zip_inefficient::<UInt16Type>(mask, truthy, falsy)
        }
        arrow::datatypes::DataType::UInt32 => {
            primitive_spread_zip_inefficient::<UInt32Type>(mask, truthy, falsy)
        }
        arrow::datatypes::DataType::UInt64 => {
            primitive_spread_zip_inefficient::<UInt64Type>(mask, truthy, falsy)
        }
        arrow::datatypes::DataType::Float16 => {
            primitive_spread_zip_inefficient::<Float16Type>(mask, truthy, falsy)
        }
        arrow::datatypes::DataType::Float32 => {
            primitive_spread_zip_inefficient::<Float32Type>(mask, truthy, falsy)
        }
        arrow::datatypes::DataType::Float64 => {
            primitive_spread_zip_inefficient::<Float64Type>(mask, truthy, falsy)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            primitive_spread_zip_inefficient::<TimestampMicrosecondType>(mask, truthy, falsy)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            primitive_spread_zip_inefficient::<TimestampMillisecondType>(mask, truthy, falsy)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            primitive_spread_zip_inefficient::<TimestampNanosecondType>(mask, truthy, falsy)
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            primitive_spread_zip_inefficient::<TimestampSecondType>(mask, truthy, falsy)
        }
        DataType::Date32 => primitive_spread_zip_inefficient::<Date32Type>(mask, truthy, falsy),
        DataType::Date64 => primitive_spread_zip_inefficient::<Date64Type>(mask, truthy, falsy),
        DataType::Time32(TimeUnit::Second) => {
            primitive_spread_zip_inefficient::<Time32SecondType>(mask, truthy, falsy)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            primitive_spread_zip_inefficient::<Time32MillisecondType>(mask, truthy, falsy)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            primitive_spread_zip_inefficient::<Time64MicrosecondType>(mask, truthy, falsy)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            primitive_spread_zip_inefficient::<Time64NanosecondType>(mask, truthy, falsy)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            primitive_spread_zip_inefficient::<DurationMicrosecondType>(mask, truthy, falsy)
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            primitive_spread_zip_inefficient::<DurationMillisecondType>(mask, truthy, falsy)
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            primitive_spread_zip_inefficient::<DurationNanosecondType>(mask, truthy, falsy)
        }
        DataType::Duration(TimeUnit::Second) => {
            primitive_spread_zip_inefficient::<DurationSecondType>(mask, truthy, falsy)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            primitive_spread_zip_inefficient::<IntervalDayTimeType>(mask, truthy, falsy)
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            primitive_spread_zip_inefficient::<IntervalMonthDayNanoType>(mask, truthy, falsy)
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            primitive_spread_zip_inefficient::<IntervalYearMonthType>(mask, truthy, falsy)
        }
        DataType::Utf8 => string_spread_zip(mask, truthy, falsy),
        DataType::Struct(fields) => struct_spread_zip(fields, mask, truthy, falsy),
        unsupported => Err(anyhow::anyhow!(
            "Unsupported type for spread_zip: {:?}",
            unsupported
        )),
    }
}

fn boolean_spread_zip(
    mask: &BooleanArray,
    truthy: &dyn Array,
    falsy: &dyn Array,
) -> anyhow::Result<ArrayRef> {
    // PERFORMANCE: There are probably ways to do this using bit masks / operating
    // on bytes. But they are probably reasonably complicated. If we find
    // that (a) we're frequently merging boolean columns, they may be worth
    // exploring.

    let mut truthy = downcast_boolean_array(truthy)?.iter();
    let mut falsy = downcast_boolean_array(falsy)?.iter();

    let mut builder = BooleanArray::builder(mask.len());
    for mask in mask.iter() {
        match mask {
            None => builder.append_null(),
            Some(true) => {
                // This should exist since `truthy.len() == mask.count_true()`.
                let value = truthy.next().context("next value")?;
                builder.append_option(value);
            }
            Some(false) => {
                // This should exist since `falsy.len() == mask.count_tfalsy()`.
                let value = falsy.next().context("next value")?;
                builder.append_option(value);
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

// TODO: Bug exists here that causes incorrect spread of rows.
// Current implementation uses inefficient version of spread_zip.
#[allow(unused)]
fn primitive_spread_zip<T: ArrowPrimitiveType>(
    mask: &BooleanArray,
    truthy: &dyn Array,
    falsy: &dyn Array,
) -> anyhow::Result<ArrayRef> {
    let truthy: &PrimitiveArray<T> = downcast_primitive_array(truthy)?;
    let falsy: &PrimitiveArray<T> = downcast_primitive_array(falsy)?;

    let mut builder = PrimitiveBuilder::<T>::with_capacity(mask.len());

    // If there are no null values, we can avoid checking for null in the inner
    // loop.
    if mask.null_count() == 0 {
        let mut truthy_index = 0;
        let mut falsy_index = 0;
        let mut last_mask_end_exclusive = 0;
        for (start_inclusive, end_exclusive) in super::spread::bit_run_iterator(mask) {
            // From last_mask_end_exclusive .. start_inclusive is false -- so append a slice
            // from falsy.
            let falsy_len = start_inclusive - last_mask_end_exclusive;
            builder.append_slice(&falsy.values()[falsy_index..falsy_index + falsy_len]);
            falsy_index += falsy_len;

            // From start_inclusive .. end_exclusive is true -- so append a slice from true.
            let truthy_len = end_exclusive - start_inclusive;
            builder.append_slice(&truthy.values()[truthy_index..truthy_index + truthy_len]);
            truthy_index += truthy_len;

            // Update the end of the last `true` range.
            last_mask_end_exclusive = end_exclusive;
        }
        // After the last run of trues, the remaining values are from falsy.
        let falsy_len = mask.len() - last_mask_end_exclusive;
        builder.append_slice(&falsy.values()[falsy_index..falsy_index + falsy_len]);
    } else {
        anyhow::bail!("Null values in mask of spread_zip not yet supported")
    }

    Ok(Arc::new(builder.finish()))
}

fn primitive_spread_zip_inefficient<T: ArrowPrimitiveType>(
    mask: &BooleanArray,
    truthy: &dyn Array,
    falsy: &dyn Array,
) -> anyhow::Result<ArrayRef> {
    let truthy: &PrimitiveArray<T> = downcast_primitive_array(truthy)?;
    let falsy: &PrimitiveArray<T> = downcast_primitive_array(falsy)?;
    let mut truthy = truthy.iter();
    let mut falsy = falsy.iter();

    let mut builder = PrimitiveBuilder::<T>::with_capacity(mask.len());
    for signal in mask.iter() {
        match signal {
            None => builder.append_null(),
            Some(true) => builder.append_option(truthy.next().context("missing truthy value")?),
            Some(false) => builder.append_option(falsy.next().context("missing falsy value")?),
        };
    }

    Ok(Arc::new(builder.finish()))
}

fn string_spread_zip(
    mask: &BooleanArray,
    truthy: &dyn Array,
    falsy: &dyn Array,
) -> anyhow::Result<ArrayRef> {
    let truthy: &StringArray = downcast_string_array(truthy)?;
    let falsy: &StringArray = downcast_string_array(falsy)?;

    let mut truthy = truthy.iter();
    let mut falsy = falsy.iter();

    let mut builder = StringBuilder::with_capacity(mask.len(), 1024);
    for signal in mask.iter() {
        match signal {
            None => builder.append_null(),
            Some(true) => builder.append_option(truthy.next().context("missing truthy value")?),
            Some(false) => builder.append_option(falsy.next().context("missing falsy value")?),
        };
    }

    Ok(Arc::new(builder.finish()))
}

fn struct_spread_zip(
    fields: &Fields,
    mask: &BooleanArray,
    truthy: &dyn Array,
    falsy: &dyn Array,
) -> anyhow::Result<ArrayRef> {
    let truthy = downcast_struct_array(truthy)?;
    let falsy = downcast_struct_array(falsy)?;

    debug_assert_eq!(truthy.num_columns(), falsy.num_columns());
    let spread_arrays: Vec<_> = izip!(truthy.columns().iter(), falsy.columns().iter())
        .map(|(truthy_field, falsy_field)| -> anyhow::Result<_> {
            let spread = spread_zip(mask, truthy_field.as_ref(), falsy_field.as_ref())?;
            Ok(spread)
        })
        .try_collect()?;

    // We already created the spread fields, but we need to create (and set) the
    // null mask for the StructArray. Specifically, it should be null in the
    // following cases:
    //
    // 1. `mask` is `null`.
    // 2. `mask` is `true` and the next value from `truthy` is `null`.
    // 3. `mask` is `false` and the next value from `falsy` is`null`.
    //
    // TODO: There are likely ways to do this in chunks using bit-wise logical
    // operations. That would likely be more efficient.
    let mut null_buffer = BooleanBufferBuilder::new(mask.len());
    let mut truthy_null = truthy.nulls().map(|nulls| nulls.iter());
    let mut falsy_null = falsy.nulls().map(|nulls| nulls.iter());
    for mask in mask.iter() {
        let is_valid = match mask {
            None => false,
            Some(true) => {
                if let Some(truthy) = &mut truthy_null {
                    truthy.next().context("next truthy value")?
                } else {
                    true
                }
            }
            Some(false) => {
                if let Some(falsy) = &mut falsy_null {
                    falsy.next().context("next falsy value")?
                } else {
                    true
                }
            }
        };
        null_buffer.append(is_valid);
    }
    let null_buffer = null_buffer.finish();
    let null_buffer = NullBuffer::new(null_buffer);

    Ok(Arc::new(StructArray::new(
        fields.clone(),
        spread_arrays,
        Some(null_buffer),
    )))
}
