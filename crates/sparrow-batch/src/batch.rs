use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{TimestampNanosecondType, UInt64Type};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, RecordBatch, StructArray, TimestampNanosecondArray,
    UInt64Array,
};
use arrow_schema::{Fields, Schema};
use error_stack::{IntoReport, ResultExt};
use itertools::Itertools;

use crate::{Error, RowTime};

/// A batch to be processed by the system.
#[derive(Clone, PartialEq, Debug)]
pub struct Batch {
    /// The data associated with the batch.
    pub(crate) data: Option<BatchInfo>,

    /// An indication that the batch stream has completed up to the given time.
    /// Any rows in future batches on this stream must have a time strictly
    /// greater than this.
    pub up_to_time: RowTime,
}

impl Batch {
    pub fn new_empty(up_to_time: RowTime) -> Self {
        Self {
            data: None,
            up_to_time,
        }
    }

    /// Construct a new batch, inferring the upper bound from the data.
    ///
    /// It is expected that that data is sorted.
    pub fn try_new_from_batch(data: RecordBatch) -> error_stack::Result<Self, Error> {
        error_stack::ensure!(
            data.num_rows() > 0,
            Error::internal_msg("Unable to create batch from empty data".to_owned())
        );

        let time_column: &TimestampNanosecondArray = data.column(0).as_primitive();
        let up_to_time = RowTime::from_timestamp_ns(time_column.value(time_column.len() - 1));

        #[cfg(debug_assertions)]
        validate(&data, up_to_time)?;

        let time: &TimestampNanosecondArray = data.column(0).as_primitive();
        let min_present_time = time.value(0);
        let max_present_time = time.value(time.len() - 1);

        let time = data.column(0).clone();
        let subsort = data.column(1).clone();
        let key_hash = data.column(2).clone();

        // TODO: This creates a `Fields` from the schema, dropping the key columns.
        // Under the hood, this creates a new vec of fields. It would be better to
        // do this outside of this try_new, then share that `Fields`.
        let schema = data.schema();
        let fields: Fields = schema.fields()[3..].into();
        let columns: Vec<ArrayRef> = data.columns()[3..].to_vec();

        let data = Arc::new(StructArray::new(fields, columns, None));

        Ok(Self {
            data: Some(BatchInfo {
                data,
                time,
                subsort,
                key_hash,
                min_present_time: min_present_time.into(),
                max_present_time: max_present_time.into(),
            }),
            up_to_time,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_none()
    }

    pub fn num_rows(&self) -> usize {
        match &self.data {
            Some(data) => data.data.len(),
            None => 0,
        }
    }

    pub fn time(&self) -> Option<&TimestampNanosecondArray> {
        self.data.as_ref().map(|data| data.time())
    }

    pub fn subsort(&self) -> Option<&UInt64Array> {
        self.data.as_ref().map(|data| data.subsort())
    }

    pub fn key_hash(&self) -> Option<&UInt64Array> {
        self.data.as_ref().map(|data| data.key_hash())
    }

    /// Return the minimum time present in the batch.
    pub fn min_present_time(&self) -> Option<RowTime> {
        self.data.as_ref().map(|info| info.min_present_time)
    }

    pub fn max_present_time(&self) -> Option<RowTime> {
        self.data.as_ref().map(|info| info.max_present_time)
    }

    pub fn data(&self) -> Option<&ArrayRef> {
        self.data.as_ref().map(|info| &info.data)
    }

    /// Create a new `Batch` containing the given batch data.
    ///
    /// `time`, `subsort` and `key_hash` are references to the key columns.
    /// They may be references to columns within the batch or not.
    ///
    /// `up_to_time` is a [RowTime] such that:
    ///  (a) all rows so far are less than or equal to `up_to_time`
    ///  (b) all rows in this batch or less than or equal to `up_to_time`
    ///  (c) all future rows are greater than `up_to_time`.
    pub fn new_with_data(
        data: ArrayRef,
        time: ArrayRef,
        subsort: ArrayRef,
        key_hash: ArrayRef,
        up_to_time: RowTime,
    ) -> Self {
        debug_assert_eq!(data.len(), time.len());
        debug_assert_eq!(data.len(), subsort.len());
        debug_assert_eq!(data.len(), key_hash.len());
        debug_assert_eq!(time.data_type(), &TimestampNanosecondType::DATA_TYPE);
        debug_assert_eq!(subsort.data_type(), &UInt64Type::DATA_TYPE);
        debug_assert_eq!(key_hash.data_type(), &UInt64Type::DATA_TYPE);
        let data = if data.len() == 0 {
            None
        } else {
            Some(BatchInfo::new(data, time, subsort, key_hash))
        };

        Self { data, up_to_time }
    }

    /// Return a new `Batch` with the same time properties, but new data.
    pub fn with_projection(&self, new_data: ArrayRef) -> Self {
        assert_eq!(new_data.len(), self.num_rows());
        Self {
            data: self.data.as_ref().map(|data| BatchInfo {
                data: new_data,
                time: data.time.clone(),
                subsort: data.subsort.clone(),
                key_hash: data.key_hash.clone(),
                min_present_time: data.min_present_time,
                max_present_time: data.max_present_time,
            }),
            up_to_time: self.up_to_time,
        }
    }

    /// Split off the rows less or equal to the given time.
    ///
    /// Returns the rows less than or equal to the given time and
    /// leaves the remaining rows in `self`.
    pub fn split_up_to(&mut self, time_inclusive: RowTime) -> Option<Batch> {
        let Some(data) = &mut self.data else {
            return None;
        };

        if time_inclusive < data.min_present_time {
            // time_inclusive < min_present <= max_present.
            // none of the rows in the batch should be taken.
            return None;
        }

        if data.max_present_time <= time_inclusive {
            // min_present <= max_present <= time_inclusive
            // all rows should be taken
            return Some(Batch {
                data: self.data.take(),
                // Even though we took all the rows, the batch
                // we return is only as complete as the original
                // batch. There may be other batches after this
                // that have equal rows less than time inclusive.
                up_to_time: self.up_to_time,
            });
        }

        // If we reach this point, then we need to actually split the data.
        debug_assert!(time_inclusive <= self.up_to_time);
        Some(Batch {
            data: Some(data.split_up_to(time_inclusive)),
            // We can be complete up to time_inclusive because it is less
            // than or equal to the time this batch was complete to. We put
            // all of the rows this batch had up to that time in the result,
            // and only left the batches after that time.
            up_to_time: time_inclusive,
        })
    }

    pub fn concat(batches: Vec<Batch>, up_to_time: RowTime) -> error_stack::Result<Batch, Error> {
        // TODO: Add debug assertions for batch ordering?
        if batches.iter().all(|batch| batch.is_empty()) {
            return Ok(Batch::new_empty(up_to_time));
        }

        let min_present_time = batches
            .iter()
            .find_map(|batch| batch.data.as_ref().map(|data| data.min_present_time))
            .expect("at least one non-empty batch");

        let max_present_time = batches
            .iter()
            .rev()
            .find_map(|batch| batch.data.as_ref().map(|data| data.max_present_time))
            .expect("at least one non-empty batch");

        let times: Vec<_> = batches
            .iter()
            .flat_map(|batch| batch.time())
            .map(|batch| -> &dyn arrow_array::Array { batch })
            .collect();
        let time = arrow_select::concat::concat(&times)
            .into_report()
            .change_context(Error::internal())?;

        let subsorts: Vec<_> = batches
            .iter()
            .flat_map(|batch| batch.subsort())
            .map(|batch| -> &dyn arrow_array::Array { batch })
            .collect();
        let subsort = arrow_select::concat::concat(&subsorts)
            .into_report()
            .change_context(Error::internal())?;

        let key_hashes: Vec<_> = batches
            .iter()
            .flat_map(|batch| batch.key_hash())
            .map(|batch| -> &dyn arrow_array::Array { batch })
            .collect();
        let key_hash = arrow_select::concat::concat(&key_hashes)
            .into_report()
            .change_context(Error::internal())?;

        let batches: Vec<_> = batches
            .iter()
            .flat_map(|batch| batch.data())
            .map(|data| data.as_ref())
            .collect();
        let data = arrow_select::concat::concat(&batches)
            .into_report()
            .change_context(Error::internal())?;

        Ok(Self {
            data: Some(BatchInfo {
                data,
                time,
                subsort,
                key_hash,
                min_present_time,
                max_present_time,
            }),
            up_to_time,
        })
    }

    pub fn take(&self, indices: &UInt64Array) -> error_stack::Result<Self, Error> {
        match &self.data {
            Some(info) => {
                let data = arrow_select::take::take(info.data.as_ref(), indices, None)
                    .into_report()
                    .change_context(Error::internal())?;
                let time = arrow_select::take::take(info.time.as_ref(), indices, None)
                    .into_report()
                    .change_context(Error::internal())?;
                let subsort = arrow_select::take::take(info.subsort.as_ref(), indices, None)
                    .into_report()
                    .change_context(Error::internal())?;
                let key_hash = arrow_select::take::take(info.key_hash.as_ref(), indices, None)
                    .into_report()
                    .change_context(Error::internal())?;
                let info = BatchInfo {
                    data,
                    time,
                    subsort,
                    key_hash,
                    // TODO: Should the `*_present_time` be updated to reflect actual contents of batch?
                    min_present_time: info.min_present_time,
                    max_present_time: info.max_present_time,
                };

                Ok(Self {
                    data: Some(info),
                    up_to_time: self.up_to_time,
                })
            }
            None => {
                assert_eq!(indices.len() - indices.null_count(), 0);
                Ok(self.clone())
            }
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        match &self.data {
            Some(info) => {
                let info = BatchInfo {
                    data: info.data.slice(offset, length),
                    time: info.time.slice(offset, length),
                    subsort: info.subsort.slice(offset, length),
                    key_hash: info.key_hash.slice(offset, length),
                    min_present_time: info.min_present_time,
                    max_present_time: info.max_present_time,
                };
                Self {
                    data: Some(info),
                    up_to_time: self.up_to_time,
                }
            }
            None => {
                assert_eq!(offset, 0);
                assert_eq!(length, 0);
                self.clone()
            }
        }
    }

    /// Creates a batch with the given times and key hashes.
    #[cfg(any(test, feature = "testing"))]
    pub fn minimal_from(
        time: impl Into<TimestampNanosecondArray>,
        key_hash: impl Into<arrow_array::UInt64Array>,
        up_to_time: i64,
    ) -> Self {
        let time: TimestampNanosecondArray = time.into();
        let subsort: UInt64Array = (0..(time.len() as u64)).collect_vec().into();
        let key_hash: UInt64Array = key_hash.into();

        let time: ArrayRef = Arc::new(time);
        let subsort: ArrayRef = Arc::new(subsort);
        let key_hash: ArrayRef = Arc::new(key_hash);

        let data = Arc::new(StructArray::new(
            MINIMAL_SCHEMA.fields().clone(),
            vec![time.clone(), key_hash.clone()],
            None,
        ));

        Batch::new_with_data(
            data,
            time,
            subsort,
            key_hash,
            RowTime::from_timestamp_ns(up_to_time),
        )
    }
}

#[cfg(debug_assertions)]
fn validate(data: &RecordBatch, up_to_time: RowTime) -> error_stack::Result<(), Error> {
    validate_batch_schema(data.schema().as_ref())?;

    for key_column in 0..3 {
        error_stack::ensure!(
            data.column(key_column).null_count() == 0,
            Error::internal_msg(format!(
                "Key column '{}' should not contain null",
                data.schema().field(key_column).name()
            ))
        );
    }

    validate_bounds(data.column(0), data.column(1), data.column(2), up_to_time)
}

#[cfg(debug_assertions)]
/// Validate that the result is sorted.
///
/// Note: This only validates the up_to_time bound.
pub(crate) fn validate_bounds(
    time: &ArrayRef,
    subsort: &ArrayRef,
    key_hash: &ArrayRef,
    up_to_time: RowTime,
) -> error_stack::Result<(), Error> {
    use arrow::compute::SortColumn;

    if time.len() == 0 {
        // No more validation necessary for empty batches.
        return Ok(());
    }

    let sort_indices = arrow::compute::lexsort_to_indices(
        &[
            SortColumn {
                values: time.clone(),
                options: None,
            },
            SortColumn {
                values: subsort.clone(),
                options: None,
            },
            SortColumn {
                values: key_hash.clone(),
                options: None,
            },
        ],
        None,
    )
    .into_report()
    .change_context(Error::internal_msg("lexsort_to_indices".to_owned()))?;

    let is_sorted = sort_indices
        .values()
        .iter()
        .enumerate()
        .all(|(i, x)| i == *x as usize);

    error_stack::ensure!(
        is_sorted,
        Error::internal_msg("Expected sorted batch".to_owned())
    );

    let time: &TimestampNanosecondArray = time.as_primitive();
    let last_time = time.values()[time.len() - 1];
    let up_to_time: i64 = up_to_time.into();
    let order = last_time.cmp(&up_to_time);
    error_stack::ensure!(
        order.is_le(),
        Error::internal_msg(format!(
            "Expected last data <= upper bound, but ({}) > ({})",
            last_time, up_to_time
        ))
    );

    Ok(())
}

fn validate_batch_schema(schema: &Schema) -> error_stack::Result<(), Error> {
    use arrow::datatypes::{DataType, TimeUnit};

    // Validate the three key columns are present, non-null and have the right type.
    validate_key_column(
        schema,
        0,
        "_time",
        &DataType::Timestamp(TimeUnit::Nanosecond, None),
    )?;
    validate_key_column(schema, 1, "_subsort", &DataType::UInt64)?;
    validate_key_column(schema, 2, "_key_hash", &DataType::UInt64)?;

    Ok(())
}

fn validate_key_column(
    schema: &Schema,
    index: usize,
    expected_name: &str,
    expected_type: &arrow::datatypes::DataType,
) -> error_stack::Result<(), Error> {
    error_stack::ensure!(
        schema.field(index).name() == expected_name,
        Error::internal_msg(format!(
            "Expected column {} to be '{}' but was '{}'",
            index,
            expected_name,
            schema.field(index).name()
        ))
    );
    error_stack::ensure!(
        schema.field(index).data_type() == expected_type,
        Error::internal_msg(format!(
            "Key column '{}' should be '{:?}' but was '{:?}'",
            expected_name,
            schema.field(index).data_type(),
            expected_type,
        ))
    );

    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct BatchInfo {
    pub(crate) data: ArrayRef,
    pub(crate) time: ArrayRef,
    pub(crate) subsort: ArrayRef,
    pub(crate) key_hash: ArrayRef,
    min_present_time: RowTime,
    max_present_time: RowTime,
}

impl PartialEq for BatchInfo {
    fn eq(&self, other: &Self) -> bool {
        self.data.as_ref() == other.data.as_ref()
            && self.time.as_ref() == other.time.as_ref()
            && self.min_present_time == other.min_present_time
            && self.max_present_time == other.max_present_time
    }
}

impl BatchInfo {
    fn new(data: ArrayRef, time: ArrayRef, subsort: ArrayRef, key_hash: ArrayRef) -> Self {
        debug_assert_eq!(data.len(), time.len());
        debug_assert_eq!(data.len(), subsort.len());
        debug_assert_eq!(data.len(), key_hash.len());
        debug_assert!(data.len() > 0);
        debug_assert_eq!(time.null_count(), 0);

        let time_column: &TimestampNanosecondArray = time.as_primitive();

        // Debug assertion that the array is sorted by time.
        // Once `is_sorted` is stable (https://github.com/rust-lang/rust/issues/53485).
        // debug_assert!(time_column.iter().is_sorted());
        //
        debug_assert!(time_column.iter().tuple_windows().all(|(a, b)| a <= b));

        let min_present_time = RowTime::from_timestamp_ns(time_column.values()[0]);
        let max_present_time =
            RowTime::from_timestamp_ns(time_column.values()[time_column.len() - 1]);

        Self {
            data,
            time,
            subsort,
            key_hash,
            min_present_time,
            max_present_time,
        }
    }

    /// Split off the rows less or equal to the given time.
    ///
    /// Returns the rows less than or equal to the given time and
    /// leaves the remaining rows in `self`.
    fn split_up_to(&mut self, time_inclusive: RowTime) -> Self {
        let time_column: &TimestampNanosecondArray = self.time.as_primitive();

        // 0..slice_start   (len = slice_start)       = what we return
        // slice_start..len (len = len - slice_start) = what we leave in self
        let slice_start = time_column
            .values()
            .partition_point(|time| RowTime::from_timestamp_ns(*time) <= time_inclusive);
        let slice_len = self.data.len() - slice_start;

        let max_result_time = RowTime::from_timestamp_ns(time_column.values()[slice_start - 1]);
        let min_self_time = RowTime::from_timestamp_ns(time_column.values()[slice_start]);

        let result = Self {
            data: self.data.slice(0, slice_start),
            time: self.time.slice(0, slice_start),
            subsort: self.subsort.slice(0, slice_start),
            key_hash: self.key_hash.slice(0, slice_start),
            min_present_time: self.min_present_time,
            max_present_time: max_result_time,
        };

        self.data = self.data.slice(slice_start, slice_len);
        self.time = self.time.slice(slice_start, slice_len);
        self.key_hash = self.key_hash.slice(slice_start, slice_len);
        self.min_present_time = min_self_time;

        result
    }

    pub(crate) fn time(&self) -> &TimestampNanosecondArray {
        self.time.as_primitive()
    }

    pub(crate) fn subsort(&self) -> &UInt64Array {
        self.subsort.as_primitive()
    }

    pub(crate) fn key_hash(&self) -> &UInt64Array {
        self.key_hash.as_primitive()
    }
}

#[cfg(any(test, feature = "testing"))]
#[static_init::dynamic]
static MINIMAL_SCHEMA: arrow_schema::SchemaRef = {
    use std::sync::Arc;

    use arrow_array::types::{TimestampNanosecondType, UInt64Type};
    use arrow_array::ArrowPrimitiveType;
    use arrow_schema::{Field, Schema};

    let schema = Schema::new(vec![
        Field::new("time", TimestampNanosecondType::DATA_TYPE, false),
        Field::new("key_hash", UInt64Type::DATA_TYPE, false),
    ]);
    Arc::new(schema)
};

#[cfg(test)]
mod tests {

    use crate::testing::arb_arrays::arb_batch;
    use crate::{Batch, RowTime};
    use itertools::Itertools;
    use proptest::prelude::*;

    #[test]
    fn test_split_batch() {
        let mut batch = Batch::minimal_from(vec![0, 1], vec![0, 0], 1);

        let result = batch.split_up_to(RowTime::from_timestamp_ns(0)).unwrap();

        assert_eq!(result, Batch::minimal_from(vec![0], vec![0], 0));
        assert_eq!(batch, Batch::minimal_from(vec![1], vec![0], 1));
    }

    proptest::proptest! {
        #[test]
        fn test_split_in_batch(original in arb_batch(2..100)) {
            for time in original.time().unwrap().values().iter().dedup() {
                let time = RowTime::from_timestamp_ns(*time);

                let mut remainder = original.clone();

                if let Some(result) = remainder.split_up_to(time) {
                    // The result of the split is complete up to the requested time.
                    prop_assert_eq!(result.up_to_time, time);
                    // The remainder is complete up to the original time.
                    prop_assert_eq!(remainder.up_to_time, original.up_to_time);

                    // create the concatenated result
                    let concatenated = match (result.data(), remainder.data()) {
                        (None, None) => unreachable!(),
                        (Some(a), None) => a.clone(),
                        (None, Some(b)) => b.clone(),
                        (Some(a), Some(b)) => arrow_select::concat::concat(&[a.as_ref(), b.as_ref()]).unwrap(),
                    };
                    prop_assert_eq!(&concatenated, original.data().unwrap());

                    prop_assert!(result.data.is_some());
                    let result = result.data.unwrap();

                    prop_assert_eq!(result.data.len(), result.time.len());
                    prop_assert_eq!(result.time().values()[0], i64::from(result.min_present_time));
                    prop_assert_eq!(result.time().values()[result.time.len() - 1], i64::from(result.max_present_time));

                    if time == original.max_present_time().unwrap() {
                        // If the time equalled the max present time, then we consume the batch.
                        prop_assert!(remainder.is_empty());
                    } else {
                        prop_assert!(remainder.data.is_some());
                        let remainder = remainder.data.unwrap();

                        prop_assert_eq!(remainder.data.len(), remainder.time.len());
                        prop_assert_eq!(remainder.time().values()[0], i64::from(remainder.min_present_time));
                        prop_assert_eq!(remainder.time().values()[remainder.time.len() - 1], i64::from(remainder.max_present_time));
                    }
                } else {
                    prop_assert!(false)
                }
            }
        }
    }
}
