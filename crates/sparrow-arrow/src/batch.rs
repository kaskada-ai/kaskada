use arrow::error::ArrowError;
use arrow_array::types::{TimestampNanosecondType, UInt64Type};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, RecordBatch, TimestampNanosecondArray,
    UInt64Array,
};
use arrow_schema::SchemaRef;
use error_stack::{IntoReport, ResultExt};
use itertools::Itertools;

use crate::downcast::downcast_primitive_array;
use crate::RowTime;

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

    pub fn is_empty(&self) -> bool {
        self.data.is_none()
    }

    pub fn num_rows(&self) -> usize {
        match &self.data {
            Some(data) => data.batch.num_rows(),
            None => 0,
        }
    }

    pub fn into_record_batch(self) -> Option<RecordBatch> {
        self.data.map(|data| data.batch)
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

    pub fn record_batch(&self) -> Option<&RecordBatch> {
        match &self.data {
            Some(info) => Some(&info.batch),
            None => None,
        }
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
        batch: RecordBatch,
        time: ArrayRef,
        subsort: ArrayRef,
        key_hash: ArrayRef,
        up_to_time: RowTime,
    ) -> Self {
        debug_assert_eq!(batch.num_rows(), time.len());
        debug_assert_eq!(batch.num_rows(), subsort.len());
        debug_assert_eq!(batch.num_rows(), key_hash.len());
        debug_assert_eq!(time.data_type(), &TimestampNanosecondType::DATA_TYPE);
        debug_assert_eq!(subsort.data_type(), &UInt64Type::DATA_TYPE);
        debug_assert_eq!(key_hash.data_type(), &UInt64Type::DATA_TYPE);
        let data = if batch.num_rows() == 0 {
            None
        } else {
            Some(BatchInfo::new(batch, time, subsort, key_hash))
        };

        Self { data, up_to_time }
    }

    /// Return a new `Batch` with the same time properties, but new data.
    pub fn with_projection(&self, new_batch: RecordBatch) -> Self {
        assert_eq!(new_batch.num_rows(), self.num_rows());
        Self {
            data: self.data.as_ref().map(|data| BatchInfo {
                batch: new_batch,
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

    pub fn concat(
        schema: &SchemaRef,
        batches: Vec<Batch>,
        up_to_time: RowTime,
    ) -> error_stack::Result<Batch, Error> {
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
            .change_context(Error::Internal)?;

        let subsorts: Vec<_> = batches
            .iter()
            .flat_map(|batch| batch.subsort())
            .map(|batch| -> &dyn arrow_array::Array { batch })
            .collect();
        let subsort = arrow_select::concat::concat(&subsorts)
            .into_report()
            .change_context(Error::Internal)?;

        let key_hashes: Vec<_> = batches
            .iter()
            .flat_map(|batch| batch.key_hash())
            .map(|batch| -> &dyn arrow_array::Array { batch })
            .collect();
        let key_hash = arrow_select::concat::concat(&key_hashes)
            .into_report()
            .change_context(Error::Internal)?;

        let batches: Vec<_> = batches
            .into_iter()
            .flat_map(|batch| batch.into_record_batch())
            .collect();
        let batch = arrow_select::concat::concat_batches(schema, &batches)
            .into_report()
            .change_context(Error::Internal)?;

        Ok(Self {
            data: Some(BatchInfo {
                batch,
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
                let batch =
                    take_record_batch(&info.batch, indices).change_context(Error::Internal)?;
                let time = arrow_select::take::take(info.time.as_ref(), indices, None)
                    .into_report()
                    .change_context(Error::Internal)?;
                let subsort = arrow_select::take::take(info.subsort.as_ref(), indices, None)
                    .into_report()
                    .change_context(Error::Internal)?;
                let key_hash = arrow_select::take::take(info.key_hash.as_ref(), indices, None)
                    .into_report()
                    .change_context(Error::Internal)?;
                let info = BatchInfo {
                    batch,
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

    // TODO: Use "filter-bits" to avoid eagerly creating new columns.
    pub fn filter(&self, predicate: &BooleanArray) -> error_stack::Result<Self, Error> {
        match &self.data {
            Some(info) => {
                let filter = arrow_select::filter::FilterBuilder::new(predicate)
                    .optimize()
                    .build();
                let columns: Vec<_> = info
                    .batch
                    .columns()
                    .iter()
                    .map(|column| filter.filter(column))
                    .try_collect()
                    .into_report()
                    .change_context(Error::Internal)?;

                let batch = RecordBatch::try_new(info.batch.schema(), columns)
                    .into_report()
                    .change_context(Error::Internal)?;

                // TODO: This is unnecessary if `time` and `key_hash` were already in the batch.
                // We should figure out how to avoid the redundant work.
                let time = filter
                    .filter(&info.time)
                    .into_report()
                    .change_context(Error::Internal)?;
                let subsort = filter
                    .filter(&info.subsort)
                    .into_report()
                    .change_context(Error::Internal)?;
                let key_hash = filter
                    .filter(&info.key_hash)
                    .into_report()
                    .change_context(Error::Internal)?;
                let info = BatchInfo {
                    batch,
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
                assert_eq!(predicate.len(), 0);
                Ok(self.clone())
            }
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        match &self.data {
            Some(info) => {
                let info = BatchInfo {
                    batch: info.batch.slice(offset, length),
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
        use std::sync::Arc;

        let time: TimestampNanosecondArray = time.into();
        let subsort: UInt64Array = (0..(time.len() as u64)).collect_vec().into();
        let key_hash: UInt64Array = key_hash.into();

        let time: ArrayRef = Arc::new(time);
        let subsort: ArrayRef = Arc::new(subsort);
        let key_hash: ArrayRef = Arc::new(key_hash);

        let batch =
            RecordBatch::try_new(MINIMAL_SCHEMA.clone(), vec![time.clone(), key_hash.clone()])
                .unwrap();

        Batch::new_with_data(
            batch,
            time,
            subsort,
            key_hash,
            RowTime::from_timestamp_ns(up_to_time),
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BatchInfo {
    pub(crate) batch: RecordBatch,
    pub(crate) time: ArrayRef,
    pub(crate) subsort: ArrayRef,
    pub(crate) key_hash: ArrayRef,
    min_present_time: RowTime,
    max_present_time: RowTime,
}

impl PartialEq for BatchInfo {
    fn eq(&self, other: &Self) -> bool {
        self.batch == other.batch
            && self.time.as_ref() == other.time.as_ref()
            && self.min_present_time == other.min_present_time
            && self.max_present_time == other.max_present_time
    }
}

impl BatchInfo {
    fn new(batch: RecordBatch, time: ArrayRef, subsort: ArrayRef, key_hash: ArrayRef) -> Self {
        debug_assert_eq!(batch.num_rows(), time.len());
        debug_assert_eq!(batch.num_rows(), subsort.len());
        debug_assert_eq!(batch.num_rows(), key_hash.len());
        debug_assert!(batch.num_rows() > 0);
        debug_assert_eq!(time.null_count(), 0);

        let time_column: &TimestampNanosecondArray =
            downcast_primitive_array(time.as_ref()).expect("time column to be timestamp");

        // Debug assertion that the array is sorted by time.
        // Once `is_sorted` is stable (https://github.com/rust-lang/rust/issues/53485).
        // debug_assert!(time_column.iter().is_sorted());
        //
        debug_assert!(time_column.iter().tuple_windows().all(|(a, b)| a <= b));

        let min_present_time = RowTime::from_timestamp_ns(time_column.values()[0]);
        let max_present_time =
            RowTime::from_timestamp_ns(time_column.values()[time_column.len() - 1]);

        Self {
            batch,
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
        let time_column: &TimestampNanosecondArray =
            downcast_primitive_array(self.time.as_ref()).expect("time column to be timestamp");

        // 0..slice_start   (len = slice_start)       = what we return
        // slice_start..len (len = len - slice_start) = what we leave in self
        let slice_start = time_column
            .values()
            .partition_point(|time| RowTime::from_timestamp_ns(*time) <= time_inclusive);
        let slice_len = self.batch.num_rows() - slice_start;

        let max_result_time = RowTime::from_timestamp_ns(time_column.values()[slice_start - 1]);
        let min_self_time = RowTime::from_timestamp_ns(time_column.values()[slice_start]);

        let result = Self {
            batch: self.batch.slice(0, slice_start),
            time: self.time.slice(0, slice_start),
            subsort: self.subsort.slice(0, slice_start),
            key_hash: self.key_hash.slice(0, slice_start),
            min_present_time: self.min_present_time,
            max_present_time: max_result_time,
        };

        self.batch = self.batch.slice(slice_start, slice_len);
        self.time = self.time.slice(slice_start, slice_len);
        self.key_hash = self.key_hash.slice(slice_start, slice_len);
        self.min_present_time = min_self_time;

        result
    }

    pub(crate) fn time(&self) -> &TimestampNanosecondArray {
        downcast_primitive_array(self.time.as_ref()).expect("type checked in constructor")
    }

    pub(crate) fn subsort(&self) -> &UInt64Array {
        downcast_primitive_array(self.subsort.as_ref()).expect("type checked in constructor")
    }

    pub(crate) fn key_hash(&self) -> &UInt64Array {
        downcast_primitive_array(self.key_hash.as_ref()).expect("type checked in constructor")
    }
}

#[derive(Debug, derive_more::Display)]
pub enum Error {
    #[display(fmt = "internal error")]
    Internal,
}

impl error_stack::Context for Error {}

pub fn take_record_batch(
    batch: &RecordBatch,
    indices: &UInt64Array,
) -> error_stack::Result<RecordBatch, ArrowError> {
    // Produce batches based on indices
    let columns = batch
        .columns()
        .iter()
        .map(|c| arrow_select::take::take(c.as_ref(), indices, None).into_report())
        .try_collect()?;

    RecordBatch::try_new(batch.schema(), columns).into_report()
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
    use arrow_select::concat::concat_batches;
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
                    let concatenated = match (result.record_batch(), remainder.record_batch()) {
                        (None, None) => unreachable!(),
                        (Some(a), None) => a.clone(),
                        (None, Some(b)) => b.clone(),
                        (Some(a), Some(b)) => concat_batches(&a.schema(), &[a.clone(), b.clone()]).unwrap(),
                    };
                    prop_assert_eq!(&concatenated, original.record_batch().unwrap());

                    prop_assert!(result.data.is_some());
                    let result = result.data.unwrap();

                    prop_assert_eq!(result.batch.num_rows(), result.time.len());
                    prop_assert_eq!(result.time().values()[0], i64::from(result.min_present_time));
                    prop_assert_eq!(result.time().values()[result.time.len() - 1], i64::from(result.max_present_time));

                    if time == original.max_present_time().unwrap() {
                        // If the time equalled the max present time, then we consume the batch.
                        prop_assert!(remainder.is_empty());
                    } else {
                        prop_assert!(remainder.data.is_some());
                        let remainder = remainder.data.unwrap();

                        prop_assert_eq!(remainder.batch.num_rows(), remainder.time.len());
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
