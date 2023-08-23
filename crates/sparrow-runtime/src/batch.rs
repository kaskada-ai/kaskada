use anyhow::Context;
use arrow::array::{ArrayRef, TimestampNanosecondArray};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDateTime;
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_core::{KeyTriple, KeyTriples};

/// Represents a batch to be processed.
///
/// Each batch contains 0 or more rows of data and covers a range of time.
#[derive(Debug, Clone)]
pub struct Batch {
    /// The first key triple in the record batch.
    ///
    /// This represents the lower bound of the range of rows
    /// this record batch contains. Although certain operations
    /// may filter rows out, the bounds remain. This ensures we
    /// continue to make progress on downstream operations.
    /// The minimum row in the data may be past this key triple.
    pub lower_bound: KeyTriple,

    /// The last key triple in the record batch.
    ///
    /// This represents the upper bound of the range of rows
    /// this record batch contains. Although certain operations
    /// may filter rows out, the bounds remain. This ensures we
    /// continue to make progress on downstream operations.
    /// The maximum row in the data may be before this key triple.
    pub upper_bound: KeyTriple,

    /// The data contained in this batch.
    ///
    /// The data is sorted by the key triples.
    pub data: RecordBatch,
}

impl Batch {
    /// Construct a new batch, inferring the lower and upper bound from the
    /// data.
    ///
    /// It is expected that that data is sorted.
    ///
    /// This should be used only when the initial [Batch] is created while
    /// reading a table. Otherwise, bounds should be preserved using
    /// [Batch::with_data] or [Batch::try_new_bounds].
    pub fn try_new_from_batch(data: RecordBatch) -> anyhow::Result<Self> {
        anyhow::ensure!(
            data.num_rows() > 0,
            "Unable to create batch from empty data"
        );

        let key_triples = KeyTriples::try_from(&data)?;
        Self::try_new_with_bounds(
            data,
            key_triples.first().context("First key triple")?,
            key_triples.last().context("Last key triple")?,
        )
    }

    pub fn try_new_with_bounds(
        data: RecordBatch,
        lower_bound: KeyTriple,
        upper_bound: KeyTriple,
    ) -> anyhow::Result<Self> {
        #[cfg(debug_assertions)]
        validate(&data, &lower_bound, &upper_bound)?;

        Ok(Self {
            lower_bound,
            upper_bound,
            data,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.data.schema()
    }

    pub fn data(&self) -> &RecordBatch {
        &self.data
    }

    pub fn column(&self, i: usize) -> &ArrayRef {
        self.data.column(i)
    }

    pub fn columns(&self) -> &[ArrayRef] {
        self.data.columns()
    }

    pub fn lower_bound_as_date(&self) -> anyhow::Result<NaiveDateTime> {
        arrow::temporal_conversions::timestamp_ns_to_datetime(self.lower_bound.time)
            .context("convert lower bound to date")
    }

    pub fn upper_bound_as_date(&self) -> anyhow::Result<NaiveDateTime> {
        arrow::temporal_conversions::timestamp_ns_to_datetime(self.upper_bound.time)
            .context("convert upper bound to date")
    }

    /// Return the array of times within this.
    pub fn times(&self) -> anyhow::Result<&[i64]> {
        // TODO: Consider storing the downcast version of the column (possibly in an
        // `ArcRef`)?
        let times: &TimestampNanosecondArray =
            downcast_primitive_array(self.data.column(0).as_ref())?;
        Ok(times.values())
    }

    pub fn is_empty(&self) -> bool {
        self.data.num_rows() == 0
    }

    pub fn num_rows(&self) -> usize {
        self.data.num_rows()
    }

    /// Create a new batch with the given data and the same bounds.
    pub fn with_data(&self, data: RecordBatch) -> Self {
        Self {
            data,
            lower_bound: self.lower_bound,
            upper_bound: self.upper_bound,
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let data = self.data.slice(offset, length);
        Self::try_new_from_batch(data).expect("slicing batch produces valid batch")
    }

    #[cfg(test)]
    pub fn batch_from_nanos(start: i64, end: i64) -> Batch {
        let times: Vec<i64> = (start..=end).collect();
        let time = TimestampNanosecondArray::from_iter_values(times.iter().copied());
        Self::test_batch_random_keys(time)
    }

    #[cfg(test)]
    pub fn batch_from_dates(start: NaiveDateTime, end: NaiveDateTime) -> Batch {
        let times: Vec<i64> = (start.timestamp_nanos()..=end.timestamp_nanos()).collect();
        let time = TimestampNanosecondArray::from_iter_values(times.iter().copied());
        Self::test_batch_random_keys(time)
    }

    // Creates a batch from the time array and the given subsort/key repeated.
    #[cfg(test)]
    pub fn test_batch(time: TimestampNanosecondArray, subsort: u64, key: u64) -> Batch {
        use std::sync::Arc;

        use arrow::array::UInt64Array;
        use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, TimestampNanosecondType};

        let subsort = UInt64Array::from_iter_values(std::iter::repeat(subsort).take(time.len()));
        let key_hash = UInt64Array::from_iter_values(std::iter::repeat(key).take(time.len()));
        let schema = Schema::new(vec![
            Field::new("_time", TimestampNanosecondType::DATA_TYPE, false),
            Field::new("_subsort", DataType::UInt64, false),
            Field::new("_key_hash", DataType::UInt64, false),
        ]);
        let schema = Arc::new(schema);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(time), Arc::new(subsort), Arc::new(key_hash)],
        )
        .unwrap();
        Batch::try_new_from_batch(batch).unwrap()
    }

    // Creates a batch from the time array, with randomized subsort/keys.
    //
    // Note this isn't smart enough to ensure increasing random keys, so it assumes
    // that the time is strictly increasing.
    #[cfg(test)]
    pub fn test_batch_random_keys(time: TimestampNanosecondArray) -> Batch {
        use std::sync::Arc;

        use arrow::array::UInt64Array;
        use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, TimestampNanosecondType};
        use rand::Rng;

        let mut rng = rand::thread_rng();
        let vals: Vec<u64> = (0..time.len())
            .map(|_| rng.gen_range(0..10) as u64)
            .collect();

        let subsort = UInt64Array::from_iter_values(vals.clone());
        let key_hash = UInt64Array::from_iter_values(vals);
        let schema = Schema::new(vec![
            Field::new("_time", TimestampNanosecondType::DATA_TYPE, false),
            Field::new("_subsort", DataType::UInt64, false),
            Field::new("_key_hash", DataType::UInt64, false),
        ]);
        let schema = Arc::new(schema);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(time), Arc::new(subsort), Arc::new(key_hash)],
        )
        .unwrap();
        Batch::try_new_from_batch(batch).unwrap()
    }

    #[allow(dead_code)]
    pub fn as_json(&self) -> AsJson<'_> {
        AsJson(self)
    }
}

pub(crate) fn validate_batch_schema(schema: &Schema) -> anyhow::Result<()> {
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

#[cfg(debug_assertions)]
/// Validate that the result is totally sorted and that the lower and upper
/// bound are correct.
pub(crate) fn validate_bounds(
    time: &ArrayRef,
    subsort: &ArrayRef,
    key_hash: &ArrayRef,
    lower_bound: &KeyTriple,
    upper_bound: &KeyTriple,
) -> anyhow::Result<()> {
    use arrow::array::UInt64Array;
    if time.len() == 0 {
        // No more validation necessary for empty batches.
        return Ok(());
    }

    let time: &TimestampNanosecondArray = downcast_primitive_array(time)?;
    let subsort: &UInt64Array = downcast_primitive_array(subsort)?;
    let key_hash: &UInt64Array = downcast_primitive_array(key_hash)?;

    let mut prev_time = lower_bound.time;
    let mut prev_subsort = lower_bound.subsort;
    let mut prev_key_hash = lower_bound.key_hash;

    let curr_time = time.value(0);
    let curr_subsort = subsort.value(0);
    let curr_key_hash = key_hash.value(0);

    let order = prev_time
        .cmp(&curr_time)
        .then(prev_subsort.cmp(&curr_subsort))
        .then(prev_key_hash.cmp(&curr_key_hash));

    anyhow::ensure!(
        order.is_le(),
        "Expected lower_bound <= data[0], but ({}, {}, {}) > ({}, {}, {})",
        prev_time,
        prev_subsort,
        prev_key_hash,
        curr_time,
        curr_subsort,
        curr_key_hash
    );

    prev_time = curr_time;
    prev_subsort = curr_subsort;
    prev_key_hash = curr_key_hash;

    for i in 1..time.len() {
        let curr_time = time.value(i);
        let curr_subsort = subsort.value(i);
        let curr_key_hash = key_hash.value(i);

        let order = prev_time
            .cmp(&curr_time)
            .then(prev_subsort.cmp(&curr_subsort))
            .then(prev_key_hash.cmp(&curr_key_hash));

        anyhow::ensure!(
            order.is_lt(),
            "Expected data[i - 1] < data[i], but ({}, {}, {}) >= ({}, {}, {})",
            prev_time,
            prev_subsort,
            prev_key_hash,
            curr_time,
            curr_subsort,
            curr_key_hash
        );

        prev_time = curr_time;
        prev_subsort = curr_subsort;
        prev_key_hash = curr_key_hash;
    }

    let curr_time = upper_bound.time;
    let curr_subsort = upper_bound.subsort;
    let curr_key_hash = upper_bound.key_hash;

    let order = prev_time
        .cmp(&curr_time)
        .then(prev_subsort.cmp(&curr_subsort))
        .then(prev_key_hash.cmp(&curr_key_hash));

    anyhow::ensure!(
        order.is_le(),
        "Expected last data <= upper bound, but ({}, {}, {}) > ({}, {}, {})",
        prev_time,
        prev_subsort,
        prev_key_hash,
        curr_time,
        curr_subsort,
        curr_key_hash
    );

    Ok(())
}

#[cfg(debug_assertions)]
fn validate(
    data: &RecordBatch,
    lower_bound: &KeyTriple,
    upper_bound: &KeyTriple,
) -> anyhow::Result<()> {
    validate_batch_schema(data.schema().as_ref())?;

    for key_column in 0..3 {
        anyhow::ensure!(
            data.column(key_column).null_count() == 0,
            "Key column '{}' should not contain null",
            data.schema().field(key_column).name()
        );
    }

    validate_bounds(
        data.column(0),
        data.column(1),
        data.column(2),
        lower_bound,
        upper_bound,
    )
}

fn validate_key_column(
    schema: &Schema,
    index: usize,
    expected_name: &str,
    expected_type: &arrow::datatypes::DataType,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        schema.field(index).name() == expected_name,
        "Expected column {} to be '{}' but was '{}'",
        index,
        expected_name,
        schema.field(index).name()
    );
    anyhow::ensure!(
        schema.field(index).data_type() == expected_type,
        "Key column '{}' should be '{:?}' but was '{:?}'",
        expected_name,
        schema.field(index).data_type(),
        expected_type,
    );

    Ok(())
}

/// A channel for sending record batches.
pub type BatchSender = tokio::sync::mpsc::Sender<Batch>;

/// A channel for receiving record batches.
pub type BatchReceiver = tokio::sync::mpsc::Receiver<Batch>;

#[repr(transparent)]
pub struct AsJson<'a>(&'a Batch);

impl<'a> std::fmt::Display for AsJson<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut json_string = Vec::new();
        let mut writer = arrow::json::LineDelimitedWriter::new(&mut json_string);

        writer.write_batches(&[&self.0.data]).map_err(|e| {
            tracing::error!("Error formatting batch: {}", e);
            std::fmt::Error
        })?;
        writer.finish().map_err(|e| {
            tracing::error!("Error formatting batch: {}", e);
            std::fmt::Error
        })?;
        let json_string = String::from_utf8(json_string).map_err(|e| {
            tracing::error!("Error formatting batch: {}", e);
            std::fmt::Error
        })?;

        write!(f, "{json_string}")
    }
}

impl sparrow_merge::old::InputItem for Batch {
    fn min_time(&self) -> i64 {
        self.lower_bound.time
    }

    fn max_time(&self) -> i64 {
        self.upper_bound.time
    }

    fn split_at(self, split_time: i64) -> anyhow::Result<(Option<Self>, Option<Self>)> {
        if self.is_empty() {
            return Ok((None, None));
        } else if split_time <= self.min_time() {
            return Ok((None, Some(self)));
        } else if split_time > self.max_time() {
            return Ok((Some(self), None));
        }

        let times = self.times()?;
        let split_point = match times.binary_search(&split_time) {
            Ok(mut found_index) => {
                // Just do a linear search for the first value less than split time.
                while found_index > 0 && times[found_index - 1] == split_time {
                    found_index -= 1
                }
                found_index
            }
            Err(not_found_index) => not_found_index,
        };

        let lt = if split_point > 0 {
            let lt = self.data.slice(0, split_point);
            Some(Batch::try_new_from_batch(lt)?)
        } else {
            None
        };

        let gte = if split_point < self.num_rows() {
            let gte = self.data.slice(split_point, self.num_rows() - split_point);
            Some(Batch::try_new_from_batch(gte)?)
        } else {
            None
        };
        Ok((lt, gte))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field};
    use itertools::Itertools;
    use proptest::prelude::*;
    use sparrow_core::TableSchema;
    use sparrow_merge::old::InputItem;

    use super::*;
    use sparrow_merge::old::testing::{arb_i64_array, arb_key_triples};

    fn arb_batch(max_len: usize) -> impl Strategy<Value = RecordBatch> {
        (1..max_len)
            .prop_flat_map(|len| (arb_key_triples(len), arb_i64_array(len)))
            .prop_map(|((time, subsort, key_hash), values)| {
                let schema = TableSchema::from_data_fields([Arc::new(Field::new(
                    "data",
                    DataType::Int64,
                    true,
                ))])
                .unwrap();
                let schema = schema.schema_ref();
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(time),
                        Arc::new(subsort),
                        Arc::new(key_hash),
                        Arc::new(values),
                    ],
                )
                .unwrap()
            })
    }

    proptest! {
        #[test]
        fn test_splitting(batch in arb_batch(1000)) {
            // For every time value in the batch, try splitting there and make sure
            // the ordering constraints are satisfied.
            let input = Batch::try_new_from_batch(batch).unwrap();
            let times = input.times().unwrap();

            for split_time in times.iter().dedup() {
                let (lt, gte) = input.clone().split_at(*split_time).unwrap();

                if let Some(lt) = lt {
                    lt.times().unwrap().iter().all(|t| *t < *split_time);
                }
                if let Some(gte) = gte {
                    gte.times().unwrap().iter().all(|t| *t >= *split_time);
                }
            }
        }
    }
}
