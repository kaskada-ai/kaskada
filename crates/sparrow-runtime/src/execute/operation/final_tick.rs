use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

use crate::execute::Error;
use arrow::array::{
    Array, ArrayRef, BooleanArray, TimestampNanosecondArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Schema, SchemaRef, TimestampNanosecondType,
};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::StreamExt;
use itertools::{izip, Itertools};
use serde::{Deserialize, Serialize};
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_core::KeyTriple;
use sparrow_instructions::{ComputeStore, GroupingIndices, StoreKey};
use static_init::dynamic;

use super::expression_executor::InputColumn;
use super::sorted_key_hash_map::SortedKeyHashMap;
use super::{BoxedOperation, Operation, OperationContext};
use crate::execute::operation::InputBatch;
use crate::Batch;

/// Max number of rows a tick batch produces at once.
const MAX_TICK_ROWS: usize = 100_000;

#[static_init::dynamic]
static TICK_SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
    Field::new("_time", TimestampNanosecondType::DATA_TYPE, false),
    Field::new("_subsort", DataType::UInt64, false),
    Field::new("_key_hash", DataType::UInt64, false),
    Field::new("_tick", DataType::Boolean, false),
]));

/// Holds state necessary to produce the final tick batch.
///
/// This operation is responsible for producing a final tick at the
/// time of the last input + 1ns. The reason the ns is added is because
/// of ordering constraints. For example, say that at the final time T, entities
/// [A,G] were input and produced. The final tick then enumerates all entities,
/// which include the set [A,B,C,G]. However, our ordering requires entities
/// be ordered in time. Because we've already produced entity G at time T,
/// we panic when attempting to produce entities [A,B,C] again at T.
///
/// Note this is separated from [TickOperation] to reduce complexity, at the
/// cost of some duplicated code. Likely could create a `TickOperation` trait
/// with shared code, and pass a `dyn TickOperation` around to reduce
/// duplication.
pub(super) struct FinalTickOperation {
    /// Stream of input batches
    input_stream: Pin<Box<dyn futures::Stream<Item = Batch> + Send>>,

    /// The key hashes this tick operation has seen.
    key_hashes: SortedKeyHashMap,

    /// The current time seen by the operation in timestamp nanoseconds.
    ///
    /// The final tick occurs at the time plus 1ns once it has
    /// stopped receiving incoming batches.
    current_time: i64,

    /// If this is set, the final tick should occur at this time.
    /// If not, the final tick should occur at the last input time + 1ns.
    tick_at: Option<NaiveDateTime>,
}

impl std::fmt::Debug for FinalTickOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // don't include input_stream (not meaningful) or key_hashes (potentially very large)
        f.debug_struct("FinalTickOperation")
            .field("current_time", &self.current_time)
            .field("key_hashes", &format!("{} entries", self.key_hashes.len()))
            .finish_non_exhaustive()
    }
}

#[derive(Serialize, Deserialize)]
struct FinalTickOperationState {
    current_time: i64,
    tick_at: Option<NaiveDateTime>,
}

impl FinalTickOperation {
    /// Create the stream of input batches for a tick operation.
    pub(super) fn create(
        context: &mut OperationContext,
        input_channels: Vec<tokio::sync::mpsc::Receiver<Batch>>,
        input_columns: &[InputColumn],
    ) -> error_stack::Result<BoxedOperation, super::Error> {
        let input_channel = input_channels
            .into_iter()
            .exactly_one()
            .into_report()
            .change_context(Error::internal_msg("expected one channel"))?;
        let input_stream = tokio_stream::wrappers::ReceiverStream::new(input_channel).boxed();

        debug_assert!(
            input_columns[0].input_ref.input_column == 0,
            "Tick column should have 0th input index"
        );

        Ok(Box::new(Self {
            input_stream,
            key_hashes: SortedKeyHashMap::new(),
            current_time: 0,
            tick_at: context.output_at_time,
        }))
    }

    fn update_internal_state(&mut self, incoming: &Batch) -> error_stack::Result<(), super::Error> {
        if incoming.num_rows() > 0 {
            let key_hash_array: &UInt64Array =
                downcast_primitive_array(incoming.column(2).as_ref())
                    .into_report()
                    .change_context(Error::internal())?;
            self.key_hashes
                .get_or_update_indices(key_hash_array)
                .into_report()
                .change_context(Error::internal())?;
        }
        self.current_time = incoming.upper_bound.time;
        Ok(())
    }

    /// Returns the new entities discovered in the batch and their earliest
    /// times.
    ///
    /// Does not mutate the state's known entities.
    fn discover_entities(
        &self,
        batch: &Batch,
    ) -> error_stack::Result<BTreeMap<u64, i64>, super::Error> {
        let mut discovered = BTreeMap::new();
        let keys: &UInt64Array = downcast_primitive_array(batch.column(2).as_ref())
            .into_report()
            .change_context(Error::internal_msg("downcasting key hash"))?;
        let time: &TimestampNanosecondArray = downcast_primitive_array(batch.column(0).as_ref())
            .into_report()
            .change_context(Error::internal_msg("downcasting time"))?;
        for (key_hash, time) in izip!(keys, time) {
            let key_hash = key_hash.expect("non-null key");
            let time = time.expect("non-null time");
            if !self.key_hashes.contains_key(key_hash) {
                discovered.entry(key_hash).or_insert(time);
            }
        }
        Ok(discovered)
    }
}

#[async_trait]
impl Operation for FinalTickOperation {
    fn restore_from(
        &mut self,
        operation_index: u8,
        compute_store: &ComputeStore,
    ) -> anyhow::Result<()> {
        self.key_hashes
            .restore_from(operation_index, compute_store)?;
        let state = compute_store
            .get(&StoreKey::new_tick_state(operation_index))?
            .unwrap_or(FinalTickOperationState {
                current_time: 0,
                tick_at: self.tick_at,
            });
        self.current_time = state.current_time;

        Ok(())
    }

    fn store_to(&self, operation_index: u8, compute_store: &ComputeStore) -> anyhow::Result<()> {
        self.key_hashes.store_to(operation_index, compute_store)?;
        let state = FinalTickOperationState {
            current_time: self.current_time,
            tick_at: self.tick_at,
        };
        compute_store.put(&StoreKey::new_tick_state(operation_index), &state)?;
        Ok(())
    }

    async fn execute(
        &mut self,
        sender: tokio::sync::mpsc::Sender<InputBatch>,
    ) -> error_stack::Result<(), super::Error> {
        'outer: while let Some(incoming) = self.input_stream.next().await {
            if let Some(tick_at) = self.tick_at {
                let upper_bound = incoming
                    .upper_bound_as_date()
                    .into_report()
                    .change_context(Error::internal_msg("converting upper bound to date"))?;
                if upper_bound >= tick_at {
                    // Since we know we only produce a single batch from this operation before
                    // exiting, we know that we haven't yet produced a tick batch here.
                    // Add entities before the tick time to the known key hashes.
                    let discovered_entities = self.discover_entities(&incoming)?;
                    let keys_before_tick = discovered_entities
                        .iter()
                        .filter(|(_, t)| **t <= tick_at.timestamp_nanos())
                        .map(|(k, _)| *k);
                    self.key_hashes.extend(keys_before_tick);

                    break 'outer;
                } else {
                    self.update_internal_state(&incoming)?
                }
            } else {
                self.update_internal_state(&incoming)?
            }

            // Send an empty batch with bounds to allow downstream consumers to progress
            let empty_batch = InputBatch::new_empty(
                TICK_SCHEMA.clone(),
                incoming.lower_bound,
                incoming.upper_bound,
            );

            sender
                .send(empty_batch)
                .await
                .into_report()
                .change_context(Error::internal())?;
        }

        if !self.key_hashes.is_empty() {
            if let Some(tick_at) = self.tick_at {
                send_tick_batch(tick_at.timestamp_nanos() + 1, &self.key_hashes, &sender).await?;
            } else {
                send_tick_batch(self.current_time + 1, &self.key_hashes, &sender).await?;
            }
        }

        Ok(())
    }
}

/// Sends a tick batch at the given tick time, chunking if necessary.
async fn send_tick_batch(
    tick_nanos: i64,
    key_hashes: &SortedKeyHashMap,
    sender: &tokio::sync::mpsc::Sender<InputBatch>,
) -> error_stack::Result<(), super::Error> {
    error_stack::ensure!(
        !key_hashes.is_empty(),
        crate::execute::error::invalid_operation!("key hashes should be non-zero")
    );

    let key_chunks = futures::stream::iter(key_hashes.keys()).chunks(MAX_TICK_ROWS);
    let value_chunks = futures::stream::iter(key_hashes.values()).chunks(MAX_TICK_ROWS);
    let mut key_value_chunks = key_chunks.zip(value_chunks);
    while let Some((key_chunk, value_chunk)) = key_value_chunks.next().await {
        let keys = UInt64Array::from(key_chunk);
        let grouping = UInt32Array::from(value_chunk);
        let grouping = GroupingIndices::new(key_hashes.len(), grouping);

        let (first_key, last_key) = (keys.value(0), keys.value(keys.len() - 1));
        let key_column: ArrayRef = Arc::new(keys);
        let len = key_column.len();

        // TODO: Create the time column and re-use it. Ditto for subsort.
        // SAFETY: We create the iterator with a known / fixed length.
        let time_column = unsafe {
            TimestampNanosecondArray::from_trusted_len_iter(
                std::iter::repeat(Some(tick_nanos)).take(len),
            )
        };
        let time_column: ArrayRef = Arc::new(time_column);

        // The subsort value is set to `u64::MAX` in order to ensure ticks are
        // processed after all other rows at the same time.
        let subsort_column =
    // SAFETY: We create the iterator with a known / fixed length.
        unsafe { UInt64Array::from_trusted_len_iter(std::iter::repeat(Some(u64::MAX)).take(len)) };
        let subsort_column: ArrayRef = Arc::new(subsort_column);

        // Create a tick column consisting of booleans set to `true`.
        let tick_column: ArrayRef = true_column(len);
        let input_columns: Vec<ArrayRef> = vec![tick_column];

        // The tick batches only occur at one time, so the bounds are at that time.
        let lower_bound = KeyTriple {
            time: tick_nanos,
            subsort: u64::MAX,
            key_hash: first_key,
        };
        let upper_bound = KeyTriple {
            time: tick_nanos,
            subsort: u64::MAX,
            key_hash: last_key,
        };

        let input_batch = InputBatch {
            time: time_column,
            subsort: subsort_column,
            key_hash: key_column,
            grouping,
            input_columns,
            lower_bound,
            upper_bound,
        };

        sender
            .send(input_batch)
            .await
            .into_report()
            .change_context(Error::internal_msg("sending tick batch"))?
    }
    Ok(())
}

/// Allows us to reuse slices of these arrays when creating tick batches
/// rather than allocating a new one each time.
#[dynamic]
static TRUE_COLUMN: ArrayRef = {
    let all_true: BooleanArray = std::iter::repeat(Some(true)).take(MAX_TICK_ROWS).collect();
    Arc::new(all_true)
};

/// Create a true column of the given size.
///
/// Attempts to return a reference to the cached `TRUE_COLUMN` if possible.
fn true_column(len: usize) -> ArrayRef {
    match len.cmp(&MAX_TICK_ROWS) {
        std::cmp::Ordering::Less => TRUE_COLUMN.slice(0, len),
        std::cmp::Ordering::Equal => TRUE_COLUMN.clone(),
        std::cmp::Ordering::Greater => {
            let all_true: BooleanArray = std::iter::repeat(Some(true)).take(len).collect();
            Arc::new(all_true)
        }
    }
}

#[cfg(test)]
mod tests {

    use arrow::array::{TimestampNanosecondArray, UInt64Array};
    use chrono::NaiveDateTime;

    use super::*;
    use crate::Batch;

    fn default_final_tick_operation(
        input_stream: Pin<Box<dyn futures::Stream<Item = Batch> + Send>>,
    ) -> FinalTickOperation {
        FinalTickOperation {
            input_stream,
            current_time: 0,
            key_hashes: SortedKeyHashMap::new(),
            tick_at: None,
        }
    }

    fn input_stream() -> (
        tokio::sync::mpsc::Sender<Batch>,
        Pin<Box<dyn futures::Stream<Item = Batch> + Send>>,
    ) {
        let (sender, receiver) = tokio::sync::mpsc::channel(10);
        (
            sender,
            tokio_stream::wrappers::ReceiverStream::new(receiver).boxed(),
        )
    }

    fn operation_stream() -> (
        tokio::sync::mpsc::Sender<InputBatch>,
        Pin<Box<dyn futures::Stream<Item = InputBatch> + Send>>,
    ) {
        let (sender, receiver) = tokio::sync::mpsc::channel(10);
        (
            sender,
            tokio_stream::wrappers::ReceiverStream::new(receiver).boxed(),
        )
    }

    #[tokio::test]
    async fn test_produces_final_tick() {
        let mut key_hashes = SortedKeyHashMap::new();
        let (sender, input_stream) = input_stream();
        let (operation_sender, mut operation_stream) = operation_stream();
        let mut operation = default_final_tick_operation(input_stream);

        let start = NaiveDateTime::from_timestamp_opt(1, 1).unwrap();
        let end = NaiveDateTime::from_timestamp_opt(1, 10).unwrap();
        let batch1 = Batch::batch_from_dates(start, end);
        let to_add: &UInt64Array = downcast_primitive_array(batch1.column(2).as_ref()).unwrap();
        key_hashes.get_or_update_indices(to_add).unwrap();
        sender.send(batch1.clone()).await.unwrap();

        tokio::spawn(async move {
            operation.execute(operation_sender).await.unwrap();
        });

        if let Some(input) = operation_stream.next().await {
            validate_non_tick_batch(&batch1, input);
        } else {
            panic!("expected batch");
        };

        tokio::spawn(async move {
            drop(sender);
        })
        .await
        .unwrap();

        if let Some(input) = operation_stream.next().await {
            let num_rows = input.time.len();
            validate_tick_batch(end.timestamp_nanos() + 1, &mut key_hashes, input, num_rows)
        } else {
            panic!("expected batch")
        }
    }

    fn validate_non_tick_batch(expected: &Batch, output: InputBatch) {
        assert_eq!(output.time.len(), 0);
        assert_eq!(expected.lower_bound, output.lower_bound);
        assert_eq!(expected.upper_bound, output.upper_bound);
    }

    fn validate_tick_batch(
        tick: i64,
        keys: &mut SortedKeyHashMap,
        tick_batch: InputBatch,
        num_rows: usize,
    ) {
        let times =
            TimestampNanosecondArray::from_iter_values(std::iter::repeat(tick).take(num_rows));
        let subsort = UInt64Array::from_iter_values(std::iter::repeat(u64::MAX).take(num_rows));
        let keys: UInt64Array = keys.keys().take(num_rows).collect();
        assert_eq!(tick_batch.time.as_ref(), &times);
        assert_eq!(tick_batch.subsort.as_ref(), &subsort);
        assert_eq!(tick_batch.key_hash.as_ref(), &keys);

        let ticks = true_column(num_rows);
        assert_eq!(tick_batch.input_columns[0].as_ref(), &ticks)
    }

    mod incremental {
        use super::*;

        fn compute_store() -> ComputeStore {
            let tempdir = tempfile::Builder::new().tempdir().unwrap();
            ComputeStore::try_new_from_path(tempdir.path()).unwrap()
        }

        #[test]
        fn test_basic_store_restore() {
            let store = compute_store();
            let current1 = 3700;
            let keys1 = SortedKeyHashMap::new();
            let original_operation = FinalTickOperation {
                input_stream: Box::pin(futures::stream::iter(vec![])),
                current_time: current1,
                key_hashes: keys1.clone(),
                tick_at: None,
            };
            original_operation.store_to(0, &store).unwrap();

            let mut restored_operation = FinalTickOperation {
                input_stream: Box::pin(futures::stream::iter(vec![])),
                current_time: 0,
                key_hashes: SortedKeyHashMap::new(),
                tick_at: None,
            };
            restored_operation.restore_from(0, &store).unwrap();

            let FinalTickOperation {
                current_time,
                key_hashes,
                ..
            } = restored_operation;

            assert_eq!(current_time, current1);
            assert_eq!(key_hashes, keys1);
        }
    }
}
