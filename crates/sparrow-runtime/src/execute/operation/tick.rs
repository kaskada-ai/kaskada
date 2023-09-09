use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Context;
use arrow::array::{
    Array, ArrayRef, BooleanArray, TimestampNanosecondArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Schema, SchemaRef, TimestampNanosecondType,
};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use futures::StreamExt;

use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use itertools::{izip, Itertools};
use serde::{Deserialize, Serialize};
use sparrow_api::kaskada::v1alpha::operation_plan;
use sparrow_api::kaskada::v1alpha::operation_plan::tick_operation::TickBehavior;
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_core::KeyTriple;
use sparrow_instructions::{ComputeStore, GroupingIndices, StoreKey};
use static_init::dynamic;

use super::expression_executor::InputColumn;
use super::sorted_key_hash_map::SortedKeyHashMap;
use super::tick_producer::*;
use super::{BoxedOperation, Operation};
use crate::execute::operation::InputBatch;
use crate::execute::Error;
use crate::Batch;

#[static_init::dynamic]
static TICK_SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
    Field::new("_time", TimestampNanosecondType::DATA_TYPE, false),
    Field::new("_subsort", DataType::UInt64, false),
    Field::new("_key_hash", DataType::UInt64, false),
    Field::new("_tick", DataType::Boolean, false),
]));

/// Max number of rows a tick batch produces at once.
const MAX_TICK_ROWS: usize = 100_000;

/// Holds state necessary to produce batches with ticks.
///
/// This operation is responsible for producing ticks at certain
/// times, configured by the `behavior`. It reads input batches from
/// the `input_stream`, then decides whether to produce a data batch or
/// a tick batch depending on when the next tick is relative to the input
/// batch's times.
pub(super) struct TickOperation {
    /// Stream of input batches
    input_stream: Pin<Box<dyn futures::Stream<Item = Batch> + Send>>,
    /// Produces the next tick time.
    ///
    /// `None` until the first batch has been received and an
    /// initial start time is determined.
    tick_iter: Option<TickIter>,
    /// The next tick time in nanoseconds from epoch.
    next_tick: NaiveDateTime,
    /// The last input timestamp (nanos) seen by the operation.
    current_time: i64,
    /// The sorted key hashes seen by this operation up to a point in time.
    key_hashes: SortedKeyHashMap,
    /// Configures when to tick at.
    behavior: TickBehavior,
}

impl std::fmt::Debug for TickOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TickOperation")
            .field("next_tick", &self.next_tick)
            .field("current_time", &self.current_time)
            .field("key_hashes", &format!("{} entries", self.key_hashes.len()))
            .field("behavior", &self.behavior)
            .finish_non_exhaustive()
    }
}

#[derive(Serialize, Deserialize)]
struct TickOperationState {
    next_tick: NaiveDateTime,
    current_time: i64,
}

#[async_trait]
impl Operation for TickOperation {
    fn restore_from(
        &mut self,
        operation_index: u8,
        compute_store: &ComputeStore,
    ) -> anyhow::Result<()> {
        self.key_hashes
            .restore_from(operation_index, compute_store)?;

        let state = compute_store
            .get(&StoreKey::new_tick_state(operation_index))?
            .unwrap_or({
                TickOperationState {
                    next_tick: NaiveDateTime::from_timestamp_opt(0, 0)
                        .expect("zero should be valid"),
                    current_time: 0,
                }
            });
        self.current_time = state.current_time;
        self.next_tick = state.next_tick;

        let producer: Box<dyn TickProducer> = match self.behavior {
            TickBehavior::Minutely => Box::new(MinutelyTickProducer),
            TickBehavior::Hourly => Box::new(HourlyTickProducer),
            TickBehavior::Daily => Box::new(DailyTickProducer),
            TickBehavior::Monthly => Box::new(MonthlyTickProducer),
            TickBehavior::Yearly => Box::new(YearlyTickProducer),
            TickBehavior::Finished => anyhow::bail!("Final ticks should use separate operation"),
            unknown => anyhow::bail!("Unknown tick behavior {:?}", unknown),
        };

        // If `next_tick` is 0, we can assume that it has not been initialized yet.
        if self.next_tick.timestamp_nanos() == 0 {
            self.tick_iter = None
        } else {
            self.tick_iter = Some(TickIter::try_new(
                vec![producer],
                self.next_tick,
                chrono::NaiveDateTime::MAX,
            )?);
            // Advance the tick iter past the current `next_tick` value.
            let cur_tick = self.next_tick;
            self.advance_tick()?;
            anyhow::ensure!(self.next_tick == cur_tick, "Advanced tick iter too far");
        };
        Ok(())
    }

    fn store_to(&self, operation_index: u8, compute_store: &ComputeStore) -> anyhow::Result<()> {
        self.key_hashes.store_to(operation_index, compute_store)?;

        let state = TickOperationState {
            next_tick: self.next_tick,
            current_time: self.current_time,
        };
        compute_store.put(&StoreKey::new_tick_state(operation_index), &state)?;

        Ok(())
    }

    async fn execute(
        &mut self,
        sender: tokio::sync::mpsc::Sender<InputBatch>,
    ) -> error_stack::Result<(), Error> {
        // Get the first batch to initialize the tick streams.
        //
        // Note that when restoring from state, the tick iter may be initialized
        // prior to receiving a batch. This check ensures we don't overwrite the
        // tick iter with incorrect bounds.
        if self.tick_iter.is_none() {
            if let Some(incoming) = self.input_stream.next().await {
                let mut tick_iter = initialize_tick_iter(&incoming, self.behavior)
                    .into_report()
                    .change_context(Error::internal())?;
                if let Some(next_tick) = tick_iter.next() {
                    self.next_tick = next_tick
                } else {
                    // There were no valid ticks between the start of the first batch
                    // and the end of time. Return `None` to indicate this operation is
                    // complete.
                    return Ok(());
                }

                self.tick_iter = Some(tick_iter);
                self.handle_incoming_batch(incoming, &sender)
                    .await
                    .into_report()
                    .change_context(Error::internal())?;
            };
        };

        // Process the remaining incoming batches.
        while let Some(incoming) = self.input_stream.next().await {
            self.handle_incoming_batch(incoming, &sender)
                .await
                .into_report()
                .change_context(Error::internal())?;
        }

        // Once we're done with incoming batches, see if we need to
        // process one last tick at the current upper bound.
        if self.next_tick.timestamp_nanos() == self.current_time && !self.key_hashes.is_empty() {
            send_tick_batch(self.next_tick, &self.key_hashes, &sender)
                .await
                .into_report()
                .change_context(Error::internal())?;
        }

        Ok(())
    }
}

impl TickOperation {
    /// Create the stream of input batches for a tick operation.
    pub(super) fn create(
        operation: operation_plan::TickOperation,
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
            tick_iter: None,
            next_tick: NaiveDateTime::from_timestamp_opt(0, 0).expect("zero time"),
            current_time: 0,
            key_hashes: SortedKeyHashMap::new(),
            behavior: operation.behavior(),
        }))
    }

    async fn handle_incoming_batch(
        &mut self,
        incoming: Batch,
        sender: &tokio::sync::mpsc::Sender<InputBatch>,
    ) -> anyhow::Result<()> {
        let upper_bound = incoming.upper_bound_as_date()?;

        #[allow(clippy::comparison_chain)]
        if upper_bound < self.next_tick {
            // Case 1: Incoming upper bound is less than the next tick time.

            // Update the known entities
            let key_hashes: &UInt64Array = downcast_primitive_array(incoming.column(2).as_ref())?;
            let _ = self.key_hashes.get_or_update_indices(key_hashes)?;

            // Update the upper bound
            self.current_time = incoming.upper_bound.time;

            // Emit an empty data batch to the upper bound time
            let input_batch = InputBatch::new_empty(
                TICK_SCHEMA.clone(),
                incoming.upper_bound,
                incoming.upper_bound,
            );

            sender.send(input_batch).await?;
            Ok(())
        } else if upper_bound == self.next_tick {
            // Case 2: Incoming upper bound equals the next tick time.

            // In order to avoid complex bounds logic and ticking up to
            // a specific upper bound key triple, we choose to output an
            // empty batch to the upper bound time, but with bounds set to the
            // minimum subsort and key values to ensure that when the tick batch
            // is produced, the bounds are non-decreasing.
            // There is a certain risk involved with arbitrarily setting bounds,
            // but 1. It will be a non-issue when bag semantics are introduced,
            // and 2. The `time` remains the same, which is what is used for determining
            // validity of inputs used for incremental queries.

            // Update the known entities
            let key_hashes: &UInt64Array = downcast_primitive_array(incoming.column(2).as_ref())?;
            self.key_hashes.extend(key_hashes.values().iter().copied());

            // Update the tick's current upper bound
            self.current_time = incoming.upper_bound.time;

            // Update the bounds
            let new_lower_bound = KeyTriple {
                time: incoming.upper_bound.time,
                subsort: u64::MIN,
                key_hash: u64::MIN,
            };
            let new_upper_bound = KeyTriple {
                time: incoming.upper_bound.time,
                subsort: u64::MIN,
                key_hash: u64::MIN,
            };

            let input_batch =
                InputBatch::new_empty(TICK_SCHEMA.clone(), new_lower_bound, new_upper_bound);
            sender.send(input_batch).await?;
            Ok(())
        } else {
            // Case 3: Incoming upper bound is greater than the next tick time.

            let mut discovered_entities = self.discover_entities(&incoming)?;
            // Update the upper bound
            self.current_time = incoming.upper_bound.time;

            // Loop until ticks are emitted for each time up to the upper bound.
            while upper_bound > self.next_tick {
                let tick_nanos = self.next_tick.timestamp_nanos();
                // Add entities discovered prior to tick time to known entity set
                let keys_before_tick = discovered_entities
                    .iter()
                    .filter(|(_, t)| **t <= tick_nanos)
                    .map(|(k, _)| *k);
                self.key_hashes.extend(keys_before_tick);

                discovered_entities.retain(|_, t| *t > tick_nanos);

                if !self.key_hashes.is_empty() {
                    send_tick_batch(self.next_tick, &self.key_hashes, sender).await?;
                }

                // Enumerated all known entities at this time. Advance the tick iter.
                self.advance_tick()?;
            }

            // Update with remaining entities
            self.key_hashes.extend(discovered_entities.keys().copied());

            // Send one more empty batch to ensure the upper bounds propagate correctly.
            // TODO: Improve batching of tick and data batches
            self.send_empty_batch(incoming, sender).await
        }
    }

    async fn send_empty_batch(
        &mut self,
        incoming: Batch,
        sender: &tokio::sync::mpsc::Sender<InputBatch>,
    ) -> anyhow::Result<()> {
        let upper_bound = incoming.upper_bound_as_date()?;
        if self.next_tick == upper_bound {
            // A subsequent batch may have entities at the tick time, so set the bounds to
            // the minimum subsort and time to ensure the next batch isn't out of order.
            let lower_bound = KeyTriple {
                time: incoming.upper_bound.time,
                subsort: u64::MIN,
                key_hash: u64::MIN,
            };
            let upper_bound = KeyTriple {
                time: incoming.upper_bound.time,
                subsort: u64::MIN,
                key_hash: u64::MIN,
            };

            let input_batch = InputBatch::new_empty(TICK_SCHEMA.clone(), lower_bound, upper_bound);
            sender.send(input_batch).await?;
        } else {
            anyhow::ensure!(self.next_tick > upper_bound);
            let input_batch = InputBatch::new_empty(
                TICK_SCHEMA.clone(),
                incoming.upper_bound,
                incoming.upper_bound,
            );
            sender.send(input_batch).await?;
        }
        Ok(())
    }

    fn expect_tick_iter(&mut self) -> anyhow::Result<&mut TickIter> {
        if let Some(tick_iter) = &mut self.tick_iter {
            Ok(tick_iter)
        } else {
            anyhow::bail!("Expected tick iter")
        }
    }

    /// Advances the tick iter forward once and sets the `next_tick`.
    fn advance_tick(&mut self) -> anyhow::Result<()> {
        self.next_tick = if let Some(tick) = self.expect_tick_iter()?.next() {
            tick
        } else {
            // TODO: Stop ticking once next tick is past the end of time.
            // However this should only happen if the input data is
            anyhow::bail!("Ticking past end of time not supported");
        };
        Ok(())
    }

    /// Returns the new entities discovered in the batch and their earliest
    /// times.
    ///
    /// Does not mutate the state's known entities.
    fn discover_entities(&self, batch: &Batch) -> anyhow::Result<BTreeMap<u64, i64>> {
        let mut discovered = BTreeMap::new();
        let keys: &UInt64Array = downcast_primitive_array(batch.column(2).as_ref())?;
        let time: &TimestampNanosecondArray = downcast_primitive_array(batch.column(0).as_ref())?;
        for (key_hash, time) in izip!(keys, time) {
            let key_hash = key_hash.context("non-null key")?;
            let time = time.context("non-null time")?;
            if !self.key_hashes.contains_key(key_hash) {
                discovered.entry(key_hash).or_insert(time);
            }
        }
        Ok(discovered)
    }
}

/// Initializes the tick iter using the bounds of the first incoming batch.
fn initialize_tick_iter(batch: &Batch, tick_behavior: TickBehavior) -> anyhow::Result<TickIter> {
    let producer: Box<dyn TickProducer> = match tick_behavior {
        TickBehavior::Minutely => Box::new(MinutelyTickProducer),
        TickBehavior::Hourly => Box::new(HourlyTickProducer),
        TickBehavior::Daily => Box::new(DailyTickProducer),
        TickBehavior::Monthly => Box::new(MonthlyTickProducer),
        TickBehavior::Yearly => Box::new(YearlyTickProducer),
        TickBehavior::Finished => anyhow::bail!("Final ticks should use separate operation"),
        unknown => anyhow::bail!("Unknown tick behavior {:?}", unknown),
    };

    // The tick iter is initialized from the first record batch's minimum time.
    // The tick iter will produces ticks from this time until the time of the last
    // input, so we can set the upper bound to the end of time.
    let ticks = TickIter::try_new(
        vec![producer],
        batch.lower_bound_as_date()?,
        chrono::NaiveDateTime::MAX,
    )?;
    Ok(ticks)
}

/// Send a tick batch at the given tick time, chunking if necessary.
async fn send_tick_batch(
    tick: NaiveDateTime,
    key_hashes: &SortedKeyHashMap,
    sender: &tokio::sync::mpsc::Sender<InputBatch>,
) -> anyhow::Result<()> {
    anyhow::ensure!(key_hashes.len() > 0);

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
                std::iter::repeat(Some(tick.timestamp_nanos())).take(len),
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
            time: tick.timestamp_nanos(),
            subsort: u64::MAX,
            key_hash: first_key,
        };
        let upper_bound = KeyTriple {
            time: tick.timestamp_nanos(),
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

        sender.send(input_batch).await?
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
    use arrow::array::{TimestampNanosecondArray, UInt32Array, UInt64Array};
    use arrow::record_batch::RecordBatch;
    use sparrow_api::kaskada::v1alpha::operation_input_ref::Column;
    use sparrow_api::kaskada::v1alpha::operation_plan::tick_operation::TickBehavior;
    use sparrow_api::kaskada::v1alpha::{self, data_type};
    use sparrow_api::kaskada::v1alpha::{
        expression_plan, operation_input_ref, operation_plan, ExpressionPlan, OperationInputRef,
        OperationPlan,
    };

    use super::*;
    use crate::execute::operation::testing::{batch_from_csv, run_operation};

    #[tokio::test]
    async fn test_tick() {
        let plan = OperationPlan {
            expressions: vec![ExpressionPlan {
                arguments: vec![],
                result_type: Some(v1alpha::DataType {
                    kind: Some(data_type::Kind::Primitive(
                        data_type::PrimitiveType::Bool as i32,
                    )),
                }),
                output: false,
                operator: Some(expression_plan::Operator::Input(OperationInputRef {
                    producing_operation: 1,
                    column: Some(Column::Tick(())),
                    input_column: 0,
                    interpolation: operation_input_ref::Interpolation::Null as i32,
                })),
            }],
            operator: Some(operation_plan::Operator::Tick(
                operation_plan::TickOperation {
                    input: 0,
                    behavior: (TickBehavior::Hourly as i32),
                },
            )),
        };

        let input = batch_from_csv(
            "
        _time,_subsort,_key_hash,n
        1970-01-01T00:00:02.000000000,0,1,10
        1970-01-01T00:00:03.000000000,0,1,2
        1970-01-01T02:58:20.000000000,0,2,
        1970-01-01T03:01:40.000000000,0,1,4",
            None,
        )
        .unwrap();
        insta::assert_snapshot!(run_operation(vec![input], plan).await.unwrap(), @r###"
        _time,_subsort,_key_hash
        1970-01-01T01:00:00.000000000,18446744073709551615,1
        1970-01-01T02:00:00.000000000,18446744073709551615,1
        1970-01-01T03:00:00.000000000,18446744073709551615,1
        1970-01-01T03:00:00.000000000,18446744073709551615,2
        "###)
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

    fn default_tick_operation(
        behavior: TickBehavior,
        input_stream: Pin<Box<dyn futures::Stream<Item = Batch> + Send>>,
    ) -> TickOperation {
        TickOperation {
            input_stream,
            tick_iter: None,
            next_tick: NaiveDateTime::from_timestamp_opt(0, 0).expect("zero time"),
            current_time: 0,
            key_hashes: SortedKeyHashMap::new(),
            behavior,
        }
    }

    fn update_key_hashes_up_to(key_hashes: &mut SortedKeyHashMap, batch: &Batch, up_to: i64) {
        let time: &TimestampNanosecondArray =
            downcast_primitive_array(batch.column(0).as_ref()).unwrap();
        let len = time.iter().filter(|t| t.unwrap() <= up_to).count();
        let take: Vec<u32> = (0..len as u32).collect();
        let take_indices = UInt32Array::from_iter_values(take);
        let to_add = arrow::compute::take(batch.column(2).as_ref(), &take_indices, None).unwrap();
        let to_add: &UInt64Array = downcast_primitive_array(to_add.as_ref()).unwrap();
        key_hashes.get_or_update_indices(to_add).unwrap();
    }

    #[tokio::test]
    async fn test_sequence_of_incoming_batches() {
        let mut key_hashes = SortedKeyHashMap::new();
        let (sender, input_stream) = input_stream();
        let (operation_sender, mut operation_stream) = operation_stream();
        let mut operation = default_tick_operation(TickBehavior::Hourly, input_stream);

        // 1. First incoming batch
        let first_tick = NaiveDateTime::from_timestamp_opt(3600, 0).unwrap();
        let first_start = NaiveDateTime::from_timestamp_opt(3599, 900_000_000).unwrap();
        let first_end = NaiveDateTime::from_timestamp_opt(3599, 900_000_010).unwrap();
        let first_batch = Batch::batch_from_dates(first_start, first_end);
        let to_add: &UInt64Array =
            downcast_primitive_array(first_batch.column(2).as_ref()).unwrap();
        key_hashes.get_or_update_indices(to_add).unwrap();
        sender.send(first_batch.clone()).await.unwrap();

        // 2. Second incoming batch
        let second_tick = NaiveDateTime::from_timestamp_opt(7200, 0).unwrap();
        let second_start = NaiveDateTime::from_timestamp_opt(7300, 1).unwrap();
        let second_end = NaiveDateTime::from_timestamp_opt(7300, 2).unwrap();
        let second_batch = Batch::batch_from_dates(second_start, second_end);
        // You can only update the entities up the time of the first tick.
        update_key_hashes_up_to(&mut key_hashes, &second_batch, first_tick.timestamp_nanos());
        sender.send(second_batch.clone()).await.unwrap();

        // Begin processing
        tokio::spawn(async move {
            operation.execute(operation_sender).await.unwrap();
        });

        // First input batch: empty data batch with bounds
        if let Some(input) = operation_stream.next().await {
            // Expect empty batch with appropriate time bounds.
            validate_non_tick_batch(&first_batch, input)
        } else {
            panic!("expected batch")
        }

        // Second input batch: tick at 3600
        if let Some(input) = operation_stream.next().await {
            let num_rows = key_hashes.len();
            validate_tick_batch(first_tick, &mut key_hashes, input, num_rows)
        } else {
            panic!("expected batch")
        }

        // Third input batch: tick at 7200
        update_key_hashes_up_to(
            &mut key_hashes,
            &second_batch,
            second_tick.timestamp_nanos(),
        );
        if let Some(input) = operation_stream.next().await {
            let num_rows = key_hashes.len();
            validate_tick_batch(second_tick, &mut key_hashes, input, num_rows)
        } else {
            panic!("expected batch")
        }
    }

    #[tokio::test]
    async fn test_tick_batch_enumerates_all_entities() {
        let (operation_sender, mut operation_stream) = operation_stream();
        let tick = NaiveDateTime::from_timestamp_opt(0, 5).unwrap();
        let mut key_hashes = SortedKeyHashMap::new();
        let keys = UInt64Array::from(vec![0, 1, 2, 3]);
        key_hashes.get_or_update_indices(&keys).unwrap();

        send_tick_batch(tick, &key_hashes, &operation_sender)
            .await
            .unwrap();

        if let Some(input) = operation_stream.next().await {
            let num_rows = key_hashes.len();
            validate_tick_batch(tick, &mut key_hashes, input, num_rows)
        } else {
            panic!("expected batch")
        }
    }

    #[tokio::test]
    async fn test_tick_batch_truncates_at_max_rows() {
        let (operation_sender, mut operation_stream) = operation_stream();
        let tick = NaiveDateTime::from_timestamp_opt(0, 5).unwrap();
        let mut key_hashes = SortedKeyHashMap::new();
        // Create a batch with more than MAX_TICK_ROWS
        let arr: Vec<u64> = (0..100_100).collect();
        let keys = UInt64Array::from(arr);
        key_hashes.get_or_update_indices(&keys).unwrap();

        send_tick_batch(tick, &key_hashes, &operation_sender)
            .await
            .unwrap();

        if let Some(input) = operation_stream.next().await {
            let num_rows = MAX_TICK_ROWS;
            validate_tick_batch(tick, &mut key_hashes, input, num_rows)
        } else {
            panic!("expected batch")
        }
        if let Some(input) = operation_stream.next().await {
            let num_rows = key_hashes.len() - MAX_TICK_ROWS;
            let keys: &UInt64Array = downcast_primitive_array(input.key_hash.as_ref()).unwrap();
            assert_eq!(num_rows, keys.len());
            assert_eq!(keys.value(0) as usize, MAX_TICK_ROWS);
            assert_eq!(keys.value(keys.len() - 1) as usize, key_hashes.len() - 1);
        } else {
            panic!("expected batch")
        }
    }

    #[tokio::test]
    async fn test_incoming_batch_spans_multiple_ticks() {
        let mut key_hashes = SortedKeyHashMap::new();
        let (sender, input_stream) = input_stream();
        let (operation_sender, mut operation_stream) = operation_stream();

        let mut operation = default_tick_operation(TickBehavior::Hourly, input_stream);
        let first_tick = NaiveDateTime::from_timestamp_opt(3600, 0).unwrap();
        let second_tick = NaiveDateTime::from_timestamp_opt(7200, 0).unwrap();
        let third_tick = NaiveDateTime::from_timestamp_opt(10800, 0).unwrap();
        let start = NaiveDateTime::from_timestamp_opt(3599, 0).unwrap();
        let end = NaiveDateTime::from_timestamp_opt(3599, 10).unwrap();
        let batch = Batch::batch_from_dates(start, end);
        // Create upper bound above the third tick.
        let new_upper_bound = KeyTriple {
            time: NaiveDateTime::from_timestamp_opt(10801, 0)
                .unwrap()
                .timestamp_nanos(),
            subsort: 0,
            key_hash: 0,
        };
        let batch =
            Batch::try_new_with_bounds(batch.data, batch.lower_bound, new_upper_bound).unwrap();
        let to_add: &UInt64Array = downcast_primitive_array(batch.column(2).as_ref()).unwrap();
        key_hashes.get_or_update_indices(to_add).unwrap();

        sender.send(batch.clone()).await.unwrap();
        tokio::spawn(async move {
            operation.execute(operation_sender).await.unwrap();
        });

        if let Some(input) = operation_stream.next().await {
            let num_rows = key_hashes.len();
            validate_tick_batch(first_tick, &mut key_hashes, input, num_rows)
        } else {
            panic!("expected batch")
        }

        if let Some(input) = operation_stream.next().await {
            let num_rows = key_hashes.len();
            validate_tick_batch(second_tick, &mut key_hashes, input, num_rows)
        } else {
            panic!("expected batch")
        }

        if let Some(input) = operation_stream.next().await {
            let num_rows = key_hashes.len();
            validate_tick_batch(third_tick, &mut key_hashes, input, num_rows)
        } else {
            panic!("expected batch")
        }
    }

    #[tokio::test]
    async fn test_incoming_batch_at_tick_time() {
        let (sender, input_stream) = input_stream();
        let (operation_sender, mut operation_stream) = operation_stream();

        let mut operation = default_tick_operation(TickBehavior::Hourly, input_stream);
        let start = NaiveDateTime::from_timestamp_opt(3600, 0).unwrap();
        let end = NaiveDateTime::from_timestamp_opt(3600, 0).unwrap();
        let batch = Batch::batch_from_dates(start, end);

        let expected_bound = KeyTriple {
            time: start.timestamp_nanos(),
            subsort: 0,
            key_hash: 0,
        };
        sender.send(batch.clone()).await.unwrap();
        tokio::spawn(async move {
            operation.execute(operation_sender).await.unwrap();
        });

        if let Some(input) = operation_stream.next().await {
            assert_eq!(input.time.len(), 0);
            assert_eq!(input.lower_bound, expected_bound);
            assert_eq!(input.upper_bound, expected_bound);
        } else {
            panic!("expected batch")
        }
    }

    #[tokio::test]
    async fn test_empty_batch_spanning_ticks() {
        let (sender, input_stream) = input_stream();
        let (operation_sender, mut operation_stream) = operation_stream();

        let mut operation = default_tick_operation(TickBehavior::Hourly, input_stream);

        let start = NaiveDateTime::from_timestamp_opt(3400, 0).unwrap();
        let end = NaiveDateTime::from_timestamp_opt(4200, 0).unwrap();

        let batch = Batch::batch_from_nanos(0, 0);
        let batch = RecordBatch::new_empty(batch.data.schema());
        let lower_bound = KeyTriple {
            time: start.timestamp_nanos(),
            subsort: 0,
            key_hash: 0,
        };
        let upper_bound = KeyTriple {
            time: end.timestamp_nanos(),
            subsort: 0,
            key_hash: 0,
        };
        let batch = Batch::try_new_with_bounds(batch, lower_bound, upper_bound).unwrap();

        sender.send(batch.clone()).await.unwrap();
        tokio::spawn(async move {
            operation.execute(operation_sender).await.unwrap();
        });

        if let Some(input) = operation_stream.next().await {
            validate_non_tick_batch(&batch, input);
        } else {
            panic!("expected batch")
        }
    }

    #[tokio::test]
    async fn test_adds_remaining_discovered_entities() {
        let mut key_hashes = SortedKeyHashMap::new();
        let (sender, input_stream) = input_stream();
        let (operation_sender, mut operation_stream) = operation_stream();

        let mut operation = default_tick_operation(TickBehavior::Hourly, input_stream);

        let times = [
            NaiveDateTime::from_timestamp_opt(1, 0).unwrap(),
            NaiveDateTime::from_timestamp_opt(2, 0).unwrap(),
            NaiveDateTime::from_timestamp_opt(3, 0).unwrap(),
            NaiveDateTime::from_timestamp_opt(3700, 0).unwrap(),
            NaiveDateTime::from_timestamp_opt(3800, 0).unwrap(),
        ];
        let times = times.iter().map(|i| i.timestamp_nanos());
        let time = TimestampNanosecondArray::from_iter_values(times);
        let subsort = UInt64Array::from_iter_values(vec![0, 0, 0, 0, 0]);
        let keys = UInt64Array::from_iter_values(vec![0, 1, 0, 3, 1]);
        key_hashes.get_or_update_indices(&keys).unwrap();
        let values = BooleanArray::from(vec![
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
        ]);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(time),
            Arc::new(subsort),
            Arc::new(keys),
            Arc::new(values),
        ];

        // Just using tick schema for convenience
        let batch = RecordBatch::try_new(TICK_SCHEMA.clone(), columns).unwrap();
        let batch = Batch::try_new_from_batch(batch).unwrap();

        // Send first batch with entities
        sender.send(batch.clone()).await.unwrap();

        // Send an empty batch so we get a second tick afterwards
        let batch = Batch::batch_from_nanos(0, 0);
        let batch = RecordBatch::new_empty(batch.data.schema());
        let lower_bound = KeyTriple {
            time: NaiveDateTime::from_timestamp_opt(3900, 0)
                .unwrap()
                .timestamp_nanos(),
            subsort: 0,
            key_hash: 0,
        };
        let upper_bound = KeyTriple {
            time: NaiveDateTime::from_timestamp_opt(7400, 0)
                .unwrap()
                .timestamp_nanos(),
            subsort: 0,
            key_hash: 0,
        };
        let batch = Batch::try_new_with_bounds(batch, lower_bound, upper_bound).unwrap();
        sender.send(batch.clone()).await.unwrap();

        tokio::spawn(async move {
            operation.execute(operation_sender).await.unwrap();
        });

        if let Some(input) = operation_stream.next().await {
            let first_tick = NaiveDateTime::from_timestamp_opt(3600, 0).unwrap();
            // Only the first two entities exist before the time of the first tick
            let num_rows = 2;
            validate_tick_batch(first_tick, &mut key_hashes, input, num_rows);
        } else {
            panic!("expected batch")
        }

        if let Some(input) = operation_stream.next().await {
            // We'll get an empty batch to the upper bound
            let upper = NaiveDateTime::from_timestamp_opt(3800, 0)
                .unwrap()
                .timestamp_nanos();
            assert_eq!(input.upper_bound.time, upper);
        }

        if let Some(input) = operation_stream.next().await {
            let second_tick = NaiveDateTime::from_timestamp_opt(7200, 0).unwrap();
            let num_rows = 3;
            validate_tick_batch(second_tick, &mut key_hashes, input, num_rows);
        } else {
            panic!("expected batch")
        }
    }

    fn validate_non_tick_batch(expected: &Batch, actual: InputBatch) {
        assert_eq!(actual.time.len(), 0);
        assert_eq!(expected.upper_bound, actual.lower_bound);
        assert_eq!(expected.upper_bound, actual.upper_bound);
        assert_eq!(1, actual.input_columns.len());
    }

    fn validate_tick_batch(
        tick: NaiveDateTime,
        keys: &mut SortedKeyHashMap,
        tick_batch: InputBatch,
        num_rows: usize,
    ) {
        let times = TimestampNanosecondArray::from_iter_values(
            std::iter::repeat(tick.timestamp_nanos()).take(num_rows),
        );
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

        fn tick_iter(lower_bound: NaiveDateTime) -> TickIter {
            let producer = Box::new(HourlyTickProducer);
            TickIter::try_new(vec![producer], lower_bound, chrono::NaiveDateTime::MAX).unwrap()
        }

        #[test]
        fn test_basic_store_restore() {
            let store = compute_store();
            let tick1 = NaiveDateTime::from_timestamp_opt(3600, 0).unwrap();
            let current1 = 3700;
            let keys1 = SortedKeyHashMap::new();
            let tick_iter = tick_iter(tick1);
            let original_operation = TickOperation {
                input_stream: Box::pin(futures::stream::iter(vec![])),
                tick_iter: Some(tick_iter),
                next_tick: tick1,
                current_time: current1,
                key_hashes: keys1.clone(),
                behavior: TickBehavior::Hourly,
            };
            original_operation.store_to(0, &store).unwrap();

            let mut restored_operation = TickOperation {
                input_stream: Box::pin(futures::stream::iter(vec![])),
                tick_iter: None,
                next_tick: NaiveDateTime::from_timestamp_opt(0, 0).unwrap(),
                current_time: 0,
                key_hashes: SortedKeyHashMap::new(),
                behavior: TickBehavior::Hourly,
            };
            restored_operation.restore_from(0, &store).unwrap();

            let TickOperation {
                next_tick,
                current_time: current_upper_bound,
                key_hashes,
                tick_iter,
                ..
            } = restored_operation;

            assert_eq!(next_tick, tick1);
            assert_eq!(current_upper_bound, current1);
            assert_eq!(key_hashes, keys1);
            assert_eq!(
                tick_iter.unwrap().next().unwrap(),
                NaiveDateTime::from_timestamp_opt(7200, 0).unwrap()
            )
        }
    }
}
