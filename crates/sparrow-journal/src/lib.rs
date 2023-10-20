#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

//! Journaling of Batches.
//!
//! This is used by sources to ensure batches are durably stored before
//! acknowledging new data.

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use futures::stream::BoxStream;
use futures::StreamExt;
use parking_lot::Mutex;
use sparrow_batch::{Batch, RowTime};
use std::sync::Arc;

use error_stack::{IntoReport, ResultExt};

use crate::batch_io::{BatchDecoder, BatchEncoder};
use crate::checkpoints::Checkpoints;
use crate::error::Error;

mod batch_io;
mod checkpoints;
mod error;

/// A journal of batches.
///
/// Batches should be written to the journal before being acknowledged to ensure
/// durability.
///
/// The journal is built on `okaywal`, which has an active segment used for
/// appending batches as they arrive. As the segment rolls over, they are
/// combined to create checkpoints.
pub struct BatchJournal {
    wal: okaywal::WriteAheadLog,
    batches: Arc<Batches>,
    encoder: BatchEncoder,
}

impl BatchJournal {
    /// Create (or recover) a journal from the given path.
    pub fn recover(
        directory: &std::path::Path,
        data_type: DataType,
    ) -> error_stack::Result<Self, Error> {
        let schema = data_type_to_schema(data_type);

        let checkpoint_dir = directory.join("checkpoints");
        let batches = Arc::new(Batches::try_new(schema.clone(), checkpoint_dir)?);
        let manager = BatchCheckpointer {
            batches: batches.clone(),
            decoder: BatchDecoder::new(schema.clone()),
        };
        let wal = okaywal::WriteAheadLog::recover(directory, manager)
            .into_report()
            .change_context(Error::Recovering)?;
        tracing::info!(
            "Recovered journal from {} with {} rows",
            directory.display(),
            batches.batches.lock().0.len()
        );

        let encoder = BatchEncoder::new(schema)?;
        Ok(Self {
            wal,
            batches,
            encoder,
        })
    }

    /// Add a batch to the journal.
    ///
    /// When this returns (successfully), the batch will have been durably
    /// committed to the journal.
    pub fn journal(&mut self, batch: Batch) -> error_stack::Result<(), Error> {
        let num_rows = batch.num_rows();
        let encoded = self.encoder.encode(&batch)?;
        let entry_id = encoded
            .write_to_journal(&mut self.wal)
            .into_report()
            .change_context(Error::WriteBatch)?;
        tracing::trace!("Journaled batch with {num_rows} rows to {entry_id:?}");

        self.batches.add_batch(batch)?;

        Ok(())
    }

    /// Return a stream over the current contents.
    pub fn stream_current(
        &self,
    ) -> error_stack::Result<BoxStream<'static, error_stack::Result<Batch, Error>>, Error> {
        self.batches.stream_current()
    }

    /// Return a stream over the current contents and subscribed to future additions.
    pub fn stream_subscribe(
        &self,
    ) -> error_stack::Result<BoxStream<'static, error_stack::Result<Batch, Error>>, Error> {
        self.batches.stream_subscribe()
    }

    #[cfg(test)]
    /// Cause a checkpoint to be taken.
    fn checkpoint(&self) -> error_stack::Result<(), Error> {
        self.wal
            .checkpoint_active()
            .into_report()
            .change_context(Error::Checkpointing)
    }
}

/// Convert a DataType to a SchemaRef.
fn data_type_to_schema(data: DataType) -> SchemaRef {
    let mut fields = Vec::with_capacity(4);
    fields.extend_from_slice(&[
        Field::new(
            "_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("_subsort", DataType::UInt64, false),
        Field::new("_key_hash", DataType::UInt64, false),
    ]);
    fields.push(Field::new("data", data, true));
    Arc::new(Schema::new(fields))
}

/// The current in-memory batches.
#[derive(Debug)]
struct Batches {
    batches: Mutex<(Vec<Batch>, RowTime)>,
    checkpoints: Mutex<Checkpoints>,
}

impl Batches {
    fn try_new(
        schema: SchemaRef,
        checkpoint_dir: std::path::PathBuf,
    ) -> error_stack::Result<Self, Error> {
        Ok(Self {
            batches: Mutex::new((vec![], RowTime::ZERO)),
            checkpoints: Mutex::new(Checkpoints::try_new(checkpoint_dir, schema)?),
        })
    }

    fn add_batch(&self, batch: Batch) -> error_stack::Result<(), Error> {
        let mut lock = self.batches.lock();
        batch.max_present_time();
        assert!(
            lock.1 <= batch.up_to_time,
            "Expected times to be increasing, but got {} and {}",
            lock.1,
            batch.up_to_time,
        );
        lock.1 = batch.up_to_time;
        lock.0.push(batch);

        Ok(())
    }

    fn checkpoint(&self) -> error_stack::Result<(), Error> {
        // Acquire the batch lock first, and hold it until we complete.
        // This ensures no batches can be acknowledged while we are in the
        // process of checkpointing.
        //
        // This may cause new inputs to pause while checkpoints are taken.
        // If this becomes a problem, we could look at a more sophisticated
        // strategy -- for instace, we could first take the batches and concatenate
        // them into a "pending-checkpoint", at which point we could release the lock.
        // Then, once the checkpoint completes, we could acquire the lock and drop the
        // pending checkpoint batch (since it is now available in the checkpoint).
        let mut lock = self.batches.lock();
        let batches = std::mem::take(&mut lock.0);
        self.checkpoints.lock().checkpoint(batches, lock.1)?;
        Ok(())
    }

    fn in_memory_stream(
        &self,
    ) -> error_stack::Result<BoxStream<'static, error_stack::Result<Batch, Error>>, Error> {
        // This clones the vec to create an owned iterator without needing to hold the lock.
        // Cloning the vec (and batches) shouldn't be too problematic given their expected
        // size. If they are, we could concatenate them into a single batch, updating the
        // in-memory set and then only cloning that one batch.
        Ok(futures::stream::iter(self.batches.lock().0.clone())
            .map(Ok)
            .boxed())
    }

    /// Return a stream over the current contents.
    fn stream_current(
        &self,
    ) -> error_stack::Result<BoxStream<'static, error_stack::Result<Batch, Error>>, Error> {
        // Since we don't currently accept late data, we don't need to merge with the checkpoints.
        // Instead, we can just chain at the end.
        let checkpoint_stream = self.checkpoints.lock().read_checkpoints()?;
        let in_memory_stream = self.in_memory_stream()?;
        Ok(checkpoint_stream.chain(in_memory_stream).boxed())
    }

    /// Return a stream over the current contents and subscribed to future additions.
    fn stream_subscribe(
        &self,
    ) -> error_stack::Result<BoxStream<'static, error_stack::Result<Batch, Error>>, Error> {
        // TODO: Merge with checkpoints, and subscribe to future changes.
        self.stream_current()
    }
}

#[derive(Debug)]
struct BatchCheckpointer {
    batches: Arc<Batches>,
    decoder: BatchDecoder,
}

impl okaywal::LogManager for BatchCheckpointer {
    fn recover(&mut self, entry: &mut okaywal::Entry<'_>) -> std::io::Result<()> {
        // DO NOT SUBMIT: Handle errors
        if let Some(batch) = self.decoder.decode(entry).unwrap() {
            self.batches.add_batch(batch).unwrap();
        }
        Ok(())
    }

    fn checkpoint_to(
        &mut self,
        _last_checkpointed_id: okaywal::EntryId,
        _checkpointed_entries: &mut okaywal::SegmentReader,
        _wal: &okaywal::WriteAheadLog,
    ) -> std::io::Result<()> {
        self.batches
            .checkpoint()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;
    use std::ops::DerefMut;

    use futures::{Stream, TryStreamExt};

    use super::*;

    struct JournalTester {
        tempdir: tempfile::TempDir,
        data_type: DataType,
    }

    impl JournalTester {
        fn new(data_type: DataType) -> Self {
            sparrow_testing::init_test_logging();

            let tempdir = tempfile::Builder::new()
                .prefix("journal")
                .tempdir()
                .unwrap();
            Self { tempdir, data_type }
        }

        fn journal(&mut self) -> GuardedJournal<'_> {
            let journal =
                BatchJournal::recover(self.tempdir.path(), self.data_type.clone()).unwrap();
            GuardedJournal {
                journal: Some(journal),
                phantom: PhantomData,
            }
        }
    }

    struct GuardedJournal<'a> {
        journal: Option<BatchJournal>,
        phantom: PhantomData<&'a ()>,
    }

    impl<'a> std::ops::Deref for GuardedJournal<'a> {
        type Target = BatchJournal;
        fn deref(&self) -> &Self::Target {
            self.journal.as_ref().expect("not yet dropped")
        }
    }

    impl<'a> DerefMut for GuardedJournal<'a> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.journal.as_mut().expect("not yet dropped")
        }
    }

    impl<'a> Drop for GuardedJournal<'a> {
        fn drop(&mut self) {
            // Make sure the journal shutsdown and checkpoints finish, etc.
            let journal = self.journal.take().expect("not yet dropped");
            journal.wal.shutdown().unwrap();
        }
    }

    fn batch_from_stream(stream: impl Stream<Item = error_stack::Result<Batch, Error>>) -> Batch {
        let batches: Vec<_> = futures::executor::block_on(stream.try_collect()).unwrap();
        let up_to_time = batches
            .last()
            .map(|last| last.up_to_time)
            .unwrap_or_default();
        Batch::concat(batches, up_to_time).unwrap()
    }

    #[test]
    fn test_journal_one() {
        let mut tester = JournalTester::new(DataType::Null);

        let batch = Batch::null_from(vec![0, 1, 2], vec![10, 11, 12], vec![2, 3, 4], 2);

        {
            let mut journal = tester.journal();
            journal.journal(batch.clone()).unwrap();

            assert_eq!(batch_from_stream(journal.stream_current().unwrap()), batch)
        }
    }

    #[test]
    fn test_journal_one_and_recover() {
        let mut tester = JournalTester::new(DataType::Null);

        let batch = Batch::null_from(vec![0, 1, 2], vec![10, 11, 12], vec![2, 3, 4], 2);

        {
            let mut journal = tester.journal();
            journal.journal(batch.clone()).unwrap();
        }

        {
            let recovered = tester.journal();
            assert_eq!(
                batch_from_stream(recovered.stream_current().unwrap()),
                batch
            );
        }
    }

    #[test]
    fn test_journal_two_and_recover() {
        let mut tester = JournalTester::new(DataType::Null);

        {
            let mut journal = tester.journal();
            journal
                .journal(Batch::null_from(vec![0, 1], vec![10, 11], vec![2, 3], 1))
                .unwrap();

            journal
                .journal(Batch::null_from(vec![2, 3], vec![10, 11], vec![2, 2], 3))
                .unwrap();
        }

        {
            let recovered = tester.journal();
            assert_eq!(
                batch_from_stream(recovered.stream_current().unwrap()),
                Batch::null_from(vec![0, 1, 2, 3], vec![10, 11, 10, 11], vec![2, 3, 2, 2], 3)
            );
        }
    }

    #[test]
    fn test_journal_and_checkpoint_and_recover() {
        let mut tester = JournalTester::new(DataType::Null);

        {
            let mut journal = tester.journal();
            journal
                .journal(Batch::null_from(vec![0, 1], vec![10, 11], vec![2, 3], 1))
                .unwrap();

            journal
                .journal(Batch::null_from(vec![2, 3], vec![10, 11], vec![2, 2], 3))
                .unwrap();
            journal.checkpoint().unwrap();
        }

        {
            let recovered = tester.journal();
            assert_eq!(
                batch_from_stream(recovered.stream_current().unwrap()),
                Batch::null_from(vec![0, 1, 2, 3], vec![10, 11, 10, 11], vec![2, 3, 2, 2], 3)
            );
        }
    }
}
