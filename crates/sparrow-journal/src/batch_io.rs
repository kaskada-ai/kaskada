use std::collections::HashMap;
use std::io::Write;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::Buffer;
use arrow_ipc::writer::{DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::SchemaRef;
use error_stack::{IntoReport, ResultExt};
use itertools::Itertools;
use sparrow_batch::{Batch, RowTime};

use crate::error::Error;

/// Write batches to a byte array.
///
/// This uses arrow_ipc::writer::IpcDataGenerator directly. This allows it to avoid
/// copying buffers between the underlying encoded buffer and the stream being written.
pub(super) struct BatchEncoder {
    /// Track the dictionary entries in the current segment.
    dictionary_tracker: DictionaryTracker,
    /// Generator for encoded batches.
    gen: IpcDataGenerator,
    write_options: IpcWriteOptions,
    schema: SchemaRef,
}

impl std::fmt::Debug for BatchEncoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchEncoder")
            .field("gen", &self.gen)
            .field("write_options", &self.write_options)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

pub(super) struct Encoded {
    up_to_time: RowTime,
    dictionary_batches: Vec<EncodedData>,
    batch: Option<EncodedData>,
}

impl Encoded {
    pub(super) fn write_to_journal(
        self,
        wal: &mut okaywal::WriteAheadLog,
    ) -> std::io::Result<okaywal::EntryId> {
        let mut entry = wal.begin_entry()?;
        entry.write_chunk(&self.up_to_time.bytes())?;
        for dictionary_batch in self.dictionary_batches {
            entry.write_chunk(&dictionary_batch.ipc_message)?;
            entry.write_chunk(&dictionary_batch.arrow_data)?;
        }
        if let Some(batch) = self.batch {
            entry.write_chunk(&batch.ipc_message)?;
            entry.write_chunk(&batch.arrow_data)?;
        }
        entry.commit()
    }
}

impl BatchEncoder {
    pub fn new(schema: SchemaRef) -> error_stack::Result<Self, Error> {
        Ok(Self {
            dictionary_tracker: DictionaryTracker::new(false),
            gen: IpcDataGenerator {},
            write_options: IpcWriteOptions::default(),
            schema,
        })
    }

    pub fn encode(&mut self, batch: &Batch) -> error_stack::Result<Encoded, Error> {
        // TODO: Split batches to fit in a certain size.
        let up_to_time = batch.up_to_time;

        // Create a record batch for the given batch.
        if let Some(batch) = &batch.data {
            let batch = RecordBatch::try_new(
                self.schema.clone(),
                vec![
                    batch.time.clone(),
                    batch.subsort.clone(),
                    batch.key_hash.clone(),
                    batch.data.clone(),
                ],
            )
            .into_report()
            .change_context(Error::WriteBatch)?;
            let (dictionary_batches, batch) = self
                .gen
                .encoded_batch(&batch, &mut self.dictionary_tracker, &self.write_options)
                .into_report()
                .change_context(Error::WriteBatch)?;
            Ok(Encoded {
                up_to_time,
                dictionary_batches,
                batch: Some(batch),
            })
        } else {
            Ok(Encoded {
                up_to_time,
                dictionary_batches: vec![],
                batch: None,
            })
        }
    }
}

#[derive(Debug)]
pub(super) struct BatchDecoder {
    schema: SchemaRef,

    /// Optional dictionaries for each schema field.
    ///
    /// Dictionaries may be appended to in the streaming format.
    dictionaries_by_id: HashMap<i64, ArrayRef>,
}

impl BatchDecoder {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            dictionaries_by_id: HashMap::new(),
        }
    }

    /// Decode the batch from the given entry.
    ///
    /// If it was completely written, will return `Some(batch)`. If it wasn't completely
    /// written, will return `None`.
    pub fn decode(
        &mut self,
        entry: &mut okaywal::Entry<'_>,
    ) -> error_stack::Result<Option<Batch>, Error> {
        // Read the chunks.
        let Some(chunks) = entry
            .read_all_chunks()
            .into_report()
            .change_context(Error::ReadBatch)?
        else {
            return Ok(None);
        };

        let mut chunks = chunks.into_iter();

        let up_to_time = chunks.next().expect("at least one entry");
        let up_to_time = RowTime::from_bytes(up_to_time.try_into().expect("row time"));

        let mut chunk_pairs = chunks.tuples().peekable();
        while let Some((metadata, batch)) = chunk_pairs.next() {
            let message = decode_message(&metadata)?;
            error_stack::ensure!(
                message.bodyLength() as usize == batch.len(),
                Error::ReadBatch
            );

            let buf = Buffer::from(batch);

            if chunk_pairs.peek().is_some() {
                error_stack::ensure!(
                    message.header_type() == arrow_ipc::MessageHeader::DictionaryBatch,
                    Error::ReadBatch
                );

                let batch = message.header_as_dictionary_batch().unwrap();
                arrow_ipc::reader::read_dictionary(
                    &buf,
                    batch,
                    &self.schema,
                    &mut self.dictionaries_by_id,
                    &message.version(),
                )
                .into_report()
                .change_context(Error::ReadBatch)?;
            } else {
                error_stack::ensure!(
                    message.header_type() == arrow_ipc::MessageHeader::RecordBatch,
                    Error::ReadBatch
                );
                let batch = message.header_as_record_batch().unwrap();
                let batch = arrow_ipc::reader::read_record_batch(
                    &buf,
                    batch,
                    self.schema.clone(),
                    &self.dictionaries_by_id,
                    None,
                    &message.version(),
                )
                .into_report()
                .change_context(Error::ReadBatch)?;

                return Ok(Some(Batch::new_with_data(
                    batch.column(3).clone(),
                    batch.column(0).clone(),
                    batch.column(1).clone(),
                    batch.column(2).clone(),
                    up_to_time,
                )));
            }
        }

        // If we get here, we must not have had any batches above.
        Ok(Some(Batch::new_empty(up_to_time)))
    }
}

fn decode_message(buf: &[u8]) -> error_stack::Result<arrow_ipc::Message<'_>, Error> {
    arrow_ipc::root_as_message(buf)
        .map_err(|e| {
            // We'd like to conrert the flatbuffer error directly.
            // But, it only implements `std::error::Error` if the `std` feature
            // is enabled, which Arrow doesn't use.
            error_stack::report!(Error::ReadBatch).attach_printable(e)
        })
        .change_context(Error::ReadBatch)
}
