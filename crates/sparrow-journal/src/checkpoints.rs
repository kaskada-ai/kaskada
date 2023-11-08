use std::path::PathBuf;
use std::str::FromStr;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, TimestampNanosecondType};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use error_stack::{IntoReport, ResultExt};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use sparrow_batch::{Batch, RowTime};

use crate::error::Error;

/// Manages the checkpointed files.
///
/// We could use Parquet files for check-pointing but we instead use
/// Arrow IPC files, since we believe they should be easier to read.
///
/// TODO: Support checkpoints on S3.
#[derive(Debug)]
pub(super) struct Checkpoints {
    checkpoint_dir: PathBuf,
    checkpoints: Vec<CheckpointFile>,
    schema: SchemaRef,
}

#[derive(Debug)]
struct CheckpointFile {
    file_name: String,
    min_present_time: RowTime,
    max_present_time: RowTime,
}

/// Restore metadata of checkpoints by scanning entries in the directory.
///
/// TODO: In the future, we should store checkpoint metadata in the state DB
/// and do this recovery from that.
fn recover_checkpoints(
    checkpoint_path: &std::path::Path,
    schema: &SchemaRef,
) -> error_stack::Result<Vec<CheckpointFile>, Error> {
    let mut checkpoints = vec![];
    if checkpoint_path.exists() {
        error_stack::ensure!(
            checkpoint_path.is_dir(),
            Error::CheckpointDirNotDir(checkpoint_path.to_path_buf())
        );
    } else {
        std::fs::create_dir_all(checkpoint_path)
            .into_report()
            .change_context(Error::RecoveringCheckpoints)?;
    }

    for file in std::fs::read_dir(checkpoint_path)
        .into_report()
        .change_context(Error::RecoveringCheckpoints)?
    {
        let file = file
            .into_report()
            .change_context(Error::RecoveringCheckpoints)?;
        let file_type = file
            .file_type()
            .into_report()
            .change_context(Error::RecoveringCheckpoints)?;
        if !file_type.is_file() || file.path().extension() != Some("arrow".as_ref()) {
            continue;
        }

        let reader = std::fs::File::open(file.path())
            .into_report()
            .change_context(Error::RecoveringCheckpoints)?;
        let reader = arrow_ipc::reader::FileReader::try_new(reader, None)
            .into_report()
            .change_context(Error::RecoveringCheckpoints)?;
        error_stack::ensure!(&reader.schema() == schema, Error::RecoveringCheckpoints);

        let file_name = file
            .file_name()
            .into_string()
            .map_err(|_| Error::RecoveringCheckpoints)?;

        let min_present_time = reader
            .custom_metadata()
            .get("min_present_time")
            .ok_or(Error::RecoveringCheckpoints)?;
        let min_present_time =
            RowTime::from_str(min_present_time).change_context(Error::RecoveringCheckpoints)?;

        let max_present_time = reader
            .custom_metadata()
            .get("max_present_time")
            .ok_or(Error::RecoveringCheckpoints)?;
        let max_present_time =
            RowTime::from_str(max_present_time).change_context(Error::RecoveringCheckpoints)?;

        checkpoints.push(CheckpointFile {
            file_name,
            min_present_time,
            max_present_time,
        })
    }

    Ok(checkpoints)
}

impl Checkpoints {
    pub fn try_new(checkpoint_dir: PathBuf, schema: SchemaRef) -> error_stack::Result<Self, Error> {
        let checkpoints = recover_checkpoints(&checkpoint_dir, &schema)?;
        tracing::info!(
            "Recovered {} checkpoints from {}",
            checkpoints.len(),
            checkpoint_dir.display()
        );
        Ok(Self {
            checkpoint_dir,
            checkpoints,
            schema,
        })
    }

    /// Create a check-point from the given batches.
    ///
    /// This method assumes the batches are non-overlapping and ordered by time.
    pub fn checkpoint(
        &mut self,
        batches: Vec<Batch>,
        up_to_time: RowTime,
    ) -> error_stack::Result<(), Error> {
        let batch = Batch::concat(batches, up_to_time).change_context(Error::WriteCheckpoint)?;
        tracing::info!("Checkpointing {} rows", batch.num_rows());

        if let Some(batch) = batch.data {
            let file_name = format!("checkpoint_{}.arrow", self.checkpoints.len());
            let rows = batch.len();

            // Unfortunately, this writes the schema in each checkpoint file.
            // We could use arrow_ipc primitives directly (like we do in batch_io)
            // but we'd like to be able to write directly to the file, and it seems
            // easier to rely on Arrow for this. Given that this is only happening
            // per checkpoint, it should be OK (and maybe a useful sanity check).

            let writer = std::fs::File::create(self.checkpoint_dir.join(&file_name))
                .into_report()
                .change_context(Error::WriteCheckpoint)?;
            let mut writer = arrow_ipc::writer::FileWriter::try_new(writer, self.schema.as_ref())
                .into_report()
                .change_context(Error::WriteCheckpoint)?;
            writer.write_metadata("min_present_time", batch.min_present_time);
            writer.write_metadata("max_present_time", batch.max_present_time);
            let record_batch = RecordBatch::try_new(
                self.schema.clone(),
                vec![batch.time, batch.subsort, batch.key_hash, batch.data],
            )
            .into_report()
            .change_context(Error::WriteCheckpoint)?;

            writer
                .write(&record_batch)
                .into_report()
                .change_context(Error::WriteCheckpoint)?;

            writer
                .finish()
                .into_report()
                .change_context(Error::WriteCheckpoint)?;

            tracing::info!("Checkpointed {rows} jouranl rows to {file_name}");

            // Record the new checkpoint file.
            self.checkpoints.push(CheckpointFile {
                file_name,
                min_present_time: batch.min_present_time,
                max_present_time: batch.max_present_time,
            });
        }

        Ok(())
    }

    /// Create a merged read stream from all the checkpoint files.
    ///
    /// Currently, this assumes that the checkpoints are non-overlapping, which is true
    /// given the assumption of no late data.
    ///
    /// TODO: Support projection to checkpoint reads.
    pub fn read_checkpoints(
        &mut self,
    ) -> error_stack::Result<BoxStream<'static, error_stack::Result<Batch, Error>>, Error> {
        // Sort the checkpoints by min present time.
        // Since we don't allow any overlap currently, this means we can just
        // read the checkpoints in order.
        //
        // When we support overlap (and merging), this will still reflect the order
        // that files need to be opened, and can be used to limit the set of
        // active files at any point in time.

        self.checkpoints.sort_by_key(|c| c.min_present_time);

        // Clone the checkpoints to create a static iterator.
        let checkpoints: Vec<_> = self
            .checkpoints
            .iter()
            .map(|c| &c.file_name)
            .cloned()
            .collect();
        let checkpoint_dir = self.checkpoint_dir.clone();

        Ok(futures::stream::iter(checkpoints)
            .map(move |file_name| checkpoint_dir.join(file_name))
            .map(read_checkpoint_batches)
            .try_flatten()
            .boxed())
    }
}

fn read_checkpoint_batches(
    checkpoint_path: PathBuf,
) -> error_stack::Result<BoxStream<'static, error_stack::Result<Batch, Error>>, Error> {
    let reader = std::fs::File::open(checkpoint_path)
        .into_report()
        .change_context(Error::ReadingCheckpoint)?;
    let reader = arrow_ipc::reader::FileReader::try_new(reader, None)
        .into_report()
        .change_context(Error::ReadingCheckpoint)?;

    Ok(futures::stream::try_unfold(reader, |mut reader| async {
        // This isn't really async -- it actively reads the next batch.
        if let Some(batch) = reader.next() {
            let batch = batch
                .into_report()
                .change_context(Error::ReadingCheckpoint)?;

            let time = batch.column(0).clone();
            let subsort = batch.column(1).clone();
            let key_hash = batch.column(2).clone();
            let data = batch.column(3).clone();

            let up_to_time = *time
                .as_primitive::<TimestampNanosecondType>()
                .values()
                .last()
                .expect("non-empty");

            let batch =
                Batch::new_with_data(data, time, subsort, key_hash, RowTime::from(up_to_time));

            Ok(Some((batch, reader)))
        } else {
            Ok(None)
        }
    })
    .boxed())
}
