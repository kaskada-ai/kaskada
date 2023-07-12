use crate::execute::progress_reporter::ProgressUpdate;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use error_stack::Result;
use futures::stream::BoxStream;
use sparrow_api::kaskada::v1alpha::RedisDestination;

#[derive(derive_more::Display, Debug)]
#[display(fmt = "Redis Destination is unsupported")]
pub struct Error;

impl error_stack::Context for Error {}

pub(super) async fn write(
    _redis: RedisDestination,
    _schema: SchemaRef,
    _progress_updates_tx: tokio::sync::mpsc::Sender<ProgressUpdate>,
    _batches: BoxStream<'static, RecordBatch>,
) -> Result<(), Error> {
    error_stack::bail!(Error);
}
