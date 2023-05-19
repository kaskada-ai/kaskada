use std::sync::Arc;

use chrono::NaiveDateTime;

use crate::data_manager::DataHandle;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "failed to create input stream")]
    CreateStream,
    #[display(fmt = "failed to read next batch")]
    ReadNextBatch,
    #[allow(unused)]
    #[display(fmt = "unsupported: {_0}")]
    Unsupported(&'static str),
    #[display(fmt = "internal error: ")]
    Internal,
    #[display(
        fmt = "internal error: min next time ({min_next_time}) must be <= next lower bound {lower_bound}"
    )]
    MinNextGreaterThanNextLowerBound {
        min_next_time: i64,
        lower_bound: i64,
    },
    #[display(
        fmt = "internal error: max event time ({max_event_time}) must be > next upper bound {upper_bound}"
    )]
    MaxEventTimeLessThanNextUpperBound {
        max_event_time: i64,
        upper_bound: i64,
    },
    #[display(fmt = "failed to select necessary prepared files")]
    SelectPreparedFiles,
    #[display(fmt = "failed to queue download of necessary prepared files")]
    QueueFileDownloads,
    #[display(
        fmt = "unexpected file '{file:?}' in table '{table_name}' with data partially before the snapshot time {snapshot_time:?}"
    )]
    PartialOverlap {
        file: Arc<DataHandle>,
        table_name: String,
        snapshot_time: Option<NaiveDateTime>,
    },
    #[display(fmt = "failed to skip to minimum event")]
    SkippingToMinEvent,
    #[display(fmt = "failed to load table schema")]
    LoadTableSchema,
    #[display(fmt = "failed to determine projected schema")]
    DetermineProjectedSchema,
}

impl error_stack::Context for Error {}
