use chrono::NaiveDateTime;

use crate::kaskada::v1alpha::PreparedFile;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "missing field '{field}")]
    MissingField { field: &'static str },
    #[display(fmt = "invalid timestamp {timestamp:?}")]
    InvalidTimestamp {
        timestamp: prost_wkt_types::Timestamp,
    },
}

impl error_stack::Context for Error {}

impl From<&PreparedFile> for PreparedFile {
    fn from(file: &PreparedFile) -> Self {
        file.clone()
    }
}

impl PreparedFile {
    pub fn min_event_time(&self) -> error_stack::Result<NaiveDateTime, Error> {
        let Some(timestamp) = self.min_event_time.as_ref() else {
            error_stack::bail!(Error::MissingField {
                field: "min_event_time"
            })
        };
        NaiveDateTime::from_timestamp_opt(timestamp.seconds, timestamp.nanos as u32).ok_or_else(
            || {
                error_stack::report!(Error::InvalidTimestamp {
                    timestamp: timestamp.clone()
                })
            },
        )
    }

    pub fn max_event_time(&self) -> error_stack::Result<NaiveDateTime, Error> {
        let Some(timestamp) = self.max_event_time.as_ref() else {
            error_stack::bail!(Error::MissingField {
                field: "max_event_time"
            })
        };
        NaiveDateTime::from_timestamp_opt(timestamp.seconds, timestamp.nanos as u32).ok_or_else(
            || {
                error_stack::report!(Error::InvalidTimestamp {
                    timestamp: timestamp.clone()
                })
            },
        )
    }
}
