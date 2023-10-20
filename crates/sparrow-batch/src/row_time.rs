use std::str::FromStr;

use arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
use arrow_array::temporal_conversions::timestamp_ns_to_datetime;
use error_stack::{IntoReport, ResultExt};

/// Wrapper around the time of a row.
///
/// This is used to allow for future changes to the representation
/// of time in a single place.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Default)]
#[repr(transparent)]
pub struct RowTime(i64);

impl std::fmt::Display for RowTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let time = timestamp_ns_to_datetime(self.0).expect("expected date time");
        write!(f, "{time}")
    }
}

impl RowTime {
    pub const ZERO: Self = Self::from_timestamp_ns(0);
    pub const MAX: Self = Self::from_timestamp_ns(i64::MAX);

    pub const fn from_timestamp_ns(timestamp: i64) -> Self {
        Self(timestamp)
    }

    pub fn pred(&self) -> Self {
        Self(self.0 - 1)
    }

    pub fn bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    pub fn from_bytes(bytes: [u8; 8]) -> Self {
        let time = i64::from_be_bytes(bytes);
        Self(time)
    }
}

impl From<RowTime> for i64 {
    fn from(val: RowTime) -> Self {
        val.0
    }
}

impl From<i64> for RowTime {
    fn from(value: i64) -> Self {
        RowTime(value)
    }
}

impl From<RowTime> for String {
    fn from(val: RowTime) -> Self {
        format!("{val}")
    }
}

#[derive(derive_more::Display, Debug)]
#[display(fmt = "invalid row time: {_0}")]
pub struct ParseError(String);

impl error_stack::Context for ParseError {}

impl FromStr for RowTime {
    type Err = error_stack::Report<ParseError>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        string_to_timestamp_nanos(s)
            .into_report()
            .change_context_lazy(|| ParseError(s.to_string()))
            .map(Self)
    }
}
