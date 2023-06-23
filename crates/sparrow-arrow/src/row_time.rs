use arrow_array::temporal_conversions::timestamp_ns_to_datetime;

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
}

impl From<RowTime> for i64 {
    fn from(val: RowTime) -> Self {
        val.0
    }
}
