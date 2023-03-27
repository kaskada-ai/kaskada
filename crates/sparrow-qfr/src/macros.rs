/// Constant hashing based on https://casualhacks.net/blog/2020-05-31/compiletime-processing/
const fn const_hash(s: &str) -> u32 {
    let s = s.as_bytes();
    let mut hash = 3581u32;
    let mut i = 0usize;
    while i < s.len() {
        hash = hash.wrapping_mul(33).wrapping_add(s[i] as u32);
        i += 1;
    }
    hash
}

const fn const_splitmix(seed: u64) -> u64 {
    let next = seed.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = next;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

/// Create a random, deterministic unique ID from the file, line and column declaring it.
pub const fn const_unique_id(file: &str, line: u32, column: u32) -> u32 {
    let full_hash = const_splitmix(
        const_hash(file) as u64 ^ (line as u64).rotate_left(32) ^ (column as u64).rotate_left(48),
    );
    full_hash as u32
}

/// Macro to create a unique `u32` derived from the file name, line and column.
#[macro_export]
macro_rules! unique_id {
    () => {
        $crate::const_unique_id(file!(), line!(), column!())
    };
}
pub use unique_id;

/// Macro to create a gauge metric.
///
/// Gauge metrics report a value for each activity or at each point in time that
/// replaces the previously reported value. The number of output rows in the
/// current batch of processing or the number of entities currently in the
/// system.
///
/// ```rust
/// const NUM_OUTPUT_ROWS: Gauge<u64> = gauge!("num_output_rows");
/// ```
#[macro_export]
macro_rules! gauge {
    ($label:expr) => {
        $crate::Gauge::new($label, $crate::unique_id!())
    };
}
pub use gauge;

/// Macro to create a counter metric.
///
/// Counter metrics report a value that changes over time as deltas are
/// added.
///
/// ```rust
/// const NUM_ENTITIES: Counter<u64> = counter!("num_entities");
/// ```
#[macro_export]
macro_rules! counter {
    ($label:expr) => {
        $crate::Counter::new($label, $crate::unique_id!())
    };
}
pub use counter;

/// Macro to create an activity.
///
/// ```rust
/// const ROOT: Activity = activity!("merging");
/// const CHILD: Activity = activity!("gather", ROOT);
/// ```
#[macro_export]
macro_rules! activity {
    ($label:expr) => {
        $crate::Activity::new($label, $crate::unique_id!(), None)
    };
    ($label:expr, $parent:expr) => {
        $crate::Activity::new($label, $crate::unique_id!(), Some($parent.activity_id))
    };
}
pub use activity;

#[macro_export]
macro_rules! register {
    ($e1:expr) => {
        inventory::submit!($crate::Registration::from($e1));
    };
    ($e1:expr, $($es:expr),+) => {
        inventory::submit!($crate::Registration::from($e1));
        register! { $($es),+ }
    }
}
pub use register;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unique_id() {
        const ID_1: u32 = unique_id!();
        const ID_2: u32 = unique_id!();

        assert_ne!(ID_1, ID_2);
    }
}
