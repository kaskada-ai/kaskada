/// Splits an `i64` into two `i32` parts.
/// This is useful for splitting the native representation of IntervalDayTime.
#[inline]
pub(crate) fn i64_to_two_i32(v: i64) -> (i32, i32) {
    // The low part
    let low: i32 = (v & 0xFFFFFFFF) as i32;

    // The high
    let high: i32 = (v >> 32) as i32;
    (high, low)
}
