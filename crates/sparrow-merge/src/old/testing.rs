use std::fmt::Write;

use arrow_array::{Int64Array, RecordBatch, TimestampNanosecondArray, UInt64Array};
use proptest::prelude::*;

prop_compose! {
    /// Generate arbitrary sequences of key triples.
    ///
    /// We do this by generating a random sequence of steps (between 1 and 1000),
    /// and adding this to the previous key. We round keys to 0..19 and subsorts to 0..49.
    pub fn arb_key_triples(len: impl Into<prop::collection::SizeRange>)(steps in prop::collection::vec(1..1000u64, len)) ->
      (TimestampNanosecondArray, UInt64Array, UInt64Array) {
        let key_triples: Vec<_> = steps.iter().scan((0i64, 0u64, 0u64), |s, step| {
            s.2 += step;
            s.1 += s.2 / 20;
            s.0 += (s.1 / 50) as i64;
            s.2 %= 20;
            s.1 %= 50;
            Some(*s)
        }).collect();

        let time = TimestampNanosecondArray::from_iter_values(key_triples.iter().map(|(time, _, _)| *time));
        let subsort = UInt64Array::from_iter_values(key_triples.iter().map(|(_, subsort, _)| *subsort));
        let key_hash = UInt64Array::from_iter_values(key_triples.iter().map(|(_, _, key_hash)| *key_hash));

        (time, subsort, key_hash)
    }
}

/// Create an arbitrary i64 arary.
pub fn arb_i64_array(len: usize) -> impl Strategy<Value = Int64Array> {
    prop::collection::vec(prop::option::weighted(0.9, prop::num::i64::ANY), len)
        .prop_map(Int64Array::from)
}

/// Wrapper around the generated batches from an input.
///
/// The `Debug` format of record batches is extremely verbose, which
/// makes it impossible to read the generated test cases. This allows
/// us to create a custom debug format.
pub struct TestBatches(pub Vec<RecordBatch>);

/// Adapt a `std::fmt::Formatter` to `std::io::Write`.
struct DisplayWriter<'a, 'b>(&'a mut std::fmt::Formatter<'b>);

impl<'a, 'b> std::io::Write for DisplayWriter<'a, 'b> {
    fn write(&mut self, bytes: &[u8]) -> std::result::Result<usize, std::io::Error> {
        bytes
            .iter()
            .try_for_each(|c| self.0.write_char(*c as char))
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?;

        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
        Ok(())
    }
}

impl std::fmt::Debug for TestBatches {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Test Batches\n")?;
        f.write_str("------------\n")?;

        for batch in &self.0 {
            {
                let mut writer = arrow_csv::Writer::new(DisplayWriter(f));
                writer.write(batch).map_err(|_| std::fmt::Error)?;
            }

            f.write_char('\n')?;
        }

        f.write_str("------------\n")?;

        Ok(())
    }
}
