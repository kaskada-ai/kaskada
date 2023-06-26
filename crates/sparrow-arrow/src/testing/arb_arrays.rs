use arrow_array::{TimestampNanosecondArray, UInt64Array};
use proptest::prelude::*;

use crate::Batch;

proptest::prop_compose! {
    /// Generate arbitrary sequences of key triples.
    pub fn sorted_time_key_hash(len: impl Into<prop::collection::SizeRange>,
        time_advance_probability: f64)(steps in prop::collection::vec(
        (prop::bool::weighted(time_advance_probability), 0..3u64), len)) ->
        (TimestampNanosecondArray, UInt64Array) {
            let (times, key_hashes): (Vec<_>, Vec<_>) = steps.into_iter().scan((0i64, 0u64), |s, (time_step, hash_step)| {
                if time_step {
                    s.0 += 1;
                    s.1 = 0;
                }
                s.1 += hash_step;

                Some(*s)
            }).unzip();

            let time = TimestampNanosecondArray::from(times);
            let key_hash = UInt64Array::from(key_hashes);


            (time, key_hash)
    }
}

pub fn arb_batch(len: impl Into<proptest::sample::SizeRange>) -> impl Strategy<Value = Batch> {
    sorted_time_key_hash(len, 0.3).prop_map(|(time, key_hash)| {
        let max_time = time.values()[time.len() - 1];
        Batch::minimal_from(time, key_hash, max_time)
    })
}
