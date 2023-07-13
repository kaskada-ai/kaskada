use std::mem::size_of;

use criterion::{black_box, BenchmarkId, Criterion};

fn benchmark_serialize(vec: &[i64]) {
    let bytes = bincode::serialize(&vec).unwrap();
    let output = bincode::deserialize::<Vec<i64>>(&black_box(bytes)).unwrap();
    assert_eq!(output.len(), vec.len());
}

fn serialize_unsafe(vec: &[i64]) -> &[u8] {
    let len = std::mem::size_of_val(vec);
    // SAFETY: `ptr` has valid alignment (came from a vec) and contains `len` bytes.
    unsafe {
        let ptr = vec.as_ptr();
        let ptr = ptr as *const u8;
        std::slice::from_raw_parts(ptr, len)
    }
}

fn deserialize_unsafe(vec: &[u8]) -> &[i64] {
    assert_eq!(vec.len() % size_of::<i64>(), 0);
    let len = vec.len() / size_of::<i64>();
    // SAFETY: The assertion ensures that the number of bytes are correct.
    unsafe {
        let ptr = vec.as_ptr();
        let ptr = ptr as *const i64;
        std::slice::from_raw_parts(ptr, len)
    }
}

fn benchmark_serialize_unsafe(vec: &[i64]) {
    let bytes = serialize_unsafe(vec);
    let output = deserialize_unsafe(bytes);
    assert_eq!(output.len(), vec.len());
}

pub fn benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitive_accum_token_i64");

    group.bench_with_input(
        BenchmarkId::new("serialize_unsafe_i64_1000", 10000),
        &vec![0i64; 10000],
        |b, vec| b.iter(|| benchmark_serialize_unsafe(vec)),
    );

    group.bench_with_input(
        BenchmarkId::new("serialize_unsafe_i64_1000", 100000),
        &vec![0i64; 100000],
        |b, vec| b.iter(|| benchmark_serialize_unsafe(vec)),
    );

    group.bench_with_input(
        BenchmarkId::new("serialize_i64_1000", 10_000),
        &vec![0i64; 10_000],
        |b, vec| b.iter(|| benchmark_serialize(vec)),
    );

    group.bench_with_input(
        BenchmarkId::new("serialize_i64_1000", 100_000),
        &vec![0i64; 100_000],
        |b, vec| b.iter(|| benchmark_serialize(vec)),
    );
}
