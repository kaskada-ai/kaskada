use std::sync::Arc;

use arrow::array::{TimestampNanosecondArray, UInt64Array};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use sparrow_core::TableSchema;
use sparrow_runtime::merge::{binary_merge, BinaryMergeInput};

fn rand_keys(schema: &TableSchema, size: usize, min_step: usize, max_step: usize) -> RecordBatch {
    // This is a bit tricky because we want to (a) generate increasing sequences of
    // keys, (b) without duplicates while (c) still using all parts of the key.

    let mut time_builder = TimestampNanosecondArray::builder(size);
    let mut subsort_builder = UInt64Array::builder(size);
    let mut key_hash_builderd = UInt64Array::builder(size);

    let mut time = 0i64;
    let mut subsort = 0u64;
    let mut key_hash = 0u64;

    let mut rng = StdRng::seed_from_u64(42);

    for _ in 0..size {
        // Figure out which of the 3 fields we're going to increase.
        // 60% increase occurrence, 30% increase arrival, 10% increase entity.
        let increase = rng.gen_range(0..=10);
        if increase < 6 {
            time += rng.gen_range(min_step as i64..max_step as i64);
            subsort = rng.gen_range(min_step as u64..max_step as u64);
            key_hash = rng.gen_range(min_step as u64..max_step as u64);
        } else if increase < 9 {
            subsort += rng.gen_range(min_step as u64..max_step as u64);
            key_hash = rng.gen_range(min_step as u64..max_step as u64);
        } else {
            key_hash += rng.gen_range(min_step as u64..max_step as u64);
        }

        time_builder.append_value(time);
        subsort_builder.append_value(subsort);
        key_hash_builderd.append_value(key_hash);
    }

    RecordBatch::try_new(
        schema.schema_ref().clone(),
        vec![
            Arc::new(time_builder.finish()),
            Arc::new(subsort_builder.finish()),
            Arc::new(key_hash_builderd.finish()),
        ],
    )
    .unwrap()
}

fn rand_sparse(schema: &TableSchema, size: usize) -> RecordBatch {
    rand_keys(schema, size, 200, 250)
}

fn rand_dense(schema: &TableSchema, size: usize) -> RecordBatch {
    rand_keys(schema, size, 1, 5)
}

fn bench_merge(batch_a: &RecordBatch, batch_b: &RecordBatch) {
    let input_a =
        BinaryMergeInput::from_array_refs(batch_a.column(0), batch_a.column(1), batch_a.column(2))
            .unwrap();
    let input_b =
        BinaryMergeInput::from_array_refs(batch_b.column(0), batch_b.column(1), batch_b.column(2))
            .unwrap();
    criterion::black_box(binary_merge(input_a, input_b).unwrap());
}

pub fn merge_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_merge");

    let schema = TableSchema::try_from_data_schema(&Schema::new(vec![])).unwrap();
    let dense_a = rand_dense(&schema, 2000000);
    let dense_b = rand_dense(&schema, 2000000);
    let sparse_a = rand_sparse(&schema, 1000000);
    let sparse_b = rand_sparse(&schema, 1000000);

    group.throughput(Throughput::Elements(2000000));
    group.bench_function(BenchmarkId::from_parameter("two_sparse"), |b| {
        b.iter(|| bench_merge(&sparse_a, &sparse_b))
    });

    group.throughput(Throughput::Elements(3000000));
    group.bench_function(BenchmarkId::from_parameter("one_dense_one_sparse"), |b| {
        b.iter(|| bench_merge(&dense_a, &sparse_b))
    });

    group.throughput(Throughput::Elements(4000000));
    group.bench_function(BenchmarkId::from_parameter("two_dense"), |b| {
        b.iter(|| bench_merge(&dense_a, &dense_b))
    });
}
