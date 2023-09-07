use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use sparrow_state::{StateBackend, StateKey};
use sparrow_state_sled::SledStateBackend;

fn setup_entity_keys() -> Vec<u64> {
    let mut keys = Vec::new();
    for i in 0..1000 {
        let key = i % 4;
        keys.push(key);
        if key == 0 {
            keys.push(key);
        }
    }
    keys
}

fn rw_value(backend: &SledStateBackend, keys: &[u64]) -> u64 {
    let mut sum = 0;
    let mut state_key = StateKey {
        key_hash: 0,
        operation_id: 1,
        step_id: 2,
    };
    for key in keys {
        state_key.key_hash = *key;
        if let Some(value) = backend.value_get::<u64>(&state_key).unwrap() {
            sum += value;
            let new_value = value * 2;
            backend.value_put(&state_key, Some(&new_value)).unwrap();
        } else {
            backend.value_put(&state_key, Some(&1u64)).unwrap();
        }
    }

    backend.clear_all().unwrap();
    sum
}

fn w_value(backend: &SledStateBackend, keys: &[u64]) {
    let mut sum = 0;
    let mut state_key = StateKey {
        key_hash: 0,
        operation_id: 1,
        step_id: 2,
    };
    for key in keys {
        state_key.key_hash = *key;
        sum += *key;
        backend.value_put(&state_key, Some(&sum)).unwrap();
    }

    backend.clear_all().unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let tempdir = tempfile::tempdir().unwrap();
    let state_backend = SledStateBackend::open(tempdir.path()).unwrap();

    let input = setup_entity_keys();
    let mut group = c.benchmark_group("Sled");
    let group = group.throughput(criterion::Throughput::Elements(input.len() as u64));
    group.bench_with_input("rw_i64", &input, |b, keys| {
        b.iter(|| rw_value(&state_backend, keys))
    });
    group.bench_with_input("w_i64", &input, |b, keys| {
        b.iter(|| w_value(&state_backend, keys))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
