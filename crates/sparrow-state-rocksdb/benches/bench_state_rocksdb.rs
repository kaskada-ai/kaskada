use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use sparrow_state::{Keys, PrimitiveState, PrimitiveToken, StateBackend, StateKey};
use sparrow_state_rocksdb::RocksdbStateBackend;

fn setup_entity_keys() -> Keys {
    let mut keys = Vec::new();
    for i in 0..100000 {
        let key = i % 400;
        keys.push(key);
        if key == 0 {
            keys.push(key);
        }
    }
    Keys::new(47, &keys)
}

fn rw_value(backend: &RocksdbStateBackend, keys: &Keys) {
    let token: PrimitiveToken<i64> = PrimitiveToken::new(16);
    let state = sparrow_state::read(&token, keys, backend).unwrap();

    let mut batch = backend.writer().unwrap();
    sparrow_state::write(&token, keys, batch.as_mut(), state).unwrap();

    // backend.clear_all().unwrap();
}

fn w_value(backend: &RocksdbStateBackend, keys: &Keys) {
    let token: PrimitiveToken<i64> = PrimitiveToken::new(16);
    let mut state: PrimitiveState<i64> = PrimitiveState::empty(keys);
    for key in &keys.key_indices {
        state.set(*key, (*key) as i64);
    }

    let mut batch = backend.writer().unwrap();
    sparrow_state::write(&token, keys, batch.as_mut(), state).unwrap();
    // backend.clear_all().unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let tempdir = tempfile::tempdir().unwrap();
    let state_backend = RocksdbStateBackend::open(tempdir.path()).unwrap();

    let input = setup_entity_keys();
    let mut group = c.benchmark_group("RocksDB");
    let group = group.throughput(criterion::Throughput::Elements(
        input.num_key_hashes() as u64,
        // input.num_rows() as u64,
    ));
    group.bench_with_input("rw_i64", &input, |b, keys| {
        b.iter(|| rw_value(&state_backend, keys))
    });
    group.bench_with_input("w_i64", &input, |b, keys| {
        b.iter(|| w_value(&state_backend, keys))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
