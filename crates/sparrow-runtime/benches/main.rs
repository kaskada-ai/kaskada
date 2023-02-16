#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr
)]

use criterion::{criterion_group, criterion_main};

mod binary_merge_bench;

criterion_group!(benches, binary_merge_bench::merge_benchmarks);
criterion_main!(benches);
