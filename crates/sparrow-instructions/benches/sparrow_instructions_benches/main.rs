#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

use criterion::{criterion_group, criterion_main};
mod primitive_accum_token_bench;

criterion_group!(benches, primitive_accum_token_bench::benches);
criterion_main!(benches);
