#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

mod fixture;
mod fixtures;
pub(crate) use fixture::*;

mod prepare_tests;

// Basic unit tests for operators.
mod aggregation_tests;
mod basic_error_tests;
mod cast_tests;
mod coalesce_tests;
mod collection_tests;
mod comparison_tests;
mod decoration_tests;
mod entity_key_output_tests;
mod equality_tests;
mod formula_tests;
mod general_tests;
mod json_tests;
mod logical_tests;
mod lookup_tests;
mod math_tests;
mod multiple_tables;
mod record_tests;
mod resumeable_tests;
mod shift_tests;
mod string_tests;
mod tick_tests;
mod time_tests;
mod when_tests;
mod windowed_aggregation_tests;
mod with_key_tests;

// Tests for Parquet functionality.
mod parquet_tests;

// Tests for specific notebooks.

mod notebooks;
