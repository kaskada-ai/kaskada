//! Materialization library for Kaskada.
//!
//! Responsible for defining the materialization constructs
//! and how to execute a materialization process.
//!
//! Materializations are long-running processes that continuously execute
//! queries and produce results to a destination.

mod materialize;

pub use materialize::*;
