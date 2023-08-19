//! Information about the built-in functions for compilation.

mod aggregation;
mod collection;
mod comparison;
mod function;
mod general;
mod implementation;
mod json;
mod logical;
mod math;
mod pushdown;
mod registry;
mod string;
mod time;
mod time_domain_check;
mod window;

pub use function::*;
use implementation::*;
pub(crate) use pushdown::*;
pub use registry::*;
pub use time_domain_check::*;

/// Register all the functions available in the registry.
fn register_functions(registry: &mut Registry) {
    aggregation::register(registry);
    collection::register(registry);
    comparison::register(registry);
    general::register(registry);
    logical::register(registry);
    math::register(registry);
    json::register(registry);
    string::register(registry);
    time::register(registry);
    window::register(registry);
}
