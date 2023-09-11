#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

//! Interface and utilities for state management.

mod error;
mod keys;
mod primitive_state;
mod state_backend;
mod state_key;
mod state_store;
mod state_token;

pub use error::*;
pub use keys::*;
pub use primitive_state::*;
pub use state_backend::*;
pub use state_key::*;
pub use state_store::*;
pub use state_token::*;
