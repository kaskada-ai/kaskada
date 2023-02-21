#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr
)]

// The Flight record protos
pub mod kaskada {
    pub mod sparrow {
        pub mod v1alpha {
            #![allow(clippy::derive_partial_eq_without_eq)] // stop clippy erroring on generated code from tonic (proto)
            include!(concat!(env!("OUT_DIR"), "/kaskada.sparrow.v1alpha.rs"));
        }
    }
}

pub const QFR_VERSION: u32 = 1;

mod activation;
mod activity;
mod factory;
pub mod io;
mod macros;
mod metrics;
mod recorder;
mod registration;
mod timer;

pub use activity::*;
pub use factory::*;
pub use macros::*;
pub use metrics::*;
pub use recorder::*;
pub use registration::*;
pub use timer::*;
