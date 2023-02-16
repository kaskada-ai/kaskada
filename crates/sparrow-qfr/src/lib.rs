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

mod io;
mod recorder;
mod timer;

pub use io::*;
pub use recorder::*;
pub use timer::*;
