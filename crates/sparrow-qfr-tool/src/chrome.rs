//! Convert the query flight record to Chrome Tracing Events.
//!
//! The goal is to allow use of the Chrome `chrome://tracing` as a profile
//! viewer for the flight record.
//!
//! This uses the following structure:
//!
//! 1. Each pass is a separate `pid` (process ID).
//! 2. Each thread of execution gets a separate `tid`.
//! 3. Each pass gets a separate stack frame.
//! 4. Each part of executing the pass gets a separate frame in the pass.
//! 5. Each instruction in the pass gets a separate stack frame in the
//! execution.
//! 6. Each source gets a separate stack frame.
//!
//! <https://aras-p.info/blog/2017/01/23/Chrome-Tracing-as-Profiler-Frontend/>
//!
//! <https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU>

use std::path::PathBuf;

use error_stack::{IntoReport, ResultExt};
use sparrow_qfr::io::reader::FlightRecordReader;

use crate::chrome::conversion::Conversion;
use crate::error::Error;

mod conversion;

#[cfg(test)]
mod tests;

#[derive(Debug, clap::Args)]
#[command(version, rename_all = "kebab-case")]
pub(crate) struct ChromeCommand {
    /// Input file containing the Flight records.
    #[arg(long, value_name = "FILE")]
    pub input: PathBuf,

    /// Output file to write the output to.
    #[arg(long, value_name = "FILE")]
    pub output: PathBuf,
}

impl ChromeCommand {
    #[allow(clippy::print_stdout)]
    pub fn run(&self) -> error_stack::Result<(), Error> {
        let reader = FlightRecordReader::try_new(&self.input).change_context(Error::Internal)?;

        println!(
            "Converting flight records from {:?} to trace events {:?}",
            self.input, self.output
        );
        println!("Flight Record Header: {:?}", reader.header());

        let conversion = Conversion::new(reader.header());
        let records = reader.records().change_context(Error::Internal)?;
        let trace = conversion.convert_all(records)?;

        trace
            .write_to(&self.output)
            .attach_printable_lazy(|| format!("Output Path: {}", self.output.display()))?;

        println!(
            "Wrote trace events to {:?}",
            self.output
                .canonicalize()
                .into_report()
                .change_context(Error::Internal)?
        );
        Ok(())
    }
}
