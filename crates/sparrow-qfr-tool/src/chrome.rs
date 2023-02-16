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
//! execution. 6. Each source gets a separate stack frame.
//! 7.
//!
//! <https://aras-p.info/blog/2017/01/23/Chrome-Tracing-as-Profiler-Frontend/>
//!
//! <https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU>

use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;

use anyhow::Context;
use sparrow_qfr::FlightRecordReader;
use structopt::StructOpt;

mod chrome_tracing;
mod conversion;

use conversion::qfr_to_chrome;

#[derive(Debug, StructOpt)]
pub(crate) struct ChromeCommand {
    /// Input file containing the Query plan.
    #[structopt(long, parse(from_os_str))]
    pub plan: PathBuf,

    /// Input file containing the Flight records.
    #[structopt(long, parse(from_os_str))]
    pub input: PathBuf,

    /// Output file to write the output to.
    #[structopt(long, parse(from_os_str))]
    pub output: PathBuf,
}

impl ChromeCommand {
    #[allow(clippy::print_stdout)]
    pub fn run(&self) -> anyhow::Result<()> {
        let plan = File::open(&self.plan)
            .with_context(|| format!("opening plan file '{:?}'", self.plan))?;
        let plan = BufReader::new(plan);
        let plan = serde_yaml::from_reader(plan)
            .with_context(|| format!("reading plan file '{:?}'", self.plan))?;

        let reader = FlightRecordReader::try_new(&self.input)
            .with_context(|| format!("Reading {:?}", self.input))?;

        println!(
            "Converting flight records from {:?} to trace events {:?}",
            self.input, self.output
        );
        println!("Flight Record Header: {:?}", reader.header());

        let trace = qfr_to_chrome(&plan, reader).context("converting to trace events")?;

        let output =
            File::create(&self.output).with_context(|| format!("Creating {:?}", self.output))?;
        let mut buffer = BufWriter::new(output);
        serde_json::to_writer(&mut buffer, &trace).context("writing trace")?;
        buffer.flush().context("flushing output")?;

        println!(
            "Wrote trace events to {:?}",
            self.output
                .canonicalize()
                .with_context(|| format!("canonicalizing output path {:?}", self.output))?
        );
        Ok(())
    }
}
