#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr
)]

use clap::Parser;

mod chrome;
mod chrome_tracing;
mod dot;
mod error;

use crate::error::Error;

#[derive(Debug, clap::Parser)]
#[command(name = "sparrow-qfr", rename_all = "kebab-case", version)]
pub struct QfrOptions {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
    Chrome(chrome::ChromeCommand),
    DotPlan(dot::DotPlanCommand),
}

fn main() -> error_stack::Result<(), Error> {
    let options = QfrOptions::parse();

    match options.command {
        Command::Chrome(command) => command.run(),
        Command::DotPlan(command) => command.run(),
    }
}
