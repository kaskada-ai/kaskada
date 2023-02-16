#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr
)]

use structopt::StructOpt;

mod chrome;
mod dot;

#[derive(Debug, StructOpt)]
#[structopt(name = "sparrow-qfr", rename_all = "kebab-case")]
pub struct QfrOptions {
    #[structopt(subcommand)]
    command: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    Chrome(chrome::ChromeCommand),
    DotPlan(dot::DotPlanCommand),
}

fn main() -> anyhow::Result<()> {
    let options = QfrOptions::from_args();

    match options.command {
        Command::Chrome(command) => command.run(),
        Command::DotPlan(command) => command.run(),
    }
}
