use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use anyhow::Context;
use sparrow_compiler::plan_to_graphviz_file;
use sparrow_plan::ComputePlan;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub(crate) struct DotPlanCommand {
    /// Input file containing the Query plan.
    #[structopt(long, parse(from_os_str))]
    pub plan: PathBuf,

    /// Output file to write the output to.
    #[structopt(long, parse(from_os_str))]
    pub output: PathBuf,
}

impl DotPlanCommand {
    pub(super) fn run(self) -> anyhow::Result<()> {
        let plan = File::open(&self.plan)
            .with_context(|| format!("opening plan file '{:?}'", self.plan))?;
        let plan = BufReader::new(plan);
        let plan: ComputePlan = serde_yaml::from_reader(plan)
            .with_context(|| format!("reading plan file '{:?}'", self.plan))?;

        plan_to_graphviz_file(&plan, &self.output).context("writing plan graphviz")?;

        Ok(())
    }
}
