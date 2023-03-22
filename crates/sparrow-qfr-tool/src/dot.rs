use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use sparrow_api::kaskada::v1alpha::ComputePlan;

use crate::error::Error;

#[derive(Debug, clap::Args)]
#[command(version, rename_all = "kebab-case")]
pub(crate) struct DotPlanCommand {
    /// Input file containing the Query plan.
    #[arg(long, value_name = "FILE")]
    pub plan: PathBuf,

    /// Output file to write the output to.
    #[arg(long, value_name = "FILE")]
    pub output: PathBuf,
}

impl DotPlanCommand {
    pub(super) fn run(self) -> error_stack::Result<(), Error> {
        let plan = plan_from_yaml(&self.plan)
            .attach_printable_lazy(|| format!("Plan Path: {}", self.plan.display()))?;
        plan.write_to_graphviz_path(&self.output)
            .into_report()
            .change_context(Error::Internal)?;

        Ok(())
    }
}

fn plan_from_yaml(path: &std::path::Path) -> error_stack::Result<ComputePlan, Error> {
    let plan = File::open(path)
        .into_report()
        .change_context(Error::Internal)?;
    let plan = BufReader::new(plan);
    let plan: ComputePlan = serde_yaml::from_reader(plan)
        .into_report()
        .change_context(Error::Internal)?;

    Ok(plan)
}
