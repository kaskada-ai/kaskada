use std::path::PathBuf;

use clap::arg;
use clap::command;
use sparrow_api::kaskada::v1alpha::PerEntityBehavior;
use sparrow_api::kaskada::v1alpha::SliceRequest;

#[derive(clap::Args, Debug, Clone)]
#[command(rename_all = "kebab-case")]
pub struct InternalCompileOptions {
    /// Path to store a copy of the pre-optimization DFG to.
    /// Defaults to not storing anything.
    #[arg(long)]
    pub store_initial_dfg: Option<PathBuf>,

    /// Path to store a copy of the post-optimization DFG to.
    /// Defaults to not storing anything.
    #[arg(long)]
    pub store_final_dfg: Option<PathBuf>,

    /// Path to store a copy of the query plan to as a `dot` file.
    /// Defaults to not storing a plan.
    #[arg(long)]
    pub store_plan_graph: Option<PathBuf>,

    /// Path to store a copy of the query plan to as a `yaml` file.
    /// Defaults to not storing a plan.
    #[arg(long)]
    pub store_plan_yaml: Option<PathBuf>,

    /// Maximum number of iterations of the simplifier to run.
    ///
    /// NOTE: The environment variable won't affect the default values.
    #[arg(long, default_value = "30", env = "SPARROW_SIMPLIFIER_ITERATION_LIMIT")]
    pub simplifier_iteration_limit: usize,

    /// Maximum number of nodes during simplifier execution.
    ///
    /// NOTE: The environment variable won't affect the default values.
    #[arg(long, default_value = "10000", env = "SPARROW_SIMPLIFIER_NODE_LIMIT")]
    pub simplifier_node_limit: usize,

    /// Maximum amount of time to run the simplifier before ending early.
    ///
    /// NOTE: The environment variable won't affect the default values.
    // This just use an integer number of seconds. We could use a `Duration`
    // and parse it with https://docs.rs/parse_duration/latest/parse_duration/
    // but that seems like an unnecessary dependency right now.
    #[arg(
        long,
        default_value = "5",
        env = "SPARROW_SIMPLIFIER_TIME_LIMIT_SECONDS"
    )]
    pub simplifier_time_limit_seconds: f64,
}

/// Command line options that may
#[derive(clap::Args, Debug, Clone)]
#[command(rename_all = "kebab-case")]
pub struct CompilerOptions {
    #[command(flatten)]
    pub internal: InternalCompileOptions,

    /// The query behavior being compiled.
    #[arg(skip = DEFAULT_PER_ENTITY_BEHAVIOR)]
    pub per_entity_behavior: PerEntityBehavior,

    /// The slicing requested for the query being compiled.
    #[arg(skip)]
    pub slice_request: Option<SliceRequest>,

    /// Whether experimental behaviors should be enabled.
    #[arg(long, action)]
    pub experimental: bool,
}

const DEFAULT_PER_ENTITY_BEHAVIOR: PerEntityBehavior = PerEntityBehavior::All;

pub const DEFAULT_COMPILE_OPTIONS: InternalCompileOptions = InternalCompileOptions {
    store_initial_dfg: None,
    store_final_dfg: None,
    store_plan_graph: None,
    store_plan_yaml: None,
    simplifier_iteration_limit: 30,
    simplifier_node_limit: 10000,
    simplifier_time_limit_seconds: 5.0,
};

impl Default for InternalCompileOptions {
    fn default() -> Self {
        DEFAULT_COMPILE_OPTIONS.clone()
    }
}

impl Default for CompilerOptions {
    fn default() -> Self {
        Self {
            internal: InternalCompileOptions::default(),
            per_entity_behavior: DEFAULT_PER_ENTITY_BEHAVIOR,
            slice_request: None,
            experimental: false,
        }
    }
}
