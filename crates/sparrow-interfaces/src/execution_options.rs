/// The kind of results the execution should produce.
#[derive(Default)]
pub enum Results {
    /// Produce the entire history of changes.
    #[default]
    History,
    /// Produce only a snapshot at a specific point in time (for each entity).
    Snapshot,
}

/// Options affecting the execution of a query.
#[derive(Default)]
pub struct ExecutionOptions {
    /// The maximum number of rows to return.
    pub row_limit: Option<usize>,
    /// The maximum number of rows to return in a single batch.
    pub max_batch_size: Option<usize>,
    /// Whether to run execute as a materialization or not.
    pub materialize: bool,
    /// History or Snapshot results.
    pub results: Results,
    /// The changed since time. This is the minimum timestamp of changes to events.
    /// For historic queries, this limits the output points.
    /// For snapshot queries, this limits the set of entities that are considered changed.
    pub changed_since_time_s: Option<i64>,
    /// The final at time. This is the maximum timestamp output.
    /// For historic queries, this limits the output points.
    /// For snapshot queries, this determines the time at which the snapshot is produced.
    pub final_at_time_s: Option<i64>,
}
