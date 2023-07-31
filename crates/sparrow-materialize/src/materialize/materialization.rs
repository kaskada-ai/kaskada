use error_stack::ResultExt;
use sparrow_api::kaskada::v1alpha::{ComputePlan, ComputeTable, ExecuteResponse};
use sparrow_runtime::execute::output::Destination;
use tokio_stream::Stream;

use crate::Error;

/// Materialization struct that holds all information about a materialization process.
pub struct Materialization {
    /// Unique identifier of the materialization
    pub id: String,
    /// Compute plan that is used for the materialization
    pub plan: ComputePlan,
    /// Tables (or streams) that are used for the materialization
    pub tables: Vec<ComputeTable>,
    /// Destination of the materialization
    pub destination: Destination,
}

impl Materialization {
    /// Creates a new materialization struct.
    ///
    /// # Arguments
    /// * id - Unique identifier of the materialization
    /// * plan - Compute plan that is used for the materialization
    /// * tables - Tables (or streams) that are used for the materialization
    /// * destination - Destination of the materialization
    pub fn new(
        id: String,
        plan: ComputePlan,
        tables: Vec<ComputeTable>,
        destination: Destination,
    ) -> Self {
        Self {
            id,
            plan,
            tables,
            destination,
        }
    }

    /// Starts a materialization process
    ///
    /// # Arguments
    /// * materialization - materialization struct that holds all information about a materialization process
    /// * s3_helper - s3 client helper
    /// * bounded_lateness_ns - configurable value for handling the allowed lateness of events
    /// * stop_rx - receiver for the stop signal, allowing cancellation of the materialization process
    pub async fn start(
        materialization: Materialization,
        bounded_lateness_ns: Option<i64>,
        stop_rx: tokio::sync::watch::Receiver<bool>,
    ) -> error_stack::Result<
        impl Stream<Item = error_stack::Result<ExecuteResponse, sparrow_runtime::execute::error::Error>>,
        Error,
    > {
        let progress_stream = sparrow_runtime::execute::materialize(
            materialization.plan,
            materialization.destination,
            materialization.tables,
            bounded_lateness_ns,
            stop_rx,
        )
        .await
        .change_context(Error::CreateMaterialization)?;

        Ok(progress_stream)
    }
}
