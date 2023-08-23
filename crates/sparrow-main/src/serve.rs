//! Implementation of the gRPC services and the `serve` subcommand.

// Public for e2e tests.
mod compute_service;
mod error_status;
mod file_service;
pub(crate) mod preparation_service;
use error_stack::{IntoReport, ResultExt};
pub use error_status::*;

use sparrow_api::kaskada::v1alpha::compute_service_server::ComputeServiceServer;
use sparrow_api::kaskada::v1alpha::file_service_server::FileServiceServer;
use sparrow_api::kaskada::v1alpha::preparation_service_server::PreparationServiceServer;

use sparrow_runtime::stores::{ObjectStoreRegistry, ObjectStoreUrl};
use std::net::SocketAddr;

use std::str::FromStr;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{info, info_span};

use crate::serve::compute_service::ComputeServiceImpl;
use crate::serve::file_service::FileServiceImpl;
use crate::serve::preparation_service::PreparationServiceImpl;
use crate::tracing_setup::propagate_span;
use crate::BuildInfo;

/// Options for the Serve command.
#[derive(clap::Args, Debug)]
#[command(version, rename_all = "kebab-case")]
pub struct ServeCommand {
    /// Address to run the service on.
    #[arg(long, default_value = "0.0.0.0:50052", env = "SPARROW_GRPC_ADDR")]
    service_addr: SocketAddr,

    /// Path for writing (flight records and query plans).
    ///
    /// If `None`, flight records and query plans will not be written.
    ///
    /// For example, `s3://<bucket>/flight_records` to write to the
    /// `flight_records` prefix of the given S3 bucket.
    #[arg(long, env = "SPARROW_FLIGHT_RECORD_PATH")]
    flight_record_path: Option<String>,
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "invalid flight record path")]
    InvalidFlightRecordPath,
    #[display(fmt = "error running Tonic server")]
    ServerError,
}

impl error_stack::Context for Error {}

impl ServeCommand {
    pub async fn execute(&self) -> error_stack::Result<(), Error> {
        let build_info = BuildInfo::default();
        let span = info_span!("Sparrow in batch-mode", ?build_info);

        let _enter = span.enter();

        let object_stores = Arc::new(ObjectStoreRegistry::default());
        let file_service = FileServiceImpl::new(object_stores.clone());

        // Leak the diagnostic prefix to create a `&'static` reference.
        // This simplifies the lifetime management of the futures.
        // This string is fixed for the lifetime of `serve`, so leaking
        // it once doesn't create a problem.
        let flight_record_path = if let Some(flight_record_path) = &self.flight_record_path {
            let prefix = ObjectStoreUrl::from_str(flight_record_path)
                .change_context(Error::InvalidFlightRecordPath)?;
            assert!(
                prefix.is_delimited(),
                "Flight record path must end with `/` but was {flight_record_path}"
            );

            Some(prefix)
        } else {
            None
        };
        let flight_record_path = Box::leak(Box::new(flight_record_path));
        let compute_service = ComputeServiceImpl::new(flight_record_path, object_stores.clone());
        let preparation_service = PreparationServiceImpl::new(object_stores.clone());

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(sparrow_api::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
            .build()
            .unwrap();

        info!("Starting Sparrow listening on {}", self.service_addr);
        let server_future = Server::builder()
            .trace_fn(propagate_span)
            .add_service(health_service)
            .add_service(ComputeServiceServer::new(compute_service))
            .add_service(PreparationServiceServer::new(preparation_service))
            .add_service(FileServiceServer::new(file_service))
            .add_service(reflection_service)
            .serve(self.service_addr);

        // Report the query service as serving.
        health_reporter
            .set_serving::<ComputeServiceServer<ComputeServiceImpl>>()
            .await;

        server_future
            .await
            .into_report()
            .change_context(Error::ServerError)?;

        Ok(())
    }
}
