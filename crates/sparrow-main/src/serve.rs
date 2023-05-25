//! Implementation of the gRPC services and the `serve` subcommand.

// Public for e2e tests.
mod compute_service;
mod error_status;
mod file_service;
pub(crate) mod preparation_service;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
pub use error_status::*;

use sparrow_api::kaskada::v1alpha::compute_service_server::ComputeServiceServer;
use sparrow_api::kaskada::v1alpha::file_service_server::FileServiceServer;
use sparrow_api::kaskada::v1alpha::preparation_service_server::PreparationServiceServer;
use sparrow_instructions::ComputeStore;
use sparrow_runtime::s3::{S3Helper, S3Object};
use sparrow_runtime::stores::ObjectStoreRegistry;
use std::net::SocketAddr;
use std::path::Path;
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
    #[display(fmt = "internal error")]
    Internal,
}

impl error_stack::Context for Error {}

/// The main compute store for sparrow, shared across requests.
const COMPUTE_STORE_PATH: &str = "main_compute_store";

impl ServeCommand {
    pub async fn execute(&self) -> error_stack::Result<(), Error> {
        let build_info = BuildInfo::default();
        let span = info_span!("Sparrow in batch-mode", ?build_info);

        let _enter = span.enter();

        let s3 = S3Helper::new().await;
        let object_store_registry = Arc::new(ObjectStoreRegistry::new());
        let file_service = FileServiceImpl::new(object_store_registry.clone());

        // Leak the diagnostic prefix to create a `&'static` reference.
        // This simplifies the lifetime management of the futures.
        // This string is fixed for the lifetime of `serve`, so leaking
        // it once doesn't create a problem.
        let flight_record_path = if let Some(flight_record_path) = &self.flight_record_path {
            Some(
                S3Object::try_from_uri(flight_record_path)
                    .into_report()
                    .change_context(Error::InvalidFlightRecordPath)?,
            )
        } else {
            None
        };
        let flight_record_path = Box::leak(Box::new(flight_record_path));

        // Sparrow's main compute store
        let compute_store = ComputeStore::try_new_from_path(Path::new(COMPUTE_STORE_PATH))
            .into_report()
            .change_context(Error::Internal)
            .attach_printable("failed to create compute store")?;

        let compute_service =
            ComputeServiceImpl::new(flight_record_path, s3.clone(), compute_store);
        let preparation_service = PreparationServiceImpl::new(object_store_registry.clone());

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(sparrow_api::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(
                tonic_health::proto::GRPC_HEALTH_V1_FILE_DESCRIPTOR_SET,
            )
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
