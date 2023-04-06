use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use error_stack::{IntoReport, ResultExt};
use sparrow_api::kaskada::v1alpha::preparation_service_server::PreparationService;
use sparrow_api::kaskada::v1alpha::source_data::Source;
use sparrow_api::kaskada::v1alpha::{
    source_data, GetCurrentPrepIdRequest, GetCurrentPrepIdResponse, PrepareDataRequest,
    PrepareDataResponse, SourceData,
};
use sparrow_runtime::prepare::{prepare_file, Error};

use sparrow_runtime::stores::object_store_url::ObjectStoreKey;
use sparrow_runtime::stores::{ObjectStoreRegistry, ObjectStoreUrl};
use tempfile::NamedTempFile;
use tonic::Response;

use crate::IntoStatus;

// The current preparation ID of the data preparation service
const CURRENT_PREP_ID: i32 = 5;

#[derive(Debug)]
pub(super) struct PreparationServiceImpl {
    object_store_registry: Arc<ObjectStoreRegistry>,
}

impl PreparationServiceImpl {
    pub fn new(object_store_registry: Arc<ObjectStoreRegistry>) -> Self {
        Self {
            object_store_registry,
        }
    }
}

#[tonic::async_trait]
impl PreparationService for PreparationServiceImpl {
    #[tracing::instrument(err)]
    async fn prepare_data(
        &self,
        request: tonic::Request<PrepareDataRequest>,
    ) -> Result<tonic::Response<PrepareDataResponse>, tonic::Status> {
        let object_store = self.object_store_registry.clone();

        let handle = tokio::spawn(prepare_data(object_store, request));
        match handle.await {
            Ok(result) => result.into_status(),
            Err(panic) => {
                tracing::error!("Panic during prepare: {panic}");
                Err(tonic::Status::internal("panic during prepare"))
            }
        }
    }

    #[tracing::instrument(err)]
    async fn get_current_prep_id(
        &self,
        _request: tonic::Request<GetCurrentPrepIdRequest>,
    ) -> Result<tonic::Response<GetCurrentPrepIdResponse>, tonic::Status> {
        let reply = GetCurrentPrepIdResponse {
            prep_id: CURRENT_PREP_ID,
        };
        Ok(Response::new(reply))
    }
}

pub async fn prepare_data(
    object_store_registry: Arc<ObjectStoreRegistry>,
    request: tonic::Request<PrepareDataRequest>,
) -> error_stack::Result<tonic::Response<PrepareDataResponse>, Error> {
    let prepare_request = request.into_inner();
    let table_config = prepare_request
        .config
        .ok_or(Error::MissingField("table_config"))?;

    let slice_plan = prepare_request
        .slice_plan
        .ok_or(Error::MissingField("slice_plan"))?;

    error_stack::ensure!(
        slice_plan.table_name == table_config.name,
        Error::IncorrectSlicePlan {
            slice_plan: slice_plan.table_name,
            table_config: table_config.name
        }
    );

    let temp_file = NamedTempFile::new()
        .into_report()
        .change_context(Error::Internal)?;
    let source_data = convert_to_local_sourcedata(
        object_store_registry,
        prepare_request.source_data.as_ref(),
        temp_file.path(),
    )
    .await?;
    let (_prepared_metadata, prepared_files) = prepare_file(
        &source_data,
        &prepare_request.output_path_prefix,
        &prepare_request.file_prefix,
        &table_config,
        &slice_plan.slice,
    )
    .await?;

    Ok(Response::new(PrepareDataResponse {
        prep_id: CURRENT_PREP_ID,
        prepared_files,
    }))
}

pub async fn convert_to_local_sourcedata(
    object_store_registry: Arc<ObjectStoreRegistry>,
    source_data: Option<&SourceData>,
    local_path: &Path,
) -> error_stack::Result<SourceData, Error> {
    match source_data {
        None => error_stack::bail!(Error::MissingField("source_data")),
        Some(sd) => {
            let source = sd.source.as_ref().ok_or(Error::MissingField("source"))?;
            let local_path = match source {
                source_data::Source::ParquetPath(path) => {
                    let object_store_url = ObjectStoreUrl::from_str(path).unwrap();
                    let object_store_key = object_store_url.key().unwrap();
                    match object_store_key {
                        ObjectStoreKey::Local | ObjectStoreKey::Memory => {
                            Source::ParquetPath(format!("/{}", object_store_url.path().unwrap()))
                        }
                        ObjectStoreKey::Aws {
                            bucket: _,
                            region: _,
                            virtual_hosted_style_request: _,
                        } => {
                            object_store_url
                                .download(&object_store_registry, local_path.to_path_buf())
                                .await
                                .unwrap();
                            Source::ParquetPath(format!(
                                "/{}",
                                local_path.canonicalize().unwrap().to_string_lossy()
                            ))
                        }
                        ObjectStoreKey::Gcs { bucket: _ } => {
                            object_store_url
                                .download(&object_store_registry, local_path.to_path_buf())
                                .await
                                .unwrap();
                            Source::ParquetPath(format!(
                                "/{}",
                                local_path.canonicalize().unwrap().to_string_lossy()
                            ))
                        }
                    }
                }
                source_data::Source::CsvPath(csv_path) => {
                    let object_store_url = ObjectStoreUrl::from_str(csv_path).unwrap();
                    let object_store_key = object_store_url.key().unwrap();
                    match object_store_key {
                        ObjectStoreKey::Local | ObjectStoreKey::Memory => {
                            Source::CsvPath(format!("/{}", object_store_url.path().unwrap()))
                        }
                        ObjectStoreKey::Aws {
                            bucket: _,
                            region: _,
                            virtual_hosted_style_request: _,
                        } => {
                            object_store_url
                                .download(&object_store_registry, local_path.to_path_buf())
                                .await
                                .unwrap();
                            Source::CsvPath(format!(
                                "/{}",
                                local_path.canonicalize().unwrap().to_string_lossy()
                            ))
                        }
                        ObjectStoreKey::Gcs { bucket: _ } => {
                            object_store_url
                                .download(&object_store_registry, local_path.to_path_buf())
                                .await
                                .unwrap();
                            Source::CsvPath(format!(
                                "/{}",
                                local_path.canonicalize().unwrap().to_string_lossy()
                            ))
                        }
                    }
                }
                source_data::Source::CsvData(data) => source_data::Source::CsvData(data.to_owned()),
                source_data::Source::PulsarSubscription(_) => source.clone(),
            };
            Ok(SourceData {
                source: Some(local_path),
            })
        }
    }
}
