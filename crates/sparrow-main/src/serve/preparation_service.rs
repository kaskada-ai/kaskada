use std::sync::Arc;

use sparrow_api::kaskada::v1alpha::preparation_service_server::PreparationService;
use sparrow_api::kaskada::v1alpha::{
    GetCurrentPrepIdRequest, GetCurrentPrepIdResponse, PrepareDataRequest, PrepareDataResponse,
};
use sparrow_runtime::prepare::{prepare_file, Error};

use sparrow_runtime::stores::ObjectStoreRegistry;
use tonic::Response;

use crate::IntoStatus;

// The current preparation ID of the data preparation service
const CURRENT_PREP_ID: i32 = 6;

#[derive(Debug)]
pub(super) struct PreparationServiceImpl {
    object_store_registry: Arc<ObjectStoreRegistry>,
}

impl PreparationServiceImpl {
    pub fn new(object_stores: Arc<ObjectStoreRegistry>) -> Self {
        Self {
            object_store_registry: object_stores,
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

    let source_data = prepare_request
        .source_data
        .ok_or(Error::MissingField("source"))?;

    let prepared_files = prepare_file(
        &object_store_registry,
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
