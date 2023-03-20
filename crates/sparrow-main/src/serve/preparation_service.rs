use std::path::Path;

use sparrow_api::kaskada::v1alpha::preparation_service_server::PreparationService;
use sparrow_api::kaskada::v1alpha::prepare_data_request::SourceData;
use sparrow_api::kaskada::v1alpha::{
    file_path, GetCurrentPrepIdRequest, GetCurrentPrepIdResponse, PrepareDataRequest,
    PrepareDataResponse, PreparedFile,
};
use sparrow_runtime::prepare::{prepare_file, upload_prepared_files_to_s3, Error};
use sparrow_runtime::s3::{is_s3_path, S3Helper, S3Object};

use tempfile::NamedTempFile;
use tonic::Response;

use crate::IntoStatus;

// The current preparation ID of the data preparation service
const CURRENT_PREP_ID: i32 = 5;

#[derive(Debug)]
pub(super) struct PreparationServiceImpl {
    s3: S3Helper,
}

impl PreparationServiceImpl {
    pub fn new(s3: S3Helper) -> Self {
        Self { s3 }
    }
}

#[tonic::async_trait]
impl PreparationService for PreparationServiceImpl {
    #[tracing::instrument(err)]
    async fn prepare_data(
        &self,
        request: tonic::Request<PrepareDataRequest>,
    ) -> Result<tonic::Response<PrepareDataResponse>, tonic::Status> {
        let s3 = self.s3.clone();

        let handle = tokio::spawn(prepare_data(s3, request));
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
    s3: S3Helper,
    request: tonic::Request<PrepareDataRequest>,
) -> error_stack::Result<tonic::Response<PrepareDataResponse>, Error> {
    let prepare_request = request.into_inner();
    let table_config = prepare_request
        .config
        .ok_or(Error::MissingField("table_config"))?;

    let source_data = prepare_request
        .source_data
        .ok_or(Error::MissingField("source_data"))?;
    let file_path = match source_data {
        SourceData::FilePath(fp) => fp,
        SourceData::PulsarConfig(_) => {
            error_stack::bail!(Error::Internal)
        }
    };

    let path = file_path
        .path
        .as_ref()
        .ok_or(Error::MissingField("file_path.path"))?;

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

    let download_file = NamedTempFile::new().unwrap();
    let download_file_path = download_file.into_temp_path();
    let (is_s3_object, path) = match path {
        file_path::Path::ParquetPath(path) => {
            if is_s3_path(path) {
                let s3_object = S3Object::try_from_uri(path).unwrap();
                s3.download_s3(s3_object, download_file_path.to_owned())
                    .await
                    .unwrap();
                (
                    true,
                    file_path::Path::ParquetPath(download_file_path.to_string_lossy().to_string()),
                )
            } else {
                (false, file_path::Path::ParquetPath(path.to_string()))
            }
        }
        file_path::Path::CsvPath(path) => {
            if is_s3_path(path) {
                let s3_object = S3Object::try_from_uri(path).unwrap();
                s3.download_s3(s3_object, download_file_path.to_owned())
                    .await
                    .unwrap();
                (
                    true,
                    file_path::Path::CsvPath(download_file_path.to_string_lossy().to_string()),
                )
            } else {
                (false, file_path::Path::CsvPath(path.to_string()))
            }
        }
        file_path::Path::CsvData(data) => (false, file_path::Path::CsvData(data.to_string())),
    };

    let (prepared_metadata, prepared_files) = prepare_file(
        &path,
        Path::new(&prepare_request.output_path_prefix),
        &prepare_request.file_prefix,
        &table_config,
        &slice_plan.slice,
    )?;

    let prepared_files: Vec<PreparedFile> = if is_s3_object {
        upload_prepared_files_to_s3(
            s3,
            &prepared_metadata,
            &prepare_request.file_prefix,
            &prepare_request.output_path_prefix,
        )
        .await?
    } else {
        prepared_files
    };

    Ok(Response::new(PrepareDataResponse {
        prep_id: CURRENT_PREP_ID,
        prepared_files,
    }))
}
