use std::path::Path;

use error_stack::{IntoReport, ResultExt};
use sparrow_api::kaskada::v1alpha::preparation_service_server::PreparationService;
use sparrow_api::kaskada::v1alpha::{file_path, FilePath, GetCurrentPrepIdRequest, GetCurrentPrepIdResponse, PrepareDataRequest, PrepareDataResponse, PreparedFile, PulsarSource};
use sparrow_runtime::prepare::{prepare_file, upload_prepared_files_to_s3, Error};
use sparrow_runtime::s3::{is_s3_path, S3Helper, S3Object};

use tempfile::NamedTempFile;
use tonic::Response;
use sparrow_api::kaskada::v1alpha::get_metadata_request::Source;
use sparrow_api::kaskada::v1alpha::prepare_data_request::SourceData;

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

    let (is_s3_object, source) = convert_to_local_sourcedata(&s3, &prepare_request.source_data).await?;

    let temp_dir = tempfile::tempdir()
        .into_report()
        .change_context(Error::Internal)?;
    let output_path = if is_s3_path(&prepare_request.output_path_prefix) {
        temp_dir.path().to_str().ok_or(Error::Internal)?
    } else {
        &prepare_request.output_path_prefix
    };

    let (prepared_metadata, prepared_files) = prepare_file(
        &source,
        Path::new(&output_path),
        &prepare_request.file_prefix,
        &table_config,
        &slice_plan.slice,
    )?;

    let prepared_files: Vec<PreparedFile> = if is_s3_object {
        upload_prepared_files_to_s3(
            s3,
            &prepared_metadata,
            &prepare_request.output_path_prefix,
            &prepare_request.file_prefix,
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

pub async fn convert_to_local_source(s3: &S3Helper, source: &Source) -> error_stack::Result<(bool, Source), Error> {
    match source {
        Source::FilePath(fp) => {
            match &fp.path {
                None => error_stack::bail!(Error::MissingField("file_path")),
                Some(path) => {
                    let (is_s3, local_path) = maybe_download_file(&s3, path).await;
                    Ok((is_s3, Source::FilePath(FilePath { path: Some(local_path) })))
                }
            }
        }
        Source::PuslarSource(_) => {
            Ok((false, source.clone()))
        }
    }
}

pub async fn convert_to_local_sourcedata(s3: &S3Helper, source_data: &Option<SourceData>) -> error_stack::Result<(bool, SourceData), Error> {
    match source_data {
        None => error_stack::bail!(Error::MissingField("source_data")),
        Some(sd) => {
            match sd {
                SourceData::FilePath(fp) => {
                    match &fp.path {
                        None => error_stack::bail!(Error::MissingField("file_path")),
                        Some(path) => {
                            let (is_s3, local_path) = maybe_download_file(&s3, path).await;
                            Ok((is_s3, SourceData::FilePath(FilePath { path: Some(local_path) })))
                        }
                    }
                }
                SourceData::PulsarConfig(_) => {
                    Ok((false, sd.clone()))
                }
            }
        }
    }
}

// download remote file locally, if necessary.
async fn maybe_download_file(s3: &S3Helper, path: &file_path::Path) -> (bool, file_path::Path) {
    let download_file = NamedTempFile::new().unwrap();
    let download_file_path = download_file.into_temp_path();
    match path {
        file_path::Path::ParquetPath(path) => {
            let path = path.as_str();
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
            let path = path.as_str();
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
        file_path::Path::CsvData(data) => (false, file_path::Path::CsvData(data.to_string()))
    }
}
