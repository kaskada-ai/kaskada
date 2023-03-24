use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use error_stack::{IntoReportCompat, ResultExt, IntoReport};
use futures::future::try_join_all;
use sparrow_api::kaskada::v1alpha::file_service_server::FileService;
use sparrow_api::kaskada::v1alpha::Schema;
use sparrow_api::kaskada::v1alpha::{
    file_path, FileMetadata, FilePath, GetMetadataRequest, GetMetadataResponse,
    MergeMetadataRequest, MergeMetadataResponse,
};
use sparrow_core::context_code;
use sparrow_runtime::s3::is_s3_path;
use sparrow_runtime::{ObjectStoreRegistry, ObjectStoreUrl, RawMetadata};
use tempfile::NamedTempFile;
use tonic::{Code, Response};

use crate::serve::error_status::IntoStatus;

#[derive(Debug)]
pub(super) struct FileServiceImpl {
    object_store_registry: Arc<ObjectStoreRegistry>,
}

impl FileServiceImpl {
    pub fn new(object_store_registry: Arc<ObjectStoreRegistry>) -> Self {
        Self {
            object_store_registry,
        }
    }
}

#[tonic::async_trait]
impl FileService for FileServiceImpl {
    #[tracing::instrument]
    async fn get_metadata(
        &self,
        request: tonic::Request<GetMetadataRequest>,
    ) -> Result<tonic::Response<GetMetadataResponse>, tonic::Status> {
        let object_store = self.object_store_registry.clone();
        match tokio::spawn(get_metadata(object_store, request)).await {
            Ok(result) => result.into_status(),
            Err(panic) => {
                tracing::error!("Panic during prepare: {panic}");
                Err(tonic::Status::internal("panic during prepare"))
            }
        }
    }

    #[tracing::instrument]
    async fn merge_metadata(
        &self,
        _request: tonic::Request<MergeMetadataRequest>,
    ) -> Result<tonic::Response<MergeMetadataResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "merge_metadata not implemented",
        ))
    }
}

async fn get_metadata(
    object_store_registry: Arc<ObjectStoreRegistry>,
    request: tonic::Request<GetMetadataRequest>,
) -> anyhow::Result<tonic::Response<GetMetadataResponse>> {
    let request = request.into_inner();

    let file_metadatas: Vec<_> = request
        .file_paths
        .iter()
        .map(|source| get_source_metadata(object_store_registry.as_ref(), source))
        .collect();

    let file_metadatas = try_join_all(file_metadatas)
        .await
        .map_err(|e| anyhow::anyhow!("unable to get file metadata: {:?}", e))?;
    Ok(Response::new(GetMetadataResponse { file_metadatas }))
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "unable to get source path from request")]
    SourcePathError,
    #[display(fmt = "unable to create temporary file")]
    TempFileDownloadFileError,
    #[display(fmt = "unable to download file from object store")]
    ObjectStoreError,
    #[display(fmt = "schema error: '{_0}'")]
    SchemaError(String)
}
impl error_stack::Context for Error {}

pub(crate) async fn get_source_metadata(
    object_store_registry: &ObjectStoreRegistry,
    source: &FilePath,
) -> error_stack::Result<FileMetadata, Error> {
    let source = source
        .path
        .as_ref()
        .context(context_code!(Code::InvalidArgument, "Missing source_path"))
        .into_report()
        .change_context(Error::SourcePathError)?;

    let download_file = NamedTempFile::new()
        .into_report()
        .change_context(Error::TempFileDownloadFileError)?;
    
    let download_file_path = download_file.into_temp_path();
    let source = match source {
        file_path::Path::ParquetPath(path) => {
            if is_s3_path(path) {
                let object_store_url = ObjectStoreUrl::from_str(path).change_context(Error::ObjectStoreError)?;
                let object_store_key = object_store_url.key().change_context(Error::ObjectStoreError)?;
                let object_store = object_store_registry
                    .object_store(object_store_key)
                    .change_context(Error::ObjectStoreError)?;
                object_store_url
                    .download(object_store, download_file_path.to_owned())
                    .await
                    .change_context(Error::ObjectStoreError)?;
                file_path::Path::ParquetPath(download_file_path.to_string_lossy().to_string())
            } else {
                file_path::Path::ParquetPath(path.to_string())
            }
        }
        file_path::Path::CsvPath(path) => {
            if is_s3_path(path) {
                let object_store_url = ObjectStoreUrl::from_str(path).change_context(Error::ObjectStoreError)?;
                let object_store_key = object_store_url.key().change_context(Error::ObjectStoreError)?;
                let object_store = object_store_registry
                    .object_store(object_store_key)
                    .change_context(Error::ObjectStoreError)?;
                object_store_url
                    .download(object_store, download_file_path.to_owned())
                    .await
                    .change_context(Error::ObjectStoreError)?;
                file_path::Path::CsvPath(download_file_path.to_string_lossy().to_string())
            } else {
                file_path::Path::CsvPath(path.to_string())
            }
        }
        file_path::Path::CsvData(data) => file_path::Path::CsvData(data.to_string()),
    };

    let metadata = RawMetadata::try_from(&source)
        .into_report()
        .change_context(Error::SchemaError("unable to get raw metadata".to_owned()))?;
    let schema = Schema::try_from(metadata.table_schema.as_ref())
        .into_report()
        .change_context(Error::SchemaError(format!(
            "Unable to encode schema {:?} for source file {:?}", metadata.table_schema, source
        ).to_owned()))?;

    Ok(FileMetadata {
        schema: Some(schema),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_timestamp_with_timezone_gets_metadata() {
        // Regression test for handling files with a timestamp with a configured
        // timezone.
        let path = sparrow_testing::testdata_path("eventdata/sample_event_data.parquet");
        let object_store_registry = Arc::new(ObjectStoreRegistry::new());
        let file_service = FileServiceImpl::new(object_store_registry);

        let result = file_service
            .get_metadata(tonic::Request::new(GetMetadataRequest {
                file_paths: vec![FilePath {
                    path: Some(file_path::Path::ParquetPath(
                        path.canonicalize().unwrap().to_string_lossy().to_string(),
                    )),
                }],
            }))
            .await
            .unwrap()
            .into_inner();

        insta::assert_yaml_snapshot!(result);
    }

    #[tokio::test]
    async fn test_get_metadata_csv_path() {
        let path =
            sparrow_testing::testdata_path("eventdata/2c889258-d676-4922-9a92-d7e9c60c1dde.csv");

        let object_store_registry = Arc::new(ObjectStoreRegistry::new());
        let file_service = FileServiceImpl::new(object_store_registry);
        let result = file_service
            .get_metadata(tonic::Request::new(GetMetadataRequest {
                file_paths: vec![FilePath {
                    path: Some(file_path::Path::CsvPath(
                        path.canonicalize().unwrap().to_string_lossy().to_string(),
                    )),
                }],
            }))
            .await
            .unwrap()
            .into_inner();

        insta::assert_yaml_snapshot!(result);
    }

    #[tokio::test]
    async fn test_get_metadata_csv_data() {
        let csv_data = "id,name,value,value2\n1,awkward,taco,123\n2,taco,awkward,456\n";
        let object_store_registry = Arc::new(ObjectStoreRegistry::new());
        let file_service = FileServiceImpl::new(object_store_registry);

        let result = file_service
            .get_metadata(tonic::Request::new(GetMetadataRequest {
                file_paths: vec![FilePath {
                    path: Some(file_path::Path::CsvData(csv_data.to_string())),
                }],
            }))
            .await
            .unwrap()
            .into_inner();

        insta::assert_yaml_snapshot!(result);
    }
}
