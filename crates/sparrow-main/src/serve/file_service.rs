use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use futures::future::try_join_all;
use sparrow_api::kaskada::v1alpha::file_service_server::FileService;
use sparrow_api::kaskada::v1alpha::Schema;
use sparrow_api::kaskada::v1alpha::{
    file_path, FileMetadata, FilePath, GetMetadataRequest, GetMetadataResponse,
    MergeMetadataRequest, MergeMetadataResponse,
};
use sparrow_core::context_code;
use sparrow_runtime::s3::{is_s3_path, S3Helper, S3Object};
use sparrow_runtime::{ObjectStoreRegistry, ObjectStoreUrl, RawMetadata};
use tempfile::NamedTempFile;
use tonic::{Code, Response};

use crate::serve::error_status::IntoStatus;

#[derive(Debug)]
pub(super) struct FileServiceImpl {
    s3: S3Helper, // TODO: Delete the s3 helper.
    object_store_registry: Arc<ObjectStoreRegistry>,
}

impl FileServiceImpl {
    pub fn new(object_store_registry: Arc<ObjectStoreRegistry>, s3: S3Helper) -> Self {
        Self {
            object_store_registry,
            s3,
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
        let s3 = self.s3.clone();
        let object_store = self.object_store_registry.clone();
        match tokio::spawn(get_metadata(s3, object_store, request)).await {
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
    s3: S3Helper,
    object_store_registry: Arc<ObjectStoreRegistry>,
    request: tonic::Request<GetMetadataRequest>,
) -> anyhow::Result<tonic::Response<GetMetadataResponse>> {
    let request = request.into_inner();

    let file_metadatas: Vec<_> = request
        .file_paths
        .iter()
        .map(|source| get_source_metadata(&s3, object_store_registry.as_ref(), source))
        .collect();

    let file_metadatas = try_join_all(file_metadatas).await?;
    Ok(Response::new(GetMetadataResponse { file_metadatas }))
}

pub(crate) async fn get_source_metadata(
    s3: &S3Helper,
    object_store_registry: &ObjectStoreRegistry,
    source: &FilePath,
) -> anyhow::Result<FileMetadata> {
    let source = source
        .path
        .as_ref()
        .context(context_code!(Code::InvalidArgument, "Missing source_path"))?;

    let download_file = NamedTempFile::new()?;
    let download_file_path = download_file.into_temp_path();
    let source = match source {
        file_path::Path::ParquetPath(path) => {
            if is_s3_path(path) {
                let object_store_url = ObjectStoreUrl::from_str(path).unwrap(); // TODO: Fix this unwrap.
                let object_store_key = object_store_url.key().unwrap(); // TODO: Fix this unwrap.
                let object_store = object_store_registry
                    .object_store(object_store_key)
                    .unwrap(); // TODO: Fix this unwrap.
                object_store_url
                    .download(object_store, download_file_path.to_owned())
                    .await
                    .unwrap();
                file_path::Path::ParquetPath(download_file_path.to_string_lossy().to_string())
            } else {
                file_path::Path::ParquetPath(path.to_string())
            }
        }
        file_path::Path::CsvPath(path) => {
            if is_s3_path(path) {
                let s3_object = S3Object::try_from_uri(path)?;
                s3.download_s3(s3_object, download_file_path.to_owned())
                    .await?;
                file_path::Path::CsvPath(download_file_path.to_string_lossy().to_string())
            } else {
                file_path::Path::CsvPath(path.to_string())
            }
        }
        file_path::Path::CsvData(data) => file_path::Path::CsvData(data.to_string()),
    };

    let metadata = RawMetadata::try_from(&source)?;
    let schema = Schema::try_from(metadata.table_schema.as_ref()).with_context(|| {
        format!(
            "Unable to encode schema {:?} for source file {:?}",
            metadata.table_schema, source
        )
    })?;

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

        let s3_helper = S3Helper::new().await;
        let object_store_registry = Arc::new(ObjectStoreRegistry::new());
        let file_service = FileServiceImpl::new(object_store_registry, s3_helper);

        // TODO: We may be able to clean this up to test the metadata logic
        // without creating the S3 helper. But for now, this will suffice.
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

        let s3_helper = S3Helper::new().await;
        let object_store_registry = Arc::new(ObjectStoreRegistry::new());
        let file_service = FileServiceImpl::new(object_store_registry, s3_helper);

        // TODO: We may be able to clean this up to test the metadata logic
        // without creating the S3 helper. But for now, this will suffice.
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
        let s3_helper = S3Helper::new().await;
        let object_store_registry = Arc::new(ObjectStoreRegistry::new());
        let file_service = FileServiceImpl::new(object_store_registry, s3_helper);

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
