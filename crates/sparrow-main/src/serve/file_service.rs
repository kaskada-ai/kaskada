use anyhow::{anyhow, Context};

use sparrow_api::kaskada::v1alpha::file_service_server::FileService;
use sparrow_api::kaskada::v1alpha::{
    GetMetadataRequest, GetMetadataResponse, MergeMetadataRequest, MergeMetadataResponse,
    SourceMetadata,
};
use sparrow_api::kaskada::v1alpha::{Schema, SourceData};

use sparrow_runtime::s3::S3Helper;
use sparrow_runtime::RawMetadata;

use tempfile::NamedTempFile;
use tonic::Response;

use crate::serve::error_status::IntoStatus;
use crate::serve::preparation_service;

#[derive(Debug)]
pub(super) struct FileServiceImpl {
    s3: S3Helper,
}

impl FileServiceImpl {
    pub fn new(s3: S3Helper) -> Self {
        Self { s3 }
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

        match tokio::spawn(get_metadata(s3, request)).await {
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
    request: tonic::Request<GetMetadataRequest>,
) -> anyhow::Result<tonic::Response<GetMetadataResponse>> {
    let request = request.into_inner();
    if request.source_data.is_none() {
        anyhow::bail!("missing request source");
    }

    let file_metadata = get_source_metadata(&s3, &request.source_data.unwrap()).await?;
    Ok(Response::new(GetMetadataResponse {
        source_metadata: Some(file_metadata),
    }))
}

pub(crate) async fn get_source_metadata(
    s3: &S3Helper,
    source_data: &SourceData,
) -> anyhow::Result<SourceMetadata> {
    let temp_file = NamedTempFile::new().context("Unable to create temporary file")?;
    let (_, local_source) =
        preparation_service::convert_to_local_sourcedata(s3, Some(source_data), temp_file.path())
            .await
            .map_err(|e| anyhow!("Unable to convert source to local source: {:?}", e))?;

    let metadata =
        RawMetadata::try_from(local_source.source.as_ref().context("missing source")?).await?;
    let schema = Schema::try_from(metadata.table_schema.as_ref()).with_context(|| {
        format!(
            "Unable to encode schema {:?} for source file {:?}",
            metadata.table_schema, source_data
        )
    })?;

    // Explicitly close the temporary file here. This ensures that
    // the temporary file cleaner on different OS's know to not remove
    // the file at least until this point.
    temp_file.close()?;

    Ok(SourceMetadata {
        schema: Some(schema),
    })
}

#[cfg(test)]
mod tests {
    use sparrow_api::kaskada::v1alpha::source_data;

    use super::*;

    #[tokio::test]
    async fn test_timestamp_with_timezone_gets_metadata() {
        // Regression test for handling files with a timestamp with a configured
        // timezone.
        let path = sparrow_testing::testdata_path("eventdata/sample_event_data.parquet");

        let s3_helper = S3Helper::new().await;
        let file_service = FileServiceImpl::new(s3_helper);

        // TODO: We may be able to clean this up to test the metadata logic
        // without creating the S3 helper. But for now, this will suffice.
        let result = file_service
            .get_metadata(tonic::Request::new(GetMetadataRequest {
                source_data: Some(SourceData {
                    source: Some(source_data::Source::ParquetPath(
                        path.canonicalize().unwrap().to_string_lossy().to_string(),
                    )),
                }),
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
        let file_service = FileServiceImpl::new(s3_helper);

        // TODO: We may be able to clean this up to test the metadata logic
        // without creating the S3 helper. But for now, this will suffice.
        let result = file_service
            .get_metadata(tonic::Request::new(GetMetadataRequest {
                source_data: Some(SourceData {
                    source: Some(source_data::Source::CsvPath(
                        path.canonicalize().unwrap().to_string_lossy().to_string(),
                    )),
                }),
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
        let file_service = FileServiceImpl::new(s3_helper);
        let result = file_service
            .get_metadata(tonic::Request::new(GetMetadataRequest {
                source_data: Some(SourceData {
                    source: Some(source_data::Source::CsvData(csv_data.to_owned())),
                }),
            }))
            .await
            .unwrap()
            .into_inner();

        insta::assert_yaml_snapshot!(result);
    }
}
