use std::sync::Arc;

use error_stack::{IntoReport, ResultExt};
use sparrow_api::kaskada::v1alpha::file_service_server::FileService;
use sparrow_api::kaskada::v1alpha::{
    GetMetadataRequest, GetMetadataResponse, MergeMetadataRequest, MergeMetadataResponse,
    SourceData, SourceMetadata,
};
use sparrow_api::kaskada::v1alpha::{KafkaSubscription, PulsarSubscription, Schema};

use sparrow_runtime::RawMetadata;

use sparrow_runtime::stores::ObjectStoreRegistry;
use tonic::Response;

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
    let source = request.source.unwrap();
    let file_metadata = match source {
        sparrow_api::kaskada::v1alpha::get_metadata_request::Source::SourceData(source_data) => {
            get_source_metadata(&object_store_registry, &source_data)
                .await
                .or_else(|e| anyhow::bail!("failed getting source metadata: {}", e))
        }
        sparrow_api::kaskada::v1alpha::get_metadata_request::Source::PulsarSubscription(
            pulsar_data,
        ) => get_pulsar_metadata(&pulsar_data)
            .await
            .or_else(|e| anyhow::bail!("failed getting source metadata: {}", e)),
        sparrow_api::kaskada::v1alpha::get_metadata_request::Source::KafkaSubscription(
            kakfa_data,
        ) => get_kafka_metadata(&kakfa_data)
            .await
            .or_else(|e| anyhow::bail!("failed getting source metadata: {}", e)),
    }?;

    Ok(Response::new(GetMetadataResponse {
        source_metadata: Some(file_metadata),
    }))
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "unable to get source path from request")]
    SourcePath,
    #[display(fmt = "schema error: '{_0}'")]
    Schema(String),
}
impl error_stack::Context for Error {}

pub(crate) async fn get_source_metadata(
    object_store_registry: &ObjectStoreRegistry,
    source: &SourceData,
) -> error_stack::Result<SourceMetadata, Error> {
    let source = source.source.as_ref().ok_or(Error::SourcePath)?;
    let metadata = RawMetadata::try_from(source, object_store_registry)
        .await
        .attach_printable_lazy(|| format!("Source: {:?}", source))
        .change_context(Error::Schema(format!(
            "unable to read schema from: {:?}",
            source
        )))?;
    get_schema_from_metadata(metadata)
}

pub(crate) async fn get_pulsar_metadata(
    ps: &PulsarSubscription,
) -> error_stack::Result<SourceMetadata, Error> {
    let metadata = RawMetadata::try_from_pulsar_subscription(ps)
        .await
        .attach_printable_lazy(|| format!("Pulsar Source: {:?}", ps))
        .change_context(Error::Schema(format!(
            "unable to read schema from: {:?}",
            ps
        )))?;
    get_schema_from_metadata(metadata)
}

pub(crate) async fn get_kafka_metadata(
    ks: &KafkaSubscription,
) -> error_stack::Result<SourceMetadata, Error> {
    let metadata = RawMetadata::try_from_kafka_subscription(ks)
        .await
        .attach_printable_lazy(|| format!("Kafka Source: {:?}", ks))
        .change_context(Error::Schema(format!(
            "unable to read schema from: {:?}",
            ks
        )))?;
    get_schema_from_metadata(metadata)
}

fn get_schema_from_metadata(metadata: RawMetadata) -> error_stack::Result<SourceMetadata, Error> {
    let schema = Schema::try_from(metadata.table_schema.as_ref())
        .into_report()
        .attach_printable_lazy(|| {
            format!(
                "Raw Schema: {:?} Table Schema: {:?}",
                metadata.raw_schema, metadata.table_schema
            )
        })
        .change_context(Error::Schema(format!(
            "Unable to encode schema {:?}",
            metadata.table_schema
        )))?;
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
        let object_store_registry = Arc::new(ObjectStoreRegistry::new());
        let file_service = FileServiceImpl::new(object_store_registry);
        let path = path.canonicalize().unwrap().to_string_lossy().to_string();
        let result = file_service
            .get_metadata(tonic::Request::new(GetMetadataRequest {
                source: Some(
                    sparrow_api::kaskada::v1alpha::get_metadata_request::Source::SourceData(
                        SourceData {
                            source: Some(source_data::Source::ParquetPath(format!(
                                "file:///{path}"
                            ))),
                        },
                    ),
                ),
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
        let path = path.canonicalize().unwrap().to_string_lossy().to_string();
        let result = file_service
            .get_metadata(tonic::Request::new(GetMetadataRequest {
                source: Some(
                    sparrow_api::kaskada::v1alpha::get_metadata_request::Source::SourceData(
                        SourceData {
                            source: Some(source_data::Source::CsvPath(format!("file:///{path}"))),
                        },
                    ),
                ),
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
                source: Some(
                    sparrow_api::kaskada::v1alpha::get_metadata_request::Source::SourceData(
                        SourceData {
                            source: Some(source_data::Source::CsvData(csv_data.to_owned())),
                        },
                    ),
                ),
            }))
            .await
            .unwrap()
            .into_inner();
        insta::assert_yaml_snapshot!(result);
    }
}
