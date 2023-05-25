use std::sync::Arc;

use error_stack::{IntoReport, ResultExt};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use sparrow_api::kaskada::v1alpha::compute_service_server::ComputeService;
use sparrow_api::kaskada::v1alpha::GetMaterializationStatusRequest;
use sparrow_api::kaskada::v1alpha::GetMaterializationStatusResponse;
use sparrow_api::kaskada::v1alpha::StartMaterializationRequest;
use sparrow_api::kaskada::v1alpha::StartMaterializationResponse;
use sparrow_api::kaskada::v1alpha::StopMaterializationRequest;
use sparrow_api::kaskada::v1alpha::StopMaterializationResponse;
use sparrow_api::kaskada::v1alpha::{
    CompileRequest, CompileResponse, ExecuteRequest, ExecuteResponse,
    GetCurrentSnapshotVersionRequest, GetCurrentSnapshotVersionResponse, LongQueryState,
};
use sparrow_compiler::InternalCompileOptions;
use sparrow_instructions::ComputeStore;
use sparrow_qfr::kaskada::sparrow::v1alpha::{flight_record_header, FlightRecordHeader};
use sparrow_runtime::execute::Error;
use sparrow_runtime::s3::{S3Helper, S3Object};
use tempfile::NamedTempFile;
use tonic::{Request, Response, Status};
use tracing::{error, info, Instrument};
use uuid::Uuid;

use crate::serve::error_status::IntoStatus;
use crate::BuildInfo;

pub(super) struct ComputeServiceImpl {
    flight_record_path: &'static Option<S3Object>,
    s3_helper: S3Helper,
    compute_store: Arc<ComputeStore>,
}

impl ComputeServiceImpl {
    pub(super) fn new(
        flight_record_path: &'static Option<S3Object>,
        s3_helper: S3Helper,
        compute_store: ComputeStore,
    ) -> Self {
        Self {
            flight_record_path,
            s3_helper,
            compute_store: Arc::new(compute_store),
        }
    }
}

#[tonic::async_trait]
impl ComputeService for ComputeServiceImpl {
    type ExecuteStream = BoxStream<'static, Result<ExecuteResponse, tonic::Status>>;

    async fn get_current_snapshot_version(
        &self,
        _request: Request<GetCurrentSnapshotVersionRequest>,
    ) -> Result<Response<GetCurrentSnapshotVersionResponse>, Status> {
        Ok(Response::new(GetCurrentSnapshotVersionResponse {
            snapshot_version: ComputeStore::current_version(),
        }))
    }

    async fn compile(
        &self,
        request: Request<CompileRequest>,
    ) -> Result<Response<CompileResponse>, Status> {
        let span = tracing::info_span!("Compile");
        let _enter = span.enter();

        match tokio::spawn(compile_impl(request).in_current_span())
            .in_current_span()
            .await
        {
            Ok(result) => result.into_status(),
            Err(panic) => {
                tracing::error!("Panic during prepare: {panic}");
                Err(tonic::Status::internal("panic during prepare"))
            }
        }
    }

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        let span = tracing::info_span!("Execute");
        let _enter = span.enter();

        let handle = tokio::spawn(
            execute_impl(
                self.flight_record_path,
                self.s3_helper.clone(),
                request.into_inner(),
            )
            .in_current_span(),
        );
        match handle.in_current_span().await {
            Ok(result) => {
                let stream = result.into_status()?;
                Ok(Response::new(Box::pin(stream)))
            }
            Err(panic) => {
                tracing::error!("Panic during prepare: {panic}");
                Err(tonic::Status::internal("panic during prepare"))
            }
        }
    }

    async fn start_materialization(
        &self,
        request: Request<StartMaterializationRequest>,
    ) -> Result<Response<StartMaterializationResponse>, Status> {
        let span = tracing::info_span!("StartMaterialization");
        let _enter = span.enter();
        let compute_store = self.compute_store.clone();
        let s3_helper = self.s3_helper.clone();

        let _ = tokio::spawn({
            // All errors and progress are internally stored, and can be queried
            // for via the `get_materialization_status` endpoint.
            materialize_impl(compute_store, s3_helper, request.into_inner()).in_current_span()
        });

        Ok(Response::new(StartMaterializationResponse {}))
    }

    async fn stop_materialization(
        &self,
        _request: Request<StopMaterializationRequest>,
    ) -> Result<Response<StopMaterializationResponse>, Status> {
        Err(tonic::Status::internal("unsupported"))
    }
    async fn get_materialization_status(
        &self,
        _request: Request<GetMaterializationStatusRequest>,
    ) -> Result<Response<GetMaterializationStatusResponse>, Status> {
        Err(tonic::Status::internal("unsupported"))
    }
}

async fn compile_impl(
    request: Request<CompileRequest>,
) -> error_stack::Result<Response<CompileResponse>, sparrow_compiler::Error> {
    // Add a deterministic "query that causes panic" for use in Wren
    // integration tests, etc.
    if request
        .get_ref()
        .feature_set
        .iter()
        .any(|f| &f.query == "__INTERNAL_COMPILE_PANIC__")
    {
        panic!("Panic requested by __INTERNAL_COMPILE_PANIC__ in query")
    }

    let response =
        sparrow_compiler::compile_proto(request.into_inner(), InternalCompileOptions::default())
            .await?;
    Ok(Response::new(response))
}

async fn execute_impl(
    flight_record_path: &'static Option<S3Object>,
    s3_helper: S3Helper,
    request: ExecuteRequest,
) -> error_stack::Result<
    impl Stream<Item = Result<ExecuteResponse, Status>> + Send,
    sparrow_runtime::execute::Error,
> {
    // Create a path for the plan yaml tempfile (if needed).
    // TODO: We could include the plan as part of the flight record proto.
    // Then we'd only need to produce one file rather than plan+flight record.
    let plan_yaml_tempfile = if flight_record_path.is_some() {
        let plan = request.plan.as_ref().ok_or(Error::MissingField("plan"))?;

        let tempfile = tempfile::Builder::new()
            .prefix("plan")
            .suffix(".yaml")
            .tempfile()
            .into_report()
            .change_context(Error::internal_msg("create plan tempfile"))?;

        let writer = tempfile
            .reopen()
            .into_report()
            .change_context(Error::internal_msg("create plan tempfile"))?;
        serde_yaml::to_writer(writer, plan)
            .into_report()
            .change_context(Error::internal_msg("writing plan tempfile"))?;
        info!("Wrote plan yaml to {:?}", tempfile);

        Some(tempfile)
    } else {
        None
    };

    // Create a temp path for the flight record (if needed).
    //
    // The tempfile and its contents will be dropped after execution.
    let flight_record_tempfile = if flight_record_path.is_some() {
        let tempfile = tempfile::Builder::new()
            .prefix("flight_record")
            .suffix(".qfr")
            .tempfile()
            .into_report()
            .change_context(Error::internal_msg("create flight record tempfile"))?;
        Some(tempfile)
    } else {
        None
    };
    let flight_record_local_path = flight_record_tempfile.as_ref().map(|t| t.path().to_owned());

    let build_info = BuildInfo::default();
    let flight_record_header = FlightRecordHeader::with_registrations(
        // Set the actual request ID.
        "todo_set_request_id".to_owned(),
        flight_record_header::BuildInfo {
            sparrow_version: build_info.sparrow_version.to_owned(),
            github_ref: build_info.github_ref.to_owned(),
            github_sha: build_info.github_sha.to_owned(),
            github_workflow: build_info.github_workflow.to_owned(),
        },
    );

    let progress_stream = sparrow_runtime::execute::execute(
        request,
        s3_helper.clone(),
        None,
        flight_record_local_path,
        flight_record_header,
    )
    .await?;

    Ok(progress_stream
        .chain(futures::stream::once(debug_message(
            s3_helper,
            flight_record_path,
            plan_yaml_tempfile,
            flight_record_tempfile,
        )))
        .map(|item| item.into_status()))
}

async fn materialize_impl(
    compute_store: Arc<ComputeStore>,
    s3_helper: S3Helper,
    request: StartMaterializationRequest,
) {
    let id = request.materialization_id.clone();
    let result = sparrow_runtime::execute::materialize(
        request,
        s3_helper.clone(),
        None, // TODO: Accept configurable lateness
        compute_store.clone(),
    )
    .await;

    match result {
        Ok(_) => {
            tracing::error!("Materialization exited unexpectedly");
        }
        Err(e) => {
            tracing::error!("Materialization failed: {:?}", e);
            match compute_store.update_materialization_error(&id, &e.to_string()) {
                Err(e) => {
                    tracing::error!("Failed to update materialization error message: {:?}", e);
                }
                _ => (),
            }
        }
    }
}

/// Sends the debug message after the end of the stream.
///
/// Upload the flight record files (plan yaml and flight record),
/// compute snapshots (if applicable), and marks this as the final message.
async fn debug_message(
    s3_helper: S3Helper,
    flight_record_path: &'static Option<S3Object>,
    plan_yaml_tempfile: Option<NamedTempFile>,
    flight_record_tempfile: Option<NamedTempFile>,
) -> error_stack::Result<ExecuteResponse, Error> {
    let diagnostic_id = Uuid::new_v4();

    let uploaded_plan_yaml_path = upload_flight_record_file(
        &s3_helper,
        flight_record_path,
        plan_yaml_tempfile,
        DiagnosticFile::PlanYaml,
        &diagnostic_id,
    );
    let uploaded_flight_record_path = upload_flight_record_file(
        &s3_helper,
        flight_record_path,
        flight_record_tempfile,
        DiagnosticFile::FlightRecord,
        &diagnostic_id,
    );
    // Wait for all futures to complete
    let uploaded_plan_yaml_path = uploaded_plan_yaml_path.await.unwrap_or_else(|e| {
        error!("Failed to plan yaml: {:?}", e);
        None
    });
    let uploaded_flight_record_path = uploaded_flight_record_path.await.unwrap_or_else(|e| {
        error!("Failed to upload flight record: {:?}", e);
        None
    });

    Ok(ExecuteResponse {
        state: LongQueryState::Final as i32,
        is_query_done: true,
        progress: None,
        destination: None,
        flight_record_path: uploaded_flight_record_path,
        plan_yaml_path: uploaded_plan_yaml_path,
        compute_snapshots: Vec::new(),
    })
}

#[derive(Debug)]
enum DiagnosticFile {
    PlanYaml,
    FlightRecord,
}

impl DiagnosticFile {
    fn file_name(&self, diagnostic_id: &Uuid) -> String {
        match self {
            DiagnosticFile::PlanYaml => format!("{}_plan.yaml", diagnostic_id.as_hyphenated()),
            DiagnosticFile::FlightRecord => {
                format!("{}_record.qfr", diagnostic_id.as_hyphenated())
            }
        }
    }
}

async fn upload_flight_record_file<'a>(
    s3_helper: &'a S3Helper,
    flight_record_path: &'static Option<S3Object>,
    tempfile: Option<NamedTempFile>,
    kind: DiagnosticFile,
    diagnostic_id: &'a Uuid,
) -> anyhow::Result<Option<String>> {
    let tempfile = if let Some(tempfile) = tempfile {
        tempfile
    } else {
        info!("No diagnostic to upload for kind {:?}", kind);
        return Ok(None);
    };

    let path = if let Some(prefix) = flight_record_path {
        prefix.join_delimited(&kind.file_name(diagnostic_id))
    } else {
        info!("No diagnostic prefix -- not uploading {:?}", kind);
        return Ok(None);
    };

    let destination = path.get_formatted_key();
    s3_helper
        .upload_tempfile_to_s3(path, tempfile.into_temp_path())
        .await?;
    info!(
        "Uploaded {:?}. To retrieve: `s3 cp {} .`",
        kind, destination
    );
    Ok(Some(destination))
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::Path;

    use futures::TryStreamExt;
    use itertools::Itertools;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use sparrow_api::kaskada::v1alpha::compile_request::ExpressionKind;
    use sparrow_api::kaskada::v1alpha::execute_request::Limits;
    use sparrow_api::kaskada::v1alpha::{
        compute_table, destination, slice_plan, source_data, ComputeTable, FeatureSet, FileType,
        ObjectStoreDestination, PerEntityBehavior, SlicePlan, TableConfig, TableMetadata,
    };
    use sparrow_api::kaskada::v1alpha::{data_type, schema, DataType, Schema};
    use sparrow_api::kaskada::v1alpha::{slice_request, SliceRequest};
    use sparrow_api::kaskada::v1alpha::{Destination, SourceData};
    use sparrow_runtime::prepare::{file_sourcedata, prepared_batches};
    use sparrow_runtime::stores::ObjectStoreRegistry;
    use sparrow_runtime::{PreparedMetadata, RawMetadata};

    use super::*;

    fn analyze_input_schema() -> Schema {
        Schema {
            fields: vec![
                schema::Field {
                    name: "time".to_owned(),
                    data_type: Some(DataType {
                        kind: Some(data_type::Kind::Primitive(
                            data_type::PrimitiveType::TimestampNanosecond as i32,
                        )),
                    }),
                },
                schema::Field {
                    name: "subsort".to_owned(),
                    data_type: Some(DataType {
                        kind: Some(data_type::Kind::Primitive(
                            data_type::PrimitiveType::I32 as i32,
                        )),
                    }),
                },
                schema::Field {
                    name: "entity".to_owned(),
                    data_type: Some(DataType {
                        kind: Some(data_type::Kind::Primitive(
                            data_type::PrimitiveType::String as i32,
                        )),
                    }),
                },
                schema::Field {
                    name: "str".to_owned(),
                    data_type: Some(DataType {
                        kind: Some(data_type::Kind::Primitive(
                            data_type::PrimitiveType::String as i32,
                        )),
                    }),
                },
            ],
        }
    }

    #[tokio::test]
    async fn test_analyze_valid_query_experimental_incremental() {
        sparrow_testing::init_test_logging();

        let result = compile_impl(tonic::Request::new(CompileRequest {
            tables: vec![ComputeTable {
                config: Some(TableConfig::new_with_table_source(
                    "Table1",
                    &Uuid::new_v4(),
                    "time",
                    Some("subsort"),
                    "entity",
                    "grouping",
                )),
                file_sets: vec![],
                metadata: Some(TableMetadata {
                    schema: Some(analyze_input_schema()),
                    file_count: 0,
                }),
            }],
            feature_set: Some(FeatureSet {
                formulas: vec![],
                query: "{x: Table1.str as i64, y: Table1.str }".to_owned(),
            }),
            slice_request: None,
            expression_kind: ExpressionKind::Complete as i32,
            experimental: true,
            per_entity_behavior: PerEntityBehavior::Final as i32,
        }))
        .await
        .unwrap();

        assert!(result.get_ref().incremental_enabled)
    }

    #[tokio::test]
    async fn test_compile_valid_query_experimental_lag() {
        sparrow_testing::init_test_logging();

        let result = compile_impl(tonic::Request::new(CompileRequest {
            tables: vec![ComputeTable {
                config: Some(TableConfig::new_with_table_source(
                    "Table1",
                    &Uuid::new_v4(),
                    "time",
                    Some("subsort"),
                    "entity",
                    "grouping",
                )),
                file_sets: vec![],
                metadata: Some(TableMetadata {
                    schema: Some(analyze_input_schema()),
                    file_count: 0,
                }),
            }],
            feature_set: Some(FeatureSet {
                formulas: vec![],
                query: "{x: Table1.str as i64, y: lag(Table1.str, 1) }".to_owned(),
            }),
            slice_request: None,
            expression_kind: ExpressionKind::Complete as i32,
            experimental: true,
            per_entity_behavior: PerEntityBehavior::Final as i32,
        }))
        .await
        .unwrap();

        assert!(!result.get_ref().incremental_enabled)
    }

    #[tokio::test]
    async fn test_compile_valid_fragment() {
        sparrow_testing::init_test_logging();

        let result = compile_impl(tonic::Request::new(CompileRequest {
            tables: vec![ComputeTable {
                config: Some(TableConfig::new_with_table_source(
                    "Table1",
                    &Uuid::new_v4(),
                    "time",
                    Some("subsort"),
                    "entity",
                    "grouping",
                )),
                file_sets: vec![],
                metadata: Some(TableMetadata {
                    schema: Some(analyze_input_schema()),
                    file_count: 0,
                }),
            }],
            feature_set: Some(FeatureSet {
                formulas: vec![],
                query: "Table1.str as i64".to_owned(),
            }),
            slice_request: None,
            expression_kind: ExpressionKind::Formula as i32,
            experimental: false,
            per_entity_behavior: PerEntityBehavior::All as i32,
        }))
        .await
        .unwrap();

        insta::assert_yaml_snapshot!(result.get_ref(), @r###"
        ---
        missing_names: []
        fenl_diagnostics:
          fenl_diagnostics: []
          num_errors: 0
        plan: ~
        result_type:
          kind:
            Primitive: 6
        free_names:
          - Table1
        table_slices:
          - table_name: Table1
            slice: ~
        incremental_enabled: false
        plan_hash: ~
        "###)
    }

    // TODO: Move slice tests to e2e test crate
    #[tokio::test]
    async fn test_sliced_query_smoke() {
        sparrow_testing::init_test_logging();

        // Runs a query using the `stream_query_impl`. This is the logic behind
        // the `StreamQuery` RPC method. The purpose of this is to ensure the
        // code around the query execution (which is more heavily tested) is
        // properly hooked up. The actual integration is also tested more in
        // the Wren integration tests -- this is more of a smoke test within
        // the Rust code for easier iteration and debugging.
        //
        // This version is a bit odd -- it actually specifies the file as a
        // 50% slice, and sends a query for that 50% slice. The goal is to
        // make sure that the sliced file set is properly used.

        let file_path = "eventdata/event_data.parquet";
        let s3_helper = S3Helper::new().await;
        let part1_file_path = sparrow_testing::testdata_path(file_path);
        let table = TableConfig::new_with_table_source(
            "Events",
            &Uuid::new_v4(),
            "timestamp",
            Some("subsort_id"),
            "anonymousId",
            "user",
        );

        let slice_plan = SlicePlan {
            table_name: "Events".to_owned(),
            slice: Some(slice_plan::Slice::Percent(slice_plan::PercentSlice {
                percent: 50.0,
            })),
        };
        let input_path = SourceData::try_from_local(&part1_file_path).unwrap();
        let prepared_batches: Vec<_> = prepared_batches(
            &file_sourcedata(input_path),
            &table,
            &Some(slice_plan::Slice::Percent(slice_plan::PercentSlice {
                percent: 50.0,
            })),
        )
        .await
        .unwrap()
        .collect()
        .await;

        let part1_file_path = part1_file_path
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .to_string();

        let part1_file_path = format!("file://{part1_file_path}");
        let input_path = source_data::Source::ParquetPath(part1_file_path);
        let object_store_registry = ObjectStoreRegistry::new();
        let part1_metadata = RawMetadata::try_from(&input_path, &object_store_registry)
            .await
            .unwrap();
        let schema = Schema::try_from(part1_metadata.table_schema.as_ref()).unwrap();

        debug_assert_eq!(prepared_batches.len(), 1);

        let prepared_file = tempfile::Builder::new()
            .suffix(".parquet")
            .tempfile()
            .unwrap();

        let metadata_file = tempfile::Builder::new()
            .suffix(".parquet")
            .tempfile()
            .unwrap();

        let (record_batch, metadata) = prepared_batches[0].as_ref().unwrap();
        let output_file = File::create(&prepared_file).unwrap();
        let metadata_output_file = File::create(&metadata_file).unwrap();

        if record_batch.num_rows() > 0 {
            let mut output = parquet::arrow::arrow_writer::ArrowWriter::try_new(
                output_file,
                record_batch.schema(),
                None,
            )
            .unwrap();
            output.write(record_batch).unwrap();
            output.close().unwrap();
        }

        if metadata.num_rows() > 0 {
            let mut output = parquet::arrow::arrow_writer::ArrowWriter::try_new(
                metadata_output_file,
                metadata.schema(),
                None,
            )
            .unwrap();
            output.write(metadata).unwrap();
            output.close().unwrap();
        }

        let prepared_metadata = PreparedMetadata::try_from_local_parquet_path(
            prepared_file.path(),
            metadata_file.path(),
        )
        .unwrap();
        let file_set = compute_table::FileSet {
            slice_plan: Some(slice_plan),
            prepared_files: vec![prepared_metadata.try_into().unwrap()],
        };

        let output_dir = tempfile::TempDir::new().unwrap();

        let compile_response = compile_impl(tonic::Request::new(CompileRequest {
            tables: vec![ComputeTable {
                config: Some(table.clone()),
                metadata: Some(TableMetadata {
                    schema: Some(schema.clone()),
                    file_count: 1,
                }),
                file_sets: vec![],
            }],
            feature_set: Some(FeatureSet {
                formulas: vec![],
                query: "Events".to_owned(),
            }),
            slice_request: Some(SliceRequest {
                slice: Some(slice_request::Slice::Percent(slice_request::PercentSlice {
                    percent: 50.0,
                })),
            }),
            expression_kind: ExpressionKind::Complete as i32,
            experimental: false,
            per_entity_behavior: PerEntityBehavior::All as i32,
        }))
        .await
        .unwrap()
        .into_inner();

        assert!(compile_response.plan.is_some());

        let store = ObjectStoreDestination {
            file_type: FileType::Parquet as i32,
            output_prefix_uri: format!("file:///{}", output_dir.path().display()),
            output_paths: None,
        };
        let output_to = Destination {
            destination: Some(destination::Destination::ObjectStore(store)),
        };

        let mut results: Vec<ExecuteResponse> = execute_impl(
            &None,
            s3_helper,
            ExecuteRequest {
                plan: compile_response.plan,
                tables: vec![ComputeTable {
                    config: Some(table),
                    metadata: Some(TableMetadata {
                        schema: Some(schema),
                        file_count: 1,
                    }),
                    file_sets: vec![file_set],
                }],
                destination: Some(output_to),
                // These are weird. Wren doesn't send "no limits" and "no query hash"
                // when their missing. Instead, it sends the defaults.
                limits: Some(Limits::default()),
                compute_snapshot_config: None,
                changed_since: None,
                final_result_time: None,
            },
        )
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

        // First, check the returned parquet files.
        let file = results
            .iter()
            .filter_map(|part| part.output_paths().as_ref().cloned())
            .flatten()
            .exactly_one()
            .unwrap();
        let file = file.strip_prefix("file://").unwrap();

        // Check the number of rows.
        let file = Path::new(&file);
        let file = File::open(file).unwrap();

        let reader = SerializedFileReader::new(file).unwrap();

        let parquet_metadata = reader.metadata();
        let parquet_metadata = parquet_metadata.file_metadata();
        assert_eq!(parquet_metadata.num_rows(), 54_068);
        assert_eq!(parquet_metadata.schema_descr().num_columns(), 11);

        // Second, redact the output paths and check the response.
        for result in results.iter_mut() {
            if let Some(output) = &mut result.destination {
                if let Some(destination::Destination::ObjectStore(store)) = &mut output.destination
                {
                    store.output_prefix_uri = "<redacted_output_prefix_uri>".to_owned()
                }
            };

            let result_paths = result.output_paths_mut();
            if let Some(result_paths) = result_paths {
                for output_path in result_paths.paths.iter_mut() {
                    // assert that the path is in the temp directory
                    let output_file = Path::new(output_path);
                    assert!(
                        output_file.starts_with(&output_path),
                        "Expected '{output_file:?}' to be in '{output_path:?}'"
                    );

                    *output_path = "<redacted_output_path>".to_owned();
                }
            }
        }

        insta::assert_yaml_snapshot!(results);
    }
}
