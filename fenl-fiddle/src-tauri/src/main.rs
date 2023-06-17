// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::fs::File;
use std::sync::{Arc, Mutex};
use tauri::State;

use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::{StreamExt, TryStreamExt};
use sparrow_api::kaskada::v1alpha::destination;
use sparrow_api::kaskada::v1alpha::source_data;
use sparrow_api::kaskada::v1alpha::source_data::Source::CsvData;
use sparrow_api::kaskada::v1alpha::Destination;
use sparrow_api::kaskada::v1alpha::Schema;
use sparrow_api::kaskada::v1alpha::SourceData;
use sparrow_qfr::kaskada::sparrow::v1alpha::FlightRecordHeader;
use sparrow_runtime::s3::S3Helper;
use sparrow_runtime::PreparedMetadata;
use sparrow_runtime::RawMetadata;

use sparrow_runtime::stores::ObjectStoreRegistry;

use serde::{Deserialize, Serialize};
use sparrow_api::kaskada::v1alpha::compile_request::ExpressionKind;
use sparrow_api::kaskada::v1alpha::{
    compute_table, CompileRequest, CompileResponse, ComputePlan, ComputeTable, ExecuteRequest,
    FeatureSet, FenlDiagnostics, FileType, ObjectStoreDestination, PerEntityBehavior, TableConfig,
    TableMetadata,
};
use sparrow_compiler::InternalCompileOptions;
use tempfile::NamedTempFile;
use uuid::Uuid;

#[derive(Default)]
struct TableSchema(Mutex<Option<Schema>>);

#[derive(Default)]
struct CompileResult(Mutex<Option<CompileResponse>>);

// Learn more about Tauri commands at https://tauri.app/v1/guides/features/command
#[tauri::command(async)]
async fn get_schema(csv: String, table_schema: State<'_, TableSchema>) -> Result<Schema, String> {
    let object_store_registry = Arc::new(ObjectStoreRegistry::new());

    let source_data = SourceData {
        source: Some(CsvData(csv.clone())),
    };
    let result = get_source_metadata(&object_store_registry, &source_data)
        .await
        .unwrap();
    *table_schema.0.lock().unwrap() = Some(result.clone());
    Ok(result)
}

#[tauri::command(async)]
async fn compile(
    expression: String,
    table_name: String,
    entity_column_name: String,
    time_column_name: String,
    table_schema: State<'_, TableSchema>,
    compile_result: State<'_, CompileResult>,
) -> Result<CompileResponse, String> {
    let schema = table_schema.0.lock().unwrap().clone();
    let result = compile_expression(
        expression,
        table_name,
        entity_column_name,
        time_column_name,
        schema,
    )
    .await
    .unwrap();
    *compile_result.0.lock().unwrap() = Some(result.clone());
    Ok(result)
}

#[tauri::command(async)]
async fn compute(
    csv: String,
    table_name: String,
    entity_column_name: String,
    time_column_name: String,
    compile_result: State<'_, CompileResult>,
) -> Result<String, String> {
    let compile_response = compile_result.0.lock().unwrap().clone();

    let plan;
    match compile_response {
        Some(response) => plan = response.plan,
        None => return Err(Error::CompileQuery.to_string()),
    }

    let result = compute_expression(
        Some(csv),
        table_name,
        entity_column_name,
        time_column_name,
        plan,
    )
    .await
    .unwrap();
    Ok(result)
}

#[derive(derive_more::Display, Debug)]
enum Error {
    #[display(fmt = "unable to get source path from request")]
    SourcePath,
    #[display(fmt = "schema error: '{_0}'")]
    Schema(String),
    #[display(fmt = "failed to prepare input")]
    PrepareInput,
    #[display(fmt = "failed to compile query")]
    CompileQuery,
    #[display(fmt = "failed to compile query:\n{_0}")]
    QueryErrors(FenlDiagnostics),
    #[display(fmt = "failed to execute query")]
    ExecuteQuery,
}
impl error_stack::Context for Error {}

async fn get_source_metadata(
    object_store_registry: &ObjectStoreRegistry,
    source: &SourceData,
) -> error_stack::Result<Schema, Error> {
    let source = source.source.as_ref().ok_or(Error::SourcePath)?;
    let metadata = RawMetadata::try_from(source, object_store_registry)
        .await
        .attach_printable_lazy(|| format!("Source: {:?}", source))
        .change_context(Error::Schema(format!(
            "unable to read schema from: {:?}",
            source
        )))?;
    let schema = Schema::try_from(metadata.table_schema.as_ref())
        .into_report()
        .attach_printable_lazy(|| {
            format!(
                "Raw Schema: {:?} Table Schema: {:?}",
                metadata.raw_schema, metadata.table_schema
            )
        })
        .change_context(Error::Schema(format!(
            "Unable to encode schema {:?} for source file {:?}",
            metadata.table_schema, source
        )))?;
    Ok(schema)
}

async fn compile_expression(
    expression: String,
    table_name: String,
    entity_column_name: String,
    time_column_name: String,
    table_schema: Option<Schema>,
) -> error_stack::Result<CompileResponse, Error> {
    let table1 = ComputeTable {
        config: Some(TableConfig::new_with_table_source(
            &table_name,
            &Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap(),
            &time_column_name,
            None,
            &entity_column_name,
            &table_name,
        )),
        file_sets: vec![],
        metadata: Some(TableMetadata {
            schema: table_schema,
            file_count: 0,
        }),
    };

    let result = sparrow_compiler::compile_proto(
        CompileRequest {
            tables: vec![table1],
            feature_set: Some(FeatureSet {
                formulas: vec![],
                query: expression,
            }),
            slice_request: None,
            expression_kind: ExpressionKind::Complete as i32,
            experimental: false,
            per_entity_behavior: PerEntityBehavior::All as i32,
        },
        InternalCompileOptions::default(),
    )
    .await
    .change_context(Error::CompileQuery)?;
    Ok(result)
}

async fn compute_expression(
    csv: Option<String>,
    table_name: String,
    entity_column_name: String,
    time_column_name: String,
    compute_plan: Option<ComputePlan>,
) -> error_stack::Result<String, Error> {
    let s3_helper = S3Helper::new().await;

    // 1. Prepare the file
    let mut preparer = ExampleInputPreparer::new();
    let tables = preparer
        .prepare_inputs(
            &csv,
            table_name,
            entity_column_name,
            time_column_name,
            vec![],
        )
        .await?;

    let tempdir = tempfile::Builder::new()
        .prefix("example")
        .tempdir()
        .unwrap();

    let destination = ObjectStoreDestination {
        output_prefix_uri: format!("file:///{}", tempdir.path().display()),
        file_type: FileType::Csv.into(),
        output_paths: None,
    };
    let output_to = Destination {
        destination: Some(destination::Destination::ObjectStore(destination)),
    };

    let stream = sparrow_runtime::execute::execute(
        ExecuteRequest {
            plan: compute_plan,
            tables,
            destination: Some(output_to),
            limits: None,
            compute_snapshot_config: None,
            changed_since: None,
            final_result_time: None,
        },
        s3_helper,
        None,
        None,
        FlightRecordHeader::default(),
    )
    .await
    .change_context(Error::ExecuteQuery)?;

    let output_paths = stream
        .map_ok(|item| item.output_paths().unwrap_or_default())
        .try_concat()
        .await
        .unwrap();

    assert_eq!(
        output_paths.len(),
        1,
        "Expected one output, but got {output_paths:?}"
    );
    assert!(
        output_paths[0].starts_with("file:///"),
        "expected local file prefix"
    );
    let output_path = output_paths[0].strip_prefix("file://").unwrap();
    let output_path = std::path::Path::new(output_path);

    // Drop the first four (key columns).
    //
    // Note: This currently writes to CSV and then parses it.
    // There may be faster ways, but this lets us re-use the existing functionality
    // to write to CSV rather than creating special functionality just for examples.
    // We could also consider other options for removing the rows (regex, etc.)
    // but this works.
    let mut table = prettytable::Table::from_csv_file(output_path).unwrap();
    for row in table.row_iter_mut() {
        row.remove_cell(0);
        row.remove_cell(0);
        row.remove_cell(0);
        row.remove_cell(0);
    }

    let content =
        String::from_utf8(table.to_csv(Vec::new()).unwrap().into_inner().unwrap()).unwrap();

    tempdir.close().unwrap();

    Ok(content)
}

fn main() {
    tauri::Builder::default()
        .manage(TableSchema(Default::default()))
        .manage(CompileResult(Default::default()))
        .invoke_handler(tauri::generate_handler![get_schema, compile, compute])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

struct ExampleInputPreparer {
    /// Holds the prepared files so they aren't dropped this is.
    prepared_files: Vec<NamedTempFile>,
}

impl ExampleInputPreparer {
    fn new() -> Self {
        Self {
            prepared_files: vec![],
        }
    }

    // TODO: Use the DataFixture to accomplish this?
    async fn prepare_inputs(
        &mut self,
        input_csv: &Option<String>,
        table_name: String,
        entity_column_name: String,
        time_column_name: String,
        tables: Vec<ExampleTable>,
    ) -> error_stack::Result<Vec<ComputeTable>, Error> {
        let mut prepared_tables = Vec::with_capacity(tables.len() + 1);
        if let Some(input_csv) = input_csv {
            prepared_tables.push(
                self.prepare_input(
                    TableConfig::new_with_table_source(
                        &table_name,
                        &Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap(),
                        &time_column_name,
                        None,
                        &entity_column_name,
                        &table_name,
                    ),
                    input_csv,
                )
                .await
                .attach_printable_lazy(|| TableName("Input".to_owned()))?,
            );
        }

        for table in tables {
            let table_config = table.table_config.clone().with_table_source();
            prepared_tables.push(
                self.prepare_input(table_config, &table.input_csv)
                    .await
                    .attach_printable_lazy(|| TableName(table.table_config.name.clone()))?,
            );
        }

        Ok(prepared_tables)
    }

    async fn prepare_input(
        &mut self,
        config: TableConfig,
        input_csv: &str,
    ) -> error_stack::Result<ComputeTable, Error> {
        let sd = SourceData {
            source: Some(source_data::Source::CsvData(input_csv.to_owned())),
        };
        let prepared_batches: Vec<_> =
            sparrow_runtime::prepare::prepared_batches(&sd, &config, &None)
                .await
                .change_context(Error::PrepareInput)?
                .collect::<Vec<_>>()
                .await;

        let (prepared_batch, metadata) = match prepared_batches[0].as_ref() {
            Ok((prepared_batch, metadata)) => (prepared_batch, metadata),
            Err(_) => error_stack::bail!(Error::PrepareInput),
        };

        let prepared_file = tempfile::Builder::new()
            .suffix(".parquet")
            .tempfile()
            .unwrap();
        let output_file = File::create(prepared_file.path()).unwrap();
        let mut output = parquet::arrow::arrow_writer::ArrowWriter::try_new(
            output_file,
            prepared_batch.schema(),
            None,
        )
        .unwrap();
        output.write(prepared_batch).unwrap();
        output.close().unwrap();

        let metadata_file = tempfile::Builder::new()
            .suffix(".parquet")
            .tempfile()
            .unwrap();
        let metadata_output_file = File::create(metadata_file.path()).unwrap();
        let mut metadata_output = parquet::arrow::arrow_writer::ArrowWriter::try_new(
            metadata_output_file,
            metadata.schema(),
            None,
        )
        .unwrap();
        metadata_output.write(metadata).unwrap();
        metadata_output.close().unwrap();

        let prepared_metadata = PreparedMetadata::try_from_local_parquet_path(
            prepared_file.path(),
            metadata_file.path(),
        )
        .into_report()
        .change_context(Error::PrepareInput)?;
        let table_metadata = TableMetadata {
            schema: Some(prepared_metadata.table_schema.as_ref().try_into().unwrap()),
            file_count: 1,
        };
        self.prepared_files.push(prepared_file);
        self.prepared_files.push(metadata_file);

        Ok(ComputeTable {
            config: Some(config),
            file_sets: vec![compute_table::FileSet {
                slice_plan: None,
                prepared_files: vec![prepared_metadata.try_into().unwrap()],
            }],
            metadata: Some(table_metadata),
        })
    }
}

#[derive(derive_more::Display, Debug)]
#[display(fmt = "for table '{_0}'")]
struct TableName(String);

#[derive(Serialize, Deserialize, Debug)]
struct ExampleTable {
    #[serde(flatten)]
    pub table_config: TableConfig,
    pub input_csv: String,
}
