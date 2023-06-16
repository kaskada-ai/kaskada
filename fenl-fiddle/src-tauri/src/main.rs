// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::sync::{Arc, Mutex};
use tauri::State;

use error_stack::{IntoReport, ResultExt};
use sparrow_api::kaskada::v1alpha::Schema;
use sparrow_api::kaskada::v1alpha::SourceData;
use sparrow_api::kaskada::v1alpha::source_data::Source::CsvData;

use sparrow_runtime::RawMetadata;

use sparrow_runtime::stores::ObjectStoreRegistry;

use sparrow_api::kaskada::v1alpha::compile_request::ExpressionKind;
use sparrow_api::kaskada::v1alpha::{
    CompileRequest, CompileResponse, ComputeTable, FeatureSet, FenlDiagnostics,
    PerEntityBehavior, TableConfig, TableMetadata, 
};
use sparrow_compiler::InternalCompileOptions;
use uuid::Uuid;

#[derive(Default)]
struct TableSchema(Mutex<Option<Schema>>);

// Learn more about Tauri commands at https://tauri.app/v1/guides/features/command
#[tauri::command(async)]
async fn get_schema(csv: String, table_schema: State<'_, TableSchema>) -> Result<Schema, String> {
    let object_store_registry = Arc::new(ObjectStoreRegistry::new());

    let source_data = SourceData {
        source: Some(CsvData(csv.clone()))
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
) -> Result<CompileResponse, String> {
    let schema = table_schema.0.lock().unwrap().clone();
    let result = compile_expression(expression, table_name, entity_column_name, time_column_name, schema)
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
    ).await
    .change_context(Error::CompileQuery)?;
    Ok(result)
}

fn main() {
    tauri::Builder::default()
        .manage(TableSchema(Default::default()))
        .invoke_handler(tauri::generate_handler![get_schema, compile])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}