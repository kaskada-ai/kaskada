// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::sync::Arc;

use error_stack::{IntoReport, ResultExt};
use sparrow_api::kaskada::v1alpha::Schema;
use sparrow_api::kaskada::v1alpha::SourceData;
use sparrow_api::kaskada::v1alpha::source_data::Source::CsvData;

use sparrow_runtime::RawMetadata;

use sparrow_runtime::stores::ObjectStoreRegistry;

// Learn more about Tauri commands at https://tauri.app/v1/guides/features/command
#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[tauri::command(async)]
async fn get_schema(csv: String) -> Option<Schema> {
    let object_store_registry = Arc::new(ObjectStoreRegistry::new());

    let source_data = SourceData {
        source: Some(CsvData(csv.clone()))
    };
    let result = get_source_metadata(&object_store_registry, &source_data)
            .await
            .unwrap();
    Some(result)
}


fn main() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![greet])
        .invoke_handler(tauri::generate_handler![get_schema])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");

    
}

#[derive(derive_more::Display, Debug)]
enum Error {
    #[display(fmt = "unable to get source path from request")]
    SourcePath,
    #[display(fmt = "schema error: '{_0}'")]
    Schema(String),
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
