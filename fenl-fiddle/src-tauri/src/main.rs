// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use sparrow_api::kaskada::v1alpha::Schema;
use sparrow_api::kaskada::v1alpha::{
    GetMetadataRequest, GetMetadataResponse, MergeMetadataRequest, MergeMetadataResponse,
    SourceData, SourceMetadata,
};

use sparrow_runtime::RawMetadata;

use sparrow_runtime::stores::ObjectStoreRegistry;
use tonic::Response;

// Learn more about Tauri commands at https://tauri.app/v1/guides/features/command
#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[tauri::command]
fn get_schema(csv: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", csv)
}


fn main() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![greet])
        .invoke_handler(tauri::generate_handler![get_schema])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

