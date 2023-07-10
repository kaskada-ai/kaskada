//! e2e tests for the collection operators.

use std::{fs::File, path::PathBuf, sync::Arc};

use anyhow::Context;
use arrow::{
    datatypes::{DataType, Field, Fields, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use itertools::Itertools;
use parquet::arrow::ArrowWriter;
use sparrow_api::kaskada::v1alpha::TableConfig;
use tempfile::NamedTempFile;
use uuid::Uuid;

use crate::{DataFixture, QueryFixture};

/// Fixture for testing collection operations.
async fn invalid_map_type_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_files(
            TableConfig::new_with_table_source("Input", &Uuid::new_v4(), "time", None, "name", ""),
            &["parquet/data_with_invalid_map_types.parquet"],
        )
        .await
        .unwrap()
}

/// Fixture for testing collection operations.
async fn collection_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_files(
            TableConfig::new_with_table_source("Input", &Uuid::new_v4(), "time", None, "name", ""),
            &["parquet/data_with_map.parquet"],
        )
        .await
        .unwrap()
}

async fn json_input() -> &'static str {
    let input = r#"
            {"time": 2000, "key": 1, "e0": {"f1": 0,  "f2": 22},   "e1": 1,  "e2": 2.7,  "e3": "f1" }
            {"time": 3000, "key": 1, "e0": {"f1": 1,  "f2": 10},   "e1": 2,  "e2": 3.8,  "e3": "f2" }
            {"time": 3000, "key": 1, "e0": {"f1": 5,  "f2": 3},    "e1": 42, "e2": 4.0,  "e3": "f3" }
            {"time": 3000, "key": 1, "e0": {"f2": 13},             "e1": 42, "e2": null, "e3": "f2" }
            {"time": 4000, "key": 1, "e0": {"f1": 15, "f3": 11},   "e1": 3,  "e2": 7,    "e3": "f3" }
            "#;
    input
}

#[tokio::test]
async fn test_get_static_key() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: get(Input.e0, \"f1\") }").run_to_csv(&arrow_collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1970-01-01T00:00:00.000002000,7673491505576241514,2359047937476779835,1,0
    1970-01-01T00:00:00.000003000,7673491505576241515,2359047937476779835,1,1
    1970-01-01T00:00:00.000003000,7673491505576241516,2359047937476779835,1,5
    1970-01-01T00:00:00.000003000,7673491505576241517,2359047937476779835,1,
    1970-01-01T00:00:00.000004000,7673491505576241518,2359047937476779835,1,15
    "###);
}

#[tokio::test]
async fn test_get_static_key_second_field() {
    insta::assert_snapshot!(QueryFixture::new("{ f2: \"f2\" | get(Input.e0) }").run_to_csv(&arrow_collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f2
    1970-01-01T00:00:00.000002000,7673491505576241514,2359047937476779835,1,22
    1970-01-01T00:00:00.000003000,7673491505576241515,2359047937476779835,1,10
    1970-01-01T00:00:00.000003000,7673491505576241516,2359047937476779835,1,3
    1970-01-01T00:00:00.000003000,7673491505576241517,2359047937476779835,1,13
    1970-01-01T00:00:00.000004000,7673491505576241518,2359047937476779835,1,
    "###);
}

#[tokio::test]
async fn test_get_dynamic_key() {
    insta::assert_snapshot!(QueryFixture::new("{ value: Input.e3 | get(Input.e0) }").run_to_csv(&arrow_collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,value
    1970-01-01T00:00:00.000002000,1642954577267095822,2359047937476779835,1,0
    1970-01-01T00:00:00.000003000,1642954577267095823,2359047937476779835,1,10
    1970-01-01T00:00:00.000003000,1642954577267095824,2359047937476779835,1,
    1970-01-01T00:00:00.000003000,1642954577267095825,2359047937476779835,1,13
    1970-01-01T00:00:00.000004000,1642954577267095826,2359047937476779835,1,11
    "###);
}

fn batch_from_json(json: &str, column_types: Vec<DataType>) -> anyhow::Result<RecordBatch> {
    // Trim trailing/leading whitespace on each line.
    let json = json.lines().map(|line| line.trim()).join("\n");

    // Determine the schema.
    let schema = {
        let mut fields = vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("key", DataType::UInt64, false),
        ];

        fields.extend(
            column_types
                .into_iter()
                .enumerate()
                .map(|(index, data_type)| Field::new(format!("e{index}"), data_type, true)),
        );

        Arc::new(Schema::new(fields))
    };

    // Create the reader
    let reader = std::io::Cursor::new(json.as_bytes());
    let reader = arrow::json::ReaderBuilder::new(schema).build(reader)?;

    // Read all the batches and concatenate them.
    let batches: Vec<_> = reader.try_collect()?;
    let read_schema = batches.get(0).context("no batches read")?.schema();
    let batch = arrow::compute::concat_batches(&read_schema, &batches)
        .context("concatenate read batches")?;

    Ok(batch)
}

async fn json_to_parquet_file(json_input: &str, file: File) {
    let fields = Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]);
    // TODO: need to figure out type equality - if this is changed to "map" field name it fails
    let m1 = Arc::new(Field::new("entries", DataType::Struct(fields), false));
    let column_types = vec![
        DataType::Map(m1, false),
        DataType::Int64,
        DataType::Float64,
        DataType::Utf8,
    ];
    let record_batch = batch_from_json(json_input, column_types).unwrap();

    println!("ORIGINAL RECORD BATCH: {:?}", record_batch);

    // Create a Parquet writer
    let mut writer = ArrowWriter::try_new(file, record_batch.schema(), None).unwrap();
    writer.write(&record_batch).unwrap();

    // Close the writer to finish writing the file
    writer.close().unwrap();
}

async fn arrow_collection_data_fixture() -> DataFixture {
    let input = json_input().await;
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop();
    path.pop();
    path.push("testdata");
    path.push("test.parquet");

    let file = File::create(path).unwrap();
    json_to_parquet_file(input, file).await;

    DataFixture::new()
        .with_table_from_files(
            TableConfig::new_with_table_source("Input", &Uuid::new_v4(), "time", None, "key", ""),
            &[&"test.parquet"],
        )
        .await
        .unwrap()
}
