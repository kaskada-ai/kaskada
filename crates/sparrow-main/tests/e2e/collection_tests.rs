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
            {"time": 2000, "key": 1, "e0": {"f1": 0,  "f2": 22},   "e1": 1,  "e2": 2.7}
            {"time": 3000, "key": 1, "e0": {"f1": 1,  "f2": 10},   "e1": 2,  "e2": 3.8}
            {"time": 3000, "key": 1, "e0": {"f1": 5,  "f2": 3},    "e1": 42, "e2": 4.0}
            {"time": 3000, "key": 1, "e0": {"f2": 13},             "e1": 42, "e2": null}
            {"time": 4000, "key": 1, "e0": {"f1": 15, "f3": 11},   "e1": 3,  "e2": 7}
            "#;
    input
}

#[tokio::test]
async fn test_get_static_key() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: get(Input.e0, \"f1\") }").with_dump_dot("namasdfe").run_to_csv(&arrow_collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,eq
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,5,21,0
    1996-12-20T00:39:58.000000000,9223372036854775808,11753611437813598533,B,24,14,0
    1996-12-20T00:39:59.000000000,9223372036854775808,3650215962958587783,A,17,17,1
    1996-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,,20,
    1996-12-20T00:40:01.000000000,9223372036854775808,3650215962958587783,A,12,,
    1996-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,,,
    "###);
}

#[tokio::test]
async fn test_get_data_key() {
    insta::assert_snapshot!(QueryFixture::new("{ value: Input.map | get(Input.key)} }").run_to_csv(&collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,eq
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,5,21,0
    1996-12-20T00:39:58.000000000,9223372036854775808,11753611437813598533,B,24,14,0
    1996-12-20T00:39:59.000000000,9223372036854775808,3650215962958587783,A,17,17,1
    1996-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,,20,
    1996-12-20T00:40:01.000000000,9223372036854775808,3650215962958587783,A,12,,
    1996-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,,,
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
    let m1 = Arc::new(Field::new("map", DataType::Struct(fields), false));
    let column_types = vec![DataType::Map(m1, false), DataType::Int64, DataType::Float64];
    let record_batch = batch_from_json(json_input, column_types).unwrap();

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
