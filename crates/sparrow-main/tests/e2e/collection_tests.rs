//! e2e tests for the collection operators.

use crate::execute::operation::testing::{batch_from_json, run_operation_json};
use arrow::{datatypes::DataType, record_batch::RecordBatch};
use sparrow_api::kaskada::v1alpha::TableConfig;
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
            {"_time": 2000, "_subsort": 0, "_key_hash": 1, "e0": {"f1": 0, "f2": 22},  "e1": 1,  "e2": 2.7}
            {"_time": 3000, "_subsort": 1, "_key_hash": 1, "e0": {"f2": 10},           "e1": 2,  "e2": 3.8}
            {"_time": 3000, "_subsort": 2, "_key_hash": 1, "e0": {"f1": 5},            "e1": 42, "e2": 4.0}
            {"_time": 3000, "_subsort": 3, "_key_hash": 1, "e0": {"f1": 10, "f2": 13}, "e1": 42, "e2": null}
            {"_time": 4000, "_subsort": 0, "_key_hash": 1, "e0": {"f1": 15, "f3": 11}, "e1": 3,  "e2": 7}
            "#;
    input
}

fn batch_from_json(json: &str, column_types: Vec<DataType>) -> anyhow::Result<RecordBatch> {
    // Trim trailing/leading whitespace on each line.
    let json = json.lines().map(|line| line.trim()).join("\n");

    // Determine the schema.
    let schema = {
        let mut fields = vec![
            Field::new(
                "_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("_subsort", DataType::UInt64, false),
            Field::new("_key_hash", DataType::UInt64, false),
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

async fn create_temp_parquet(json_input: &str) -> &str {
    let batch = batch_from_json(json_input).unwrap();
    let schema = record_batch.schema();

    // Create a Parquet writer
    let file = File::create(file_path)?;
    let props = Arc::new(WriterProperties::builder().build());
    let mut writer = FileWriter::new(file, schema.clone(), props)?;

    // Write the record batch to the Parquet file
    writer.write(&record_batch)?;

    // Close the writer to finish writing the file
    writer.close()?;

    Ok(())
}

#[tokio::test]
async fn test_get_static_key() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: get(Input.map, \"f1\") }").run_to_csv(&collection_data_fixture().await).await.unwrap(), @r###"
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
