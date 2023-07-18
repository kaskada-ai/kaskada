//! e2e tests for the collection operators.

use std::{fs::File, path::PathBuf, sync::Arc};

use anyhow::Context;
use arrow::{
    array::{Int32Builder, Int64Builder, MapBuilder, StringBuilder},
    datatypes::{DataType, Field, Fields, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use itertools::Itertools;
use parquet::arrow::ArrowWriter;
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::{fixture::DataFixture, QueryFixture};

/// Create a simple table with a collection type (map).
///
/// ```json
/// {"time": "1996-12-19T16:39:57Z", "subsort": 0, "key": 1, "e0": {"f1": 0,  "f2": 22}, "e1": 1,  "e2": 2.7,  "e3": "f1" }
/// {"time": "1996-12-19T16:40:57Z", "subsort": 0, "key": 1, "e0": {"f1": 1,  "f2": 10}, "e1": 2,  "e2": 3.8,  "e3": "f2" }
/// {"time": "1996-12-19T16:40:59Z", "subsort": 0, "key": 1, "e0": {"f1": 5,  "f2": 3},  "e1": 42, "e2": 4.0,  "e3": "f3" }
/// {"time": "1996-12-19T16:41:57Z", "subsort": 0, "key": 1, "e0": {"f2": 13},           "e1": 42, "e2": null, "e3": "f2" }
/// {"time": "1996-12-19T16:42:57Z", "subsort": 0, "key": 1, "e0": {"f1": 15, "f3": 11}, "e1": 3,  "e2": 7,    "e3": "f3" }
/// ```
pub(crate) async fn collection_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_files(
            TableConfig::new_with_table_source(
                "Input",
                &Uuid::new_v4(),
                "time",
                Some("subsort"),
                "key",
                "",
            ),
            &[&"parquet/data_with_map.parquet"],
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_get_static_key() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: get(\"f1\", Input.e0) }").run_to_csv(&arrow_collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,2359047937476779835,1,0
    1996-12-19T16:40:57.000000000,0,2359047937476779835,1,1
    1996-12-19T16:40:59.000000000,0,2359047937476779835,1,5
    1996-12-19T16:41:57.000000000,0,2359047937476779835,1,
    1996-12-19T16:42:57.000000000,0,2359047937476779835,1,15
    "###);
}

#[tokio::test]
async fn test_get_static_key_second_field() {
    insta::assert_snapshot!(QueryFixture::new("{ f2: Input.e0 | get(\"f2\") }").run_to_csv(&collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f2
    1996-12-19T16:39:57.000000000,0,2359047937476779835,1,22
    1996-12-19T16:40:57.000000000,0,2359047937476779835,1,10
    1996-12-19T16:40:59.000000000,0,2359047937476779835,1,3
    1996-12-19T16:41:57.000000000,0,2359047937476779835,1,13
    1996-12-19T16:42:57.000000000,0,2359047937476779835,1,
    "###);
}

#[tokio::test]
async fn test_get_dynamic_key() {
    insta::assert_snapshot!(QueryFixture::new("{ value: Input.e0 | get(Input.e3) }").run_to_csv(&collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,value
    1996-12-19T16:39:57.000000000,0,2359047937476779835,1,0
    1996-12-19T16:40:57.000000000,0,2359047937476779835,1,10
    1996-12-19T16:40:59.000000000,0,2359047937476779835,1,
    1996-12-19T16:41:57.000000000,0,2359047937476779835,1,13
    1996-12-19T16:42:57.000000000,0,2359047937476779835,1,11
    "###);
}

#[tokio::test]
async fn test_swapped_args_for_get_map() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ f1: get(Input.e0, \"f1\") }")
        .run_to_csv(&collection_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:7"
          - "  |"
          - "1 | { f1: get(Input.e0, \"f1\") }"
          - "  |       ^^^           ---- Actual type: string"
          - "  |       |              "
          - "  |       Invalid types for parameter 'map' in call to 'get'"
          - "  |"
          - "  --> built-in signature 'get<K: key, V: any>(key: K, map: map<K, V>) -> V':1:34"
          - "  |"
          - "1 | get<K: key, V: any>(key: K, map: map<K, V>) -> V"
          - "  |                                  --------- Expected type: map<K, V>"
          - ""
          - ""
    "###);
}

fn json_input() -> &'static str {
    let input = r#"
    {"time": "1996-12-19T16:39:57Z", "subsort": 0, "key": 1, "e0": {"f1": 0,  "f2": 22}, "e1": 1,  "e2": 2.7,  "e3": "f1"  }
    {"time": "1996-12-19T16:40:57Z", "subsort": 0, "key": 1, "e0": {"f1": 1,  "f2": 10}, "e1": 2,  "e2": 3.8,  "e3": "f2"  }
    {"time": "1996-12-19T16:40:59Z", "subsort": 0, "key": 1, "e0": {"f1": 5,  "f2": 3},  "e1": 42, "e2": 4.0,  "e3": "f3"  }
    {"time": "1996-12-19T16:41:57Z", "subsort": 0, "key": 1, "e0": {"f2": 13},           "e1": 42, "e2": null, "e3": "f2"  }
    {"time": "1996-12-19T16:42:57Z", "subsort": 0, "key": 1, "e0": {"f1": 15, "f3": 11}, "e1": 3,  "e2": 7,    "e3": "f3"  }
    "#;
    input
}

pub(super) fn batch_from_json(
    json: &str,
    column_types: Vec<DataType>,
) -> anyhow::Result<RecordBatch> {
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
            Field::new("subsort", DataType::UInt64, false),
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

    let batch = add_column_and_update_schema(&batch);

    Ok(batch)
}

fn add_column_and_update_schema(batch: &RecordBatch) -> RecordBatch {
    // Create the new column data (for example, a String array in this case)

    let int1_builder = Int64Builder::with_capacity(4);
    let int_builder = Int64Builder::with_capacity(4);

    let mut builder = MapBuilder::new(None, int1_builder, int_builder);

    builder.keys().append_value(1);
    builder.values().append_value(1);
    builder.append(true).unwrap();

    builder.keys().append_value(1);
    builder.values().append_value(2);
    builder.keys().append_value(2);
    builder.values().append_value(4);
    builder.append(true).unwrap();

    builder.append(true).unwrap();

    builder.keys().append_value(2);
    builder.values().append_value(99);
    builder.append(true).unwrap();

    builder.keys().append_value(1);
    builder.values().append_value(10);
    builder.keys().append_value(3);
    builder.values().append_value(7);
    builder.append(true).unwrap();

    let array = builder.finish();
    let array = Arc::new(array);

    // Add the new column to the existing schema
    let k = Field::new("keys", DataType::Int64, false);
    let v = Field::new("values", DataType::Int64, true);
    let fields = Fields::from(vec![k, v]);
    // TODO: test null ?
    let s = Arc::new(Field::new("entries", DataType::Struct(fields), false));

    let mut new_fields = vec![];
    batch
        .schema()
        .fields()
        .iter()
        .for_each(|f| new_fields.push(f.clone()));
    new_fields.push(Arc::new(Field::new("e4", DataType::Map(s, false), false)));

    // Update the schema with the new field
    let new_schema = Arc::new(Schema::new(new_fields));

    // Create a new record batch with the updated schema and existing data
    let mut columns = vec![];
    batch.columns().iter().for_each(|c| columns.push(c.clone()));
    columns.push(array);

    RecordBatch::try_new(new_schema, columns).unwrap()
}

async fn json_to_parquet_file(json_input: &str, file: File) {
    let fields = Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]);
    let m1 = Arc::new(Field::new("map", DataType::Struct(fields), false));

    let column_types = vec![
        DataType::Map(m1, false),
        DataType::Int64,
        DataType::Float64,
        DataType::Utf8,
    ];
    let record_batch = batch_from_json(json_input, column_types).unwrap();

    // Create a Parquet writer
    let mut writer = ArrowWriter::try_new(file, record_batch.schema(), None).unwrap();
    writer.write(&record_batch).unwrap();

    // Close the writer to finish writing the file
    writer.close().unwrap();
}

async fn arrow_collection_data_fixture() -> DataFixture {
    let input = json_input();
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop();
    path.pop();
    path.push("testdata");
    path.push("parquet/data_with_map.parquet");

    let file = File::create(path).unwrap();
    json_to_parquet_file(input, file).await;

    DataFixture::new()
        .with_table_from_files(
            TableConfig::new_with_table_source("Input", &Uuid::new_v4(), "time", None, "key", ""),
            &[&"parquet/data_with_map.parquet"],
        )
        .await
        .unwrap()
}
