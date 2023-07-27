//! e2e tests for list types
use std::{fs::File, path::PathBuf, sync::Arc};

use anyhow::Context;
use arrow::{
    array::{Int32Builder, Int64Builder, ListBuilder, MapBuilder, StringBuilder},
    datatypes::{DataType, Field, Fields, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use itertools::Itertools;
use parquet::arrow::ArrowWriter;

use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::{fixture::DataFixture, QueryFixture};

/// Create a simple table with a collection type (map).
pub(crate) async fn list_data_fixture() -> DataFixture {
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
            &["parquet/data_with_list.parquet"],
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_list() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Input.list | index(0) }").with_dump_dot("asdf").run_to_csv(&arrow_list_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,18433805721903975440,1,1
    1996-12-19T16:40:57.000000000,0,18433805721903975440,1,2
    1996-12-19T16:40:59.000000000,0,18433805721903975440,1,3
    1996-12-19T16:41:57.000000000,0,18433805721903975440,1,
    1996-12-19T16:42:57.000000000,0,18433805721903975440,1,1
    "###);
}

fn json_input() -> &'static str {
    let input = r#"
    {"time": "1996-12-19T16:39:57Z", "subsort": 0, "key": 1, "list": [1, 2, 3] }
    {"time": "1996-12-19T16:40:57Z", "subsort": 0, "key": 1, "list": [1, 2, 3] } 
    {"time": "1996-12-19T16:40:59Z", "subsort": 0, "key": 1, "list": [1, 2, 3] } 
    {"time": "1996-12-19T16:41:57Z", "subsort": 0, "key": 1, "list": [1, 2, 3] } 
    {"time": "1996-12-19T16:42:57Z", "subsort": 0, "key": 1, "list": [1, 2, 3] } 
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

        fields.push(Field::new(
            "list",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
            false,
        ));

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

    // let mut builder = ListBuilder::new(Int32Builder::new());
    // builder.append_value([Some(1), Some(2), Some(3)]);
    // builder.append_value([]);
    // builder.append_value([None]);
    // builder.append_value([Some(10), Some(8), Some(4)]);
    // builder.append_value([Some(10)]);

    // let array = builder.finish();
    // let array = Arc::new(array);

    // // Add the new column to the existing schema
    // let f = Arc::new(Field::new("item", DataType::Int32, false));
    // let s = Arc::new(Field::new("list", DataType::List(f), false));

    // let mut new_fields = vec![];
    // batch
    //     .schema()
    //     .fields()
    //     .iter()
    //     .for_each(|f| new_fields.push(f.clone()));
    // new_fields.push(Arc::new(Field::new("list", DataType::Map(s, false), false)));

    // Update the schema with the new field
    // let new_schema = Arc::new(Schema::new(new_fields));

    // Create a new record batch with the updated schema and existing data
    let mut columns = vec![];
    batch.columns().iter().for_each(|c| columns.push(c.clone()));
    // columns.push(array);

    // TODO: REMOVE
    let new_schema = batch.schema().clone();

    RecordBatch::try_new(new_schema, columns).unwrap()
}

async fn json_to_parquet_file(json_input: &str, file: File) {
    let field = Arc::new(Field::new("my_item", DataType::Int32, false));
    let record_batch = batch_from_json(json_input, vec![DataType::List(field)]).unwrap();

    // Create a Parquet writer
    let mut writer = ArrowWriter::try_new(file, record_batch.schema(), None).unwrap();
    writer.write(&record_batch).unwrap();

    // Close the writer to finish writing the file
    writer.close().unwrap();
}

async fn arrow_list_data_fixture() -> DataFixture {
    let input = json_input();
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop();
    path.pop();
    path.push("testdata");
    path.push("parquet/data_with_list.parquet");

    let file = File::create(path).unwrap();
    json_to_parquet_file(input, file).await;

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
            &[&"parquet/data_with_list.parquet"],
        )
        .await
        .unwrap()
}
