//! e2e tests for the collection operators.

use itertools::Itertools;

use arrow::record_batch::RecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::{fixture::DataFixture, QueryFixture};

/// Create a simple table with a collection type (map).
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
            &["parquet/data_with_map.parquet"],
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_string_to_i64_get_static_key() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: get(\"f1\", Input.s_to_i64) }").run_to_csv(&collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,2359047937476779835,1,0
    1996-12-19T16:40:57.000000000,0,2359047937476779835,1,1
    1996-12-19T16:40:59.000000000,0,2359047937476779835,1,5
    1996-12-19T16:41:57.000000000,0,2359047937476779835,1,
    1996-12-19T16:42:57.000000000,0,2359047937476779835,1,15
    "###);
}

#[tokio::test]
async fn test_string_to_i64_get_static_key_second_field() {
    insta::assert_snapshot!(QueryFixture::new("{ f2: Input.s_to_i64 | get(\"f2\") }").run_to_csv(&collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f2
    1996-12-19T16:39:57.000000000,0,2359047937476779835,1,22
    1996-12-19T16:40:57.000000000,0,2359047937476779835,1,10
    1996-12-19T16:40:59.000000000,0,2359047937476779835,1,3
    1996-12-19T16:41:57.000000000,0,2359047937476779835,1,13
    1996-12-19T16:42:57.000000000,0,2359047937476779835,1,
    "###);
}

#[tokio::test]
async fn test_string_to_i64_get_dynamic_key() {
    insta::assert_snapshot!(QueryFixture::new("{ value: Input.s_to_i64 | get(Input.s_to_i64_key) }").run_to_csv(&collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,value
    1996-12-19T16:39:57.000000000,0,2359047937476779835,1,0
    1996-12-19T16:40:57.000000000,0,2359047937476779835,1,10
    1996-12-19T16:40:59.000000000,0,2359047937476779835,1,
    1996-12-19T16:41:57.000000000,0,2359047937476779835,1,13
    1996-12-19T16:42:57.000000000,0,2359047937476779835,1,11
    "###);
}

#[tokio::test]
async fn test_i64_to_i64_get_static_key() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: get(1, Input.i64_to_i64) }").run_to_csv(&collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,2359047937476779835,1,1
    1996-12-19T16:40:57.000000000,0,2359047937476779835,1,2
    1996-12-19T16:40:59.000000000,0,2359047937476779835,1,
    1996-12-19T16:41:57.000000000,0,2359047937476779835,1,
    1996-12-19T16:42:57.000000000,0,2359047937476779835,1,10
    "###);
}

#[tokio::test]
async fn test_u64_to_str_get_static_key() {
    // Ideally we don't have to specify `as u64`. See https://github.com/kaskada-ai/kaskada/issues/534
    insta::assert_snapshot!(QueryFixture::new("{ f1: get(4 as u64, Input.u64_to_s) }").run_to_csv(&collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,2359047937476779835,1,
    1996-12-19T16:40:57.000000000,0,2359047937476779835,1,cat
    1996-12-19T16:40:59.000000000,0,2359047937476779835,1,
    1996-12-19T16:41:57.000000000,0,2359047937476779835,1,
    1996-12-19T16:42:57.000000000,0,2359047937476779835,1,plant
    "###);
}

#[tokio::test]
async fn test_u64_to_bool_get_static_key() {
    // Ideally we don't have to specify `as u64`. See https://github.com/kaskada-ai/kaskada/issues/534
    insta::assert_snapshot!(QueryFixture::new("{ f1: get(4 as u64, Input.u64_to_bool) }").run_to_csv(&collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,2359047937476779835,1,
    1996-12-19T16:40:57.000000000,0,2359047937476779835,1,false
    1996-12-19T16:40:59.000000000,0,2359047937476779835,1,
    1996-12-19T16:41:57.000000000,0,2359047937476779835,1,
    1996-12-19T16:42:57.000000000,0,2359047937476779835,1,true
    "###);
}

#[tokio::test]
async fn test_bool_to_s_get_static_key() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: get(true, Input.bool_to_s) }").run_to_csv(&collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,2359047937476779835,1,dog
    1996-12-19T16:40:57.000000000,0,2359047937476779835,1,cat
    1996-12-19T16:40:59.000000000,0,2359047937476779835,1,
    1996-12-19T16:41:57.000000000,0,2359047937476779835,1,bird
    1996-12-19T16:42:57.000000000,0,2359047937476779835,1,plant
    "###);
}

async fn test_first_map() {
    // insta::assert_snapshot!(QueryFixture::new("{ value: Input.e0 | last() }").run_to_csv(&collection_data_fixture().await).await.unwrap(), @r###"
    // _time,_subsort,_key_hash,_key,value
    // "###);

    let hash = QueryFixture::new("{ value: Input.e0 | first() }")
        .with_dump_dot("asdf")
        .run_to_parquet(&collection_data_fixture().await)
        .await
        .unwrap();

    // let expected = "adsf";
    // assert_eq!(hash, expected);

    let file = std::fs::File::open(hash).unwrap();
    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(1000);
    let reader = parquet_reader.build().unwrap();
    let schema = reader.schema();
    let batches: Vec<_> = reader.try_collect().unwrap();
    let concatenated = arrow::compute::concat_batches(&schema, &batches).unwrap();
    println!("{:?}", concatenated);
}

#[tokio::test]
async fn test_last_map() {
    // insta::assert_snapshot!(QueryFixture::new("{ value: Input.e0 | last() }").run_to_csv(&collection_data_fixture().await).await.unwrap(), @r###"
    // _time,_subsort,_key_hash,_key,value
    // "###);

    let hash = QueryFixture::new("{ value: Input.e0 | last() }")
        .run_to_parquet(&collection_data_fixture().await)
        .await
        .unwrap();

    // let expected = "adsf";
    // assert_eq!(hash, expected);

    let file = std::fs::File::open(hash).unwrap();
    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(1000);
    let reader = parquet_reader.build().unwrap();
    let schema = reader.schema();
    let batches: Vec<_> = reader.try_collect().unwrap();
    let concatenated = arrow::compute::concat_batches(&schema, &batches).unwrap();
    println!("{:?}", concatenated);
}

#[tokio::test]
async fn test_swapped_args_for_get_map() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ f1: get(Input.s_to_i64, \"f1\") }")
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
          - "1 | { f1: get(Input.s_to_i64, \"f1\") }"
          - "  |       ^^^                 ---- Actual type: string"
          - "  |       |                    "
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

#[tokio::test]
async fn test_incompatible_key_types() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ f1: get(\"f1\", Input.i64_to_i64) }")
        .run_to_csv(&collection_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0015
        message: Incompatible argument types
        formatted:
          - "error[E0015]: Incompatible argument types"
          - "  --> Query:1:7"
          - "  |"
          - "1 | { f1: get(\"f1\", Input.i64_to_i64) }"
          - "  |       ^^^ ----  ---------------- Type: i64"
          - "  |       |   |      "
          - "  |       |   Type: string"
          - "  |       Incompatible types for call to 'get'"
          - ""
          - ""
    "###);
}
