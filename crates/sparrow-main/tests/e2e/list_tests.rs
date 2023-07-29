//! e2e tests for list types

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
async fn test_index_list_i64_static() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Input.i64_list | index(1) }").run_to_csv(&list_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,18433805721903975440,1,2
    1996-12-19T16:40:57.000000000,0,18433805721903975440,1,2
    1996-12-19T16:40:59.000000000,0,18433805721903975440,1,2
    1996-12-19T16:41:57.000000000,0,18433805721903975440,1,2
    1996-12-19T16:42:57.000000000,0,18433805721903975440,1,2
    "###);
}

#[tokio::test]
async fn test_index_list_i64_dynamic() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Input.i64_list | index(Input.index) }").run_to_csv(&list_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,18433805721903975440,1,1
    1996-12-19T16:40:57.000000000,0,18433805721903975440,1,3
    1996-12-19T16:40:59.000000000,0,18433805721903975440,1,2
    1996-12-19T16:41:57.000000000,0,18433805721903975440,1,3
    1996-12-19T16:42:57.000000000,0,18433805721903975440,1,1
    "###);
}

#[tokio::test]
async fn test_index_list_string_static() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Input.string_list | index(1) }").run_to_csv(&list_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,18433805721903975440,1,bird
    1996-12-19T16:40:57.000000000,0,18433805721903975440,1,bird
    1996-12-19T16:40:59.000000000,0,18433805721903975440,1,
    1996-12-19T16:41:57.000000000,0,18433805721903975440,1,cat
    1996-12-19T16:42:57.000000000,0,18433805721903975440,1,
    "###);
}

#[tokio::test]
async fn test_index_list_string_dynamic() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Input.string_list | index(Input.index) }").run_to_csv(&list_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,18433805721903975440,1,dog
    1996-12-19T16:40:57.000000000,0,18433805721903975440,1,fish
    1996-12-19T16:40:59.000000000,0,18433805721903975440,1,
    1996-12-19T16:41:57.000000000,0,18433805721903975440,1,
    1996-12-19T16:42:57.000000000,0,18433805721903975440,1,dog
    "###);
}

#[tokio::test]
async fn test_index_list_bool_static() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Input.bool_list | index(1) }").run_to_csv(&list_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,18433805721903975440,1,true
    1996-12-19T16:40:57.000000000,0,18433805721903975440,1,false
    1996-12-19T16:40:59.000000000,0,18433805721903975440,1,false
    1996-12-19T16:41:57.000000000,0,18433805721903975440,1,false
    1996-12-19T16:42:57.000000000,0,18433805721903975440,1,
    "###);
}

#[tokio::test]
async fn test_index_list_bool_dynamic() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Input.bool_list | index(Input.index) }").run_to_csv(&list_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,18433805721903975440,1,false
    1996-12-19T16:40:57.000000000,0,18433805721903975440,1,
    1996-12-19T16:40:59.000000000,0,18433805721903975440,1,false
    1996-12-19T16:41:57.000000000,0,18433805721903975440,1,true
    1996-12-19T16:42:57.000000000,0,18433805721903975440,1,true
    "###);
}

#[tokio::test]
async fn test_incorrect_index_type() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ f1: Input.i64_list | index(\"s\") }")
        .run_to_csv(&list_data_fixture().await).await.unwrap_err(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,5
    1996-12-20T00:39:58.000000000,9223372036854775808,2867199309159137213,B,24
    1996-12-20T00:39:59.000000000,9223372036854775808,12960666915911099378,A,22
    1996-12-20T00:40:00.000000000,9223372036854775808,12960666915911099378,A,22
    1996-12-20T00:40:01.000000000,9223372036854775808,12960666915911099378,A,34
    1996-12-20T00:40:02.000000000,9223372036854775808,12960666915911099378,A,34
    "###);
}

#[tokio::test]
async fn test_incorrect_index_type_field() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ f1: Input.i64_list | index(Input.bool_list) }")
        .run_to_csv(&list_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:24"
          - "  |"
          - "1 | { f1: Input.i64_list | index(Input.bool_list) }"
          - "  |                        ^^^^^ --------------- Actual type: list<bool>"
          - "  |                        |      "
          - "  |                        Invalid types for parameter 'i' in call to 'index'"
          - "  |"
          - "  --> built-in signature 'index<T: any>(i: i64, list: list<T>) -> T':1:18"
          - "  |"
          - "1 | index<T: any>(i: i64, list: list<T>) -> T"
          - "  |                  --- Expected type: i64"
          - ""
          - ""
    "###);
}
