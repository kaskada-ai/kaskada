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
#[ignore = "https://docs.rs/arrow-ord/44.0.0/src/arrow_ord/comparison.rs.html#1746"]
async fn test_list_equality() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Input.bool_list | first() == Input.bool_list }").run_to_csv(&list_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    "###);
}

#[tokio::test]
async fn test_first_list() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Input.string_list | first() | index(0) }").run_to_csv(&list_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,18433805721903975440,1,dog
    1996-12-19T16:40:57.000000000,0,18433805721903975440,1,dog
    1996-12-19T16:40:59.000000000,0,18433805721903975440,1,dog
    1996-12-19T16:41:57.000000000,0,18433805721903975440,1,dog
    1996-12-19T16:42:57.000000000,0,18433805721903975440,1,dog
    "###);
}

#[tokio::test]
async fn test_last_list() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Input.string_list | last() | index(0) }").run_to_csv(&list_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,18433805721903975440,1,dog
    1996-12-19T16:40:57.000000000,0,18433805721903975440,1,cat
    1996-12-19T16:40:59.000000000,0,18433805721903975440,1,
    1996-12-19T16:41:57.000000000,0,18433805721903975440,1,dog
    1996-12-19T16:42:57.000000000,0,18433805721903975440,1,dog
    "###);
}

#[tokio::test]
async fn test_list_len() {
    insta::assert_snapshot!(QueryFixture::new("{
        len_struct: { s: Input.string_list } | collect(max=null) | list_len(),
        len_num: Input.i64_list | list_len(),
        len_str: Input.string_list | list_len(),
        len_bool: Input.bool_list | list_len(),
    }
     ").run_to_csv(&list_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,len_struct,len_num,len_str,len_bool
    1996-12-19T16:39:57.000000000,0,18433805721903975440,1,1,3,2,2
    1996-12-19T16:40:57.000000000,0,18433805721903975440,1,2,3,3,2
    1996-12-19T16:40:59.000000000,0,18433805721903975440,1,3,3,0,3
    1996-12-19T16:41:57.000000000,0,18433805721903975440,1,4,3,2,3
    1996-12-19T16:42:57.000000000,0,18433805721903975440,1,5,3,1,1
    "###);
}

#[tokio::test]
async fn test_list_schemas_are_compatible() {
    // This query puts a collect() into a record, which
    // does schema validation when constructing the struct array.
    let hash = QueryFixture::new(
        "
    let s_list = Input.string_list
    let first_elem = s_list | index(0)
    let list_with_first_elems = first_elem | collect(max = null)
    in { l: Input.string_list, list_with_first_elems }
    ",
    )
    .run_to_parquet_hash(&list_data_fixture().await)
    .await
    .unwrap();

    insta::assert_snapshot!(
        hash,
        @"36F43DDEFC30C844CED73BEDAB7EF0CA0365A52B6B079EA23E7FFB98"
    );
}

#[tokio::test]
async fn test_using_list_in_get_fails() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ f1: Input.i64_list | get(\"s\") }")
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
          - "1 | { f1: Input.i64_list | get(\"s\") }"
          - "  |                        ^^^ Invalid types for parameter 'map' in call to 'get'"
          - "  |"
          - "  --> internal:1:1"
          - "  |"
          - 1 | $input
          - "  | ------ Actual type: list<i64>"
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
async fn test_incorrect_index_type() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ f1: Input.i64_list | index(\"s\") }")
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
          - "1 | { f1: Input.i64_list | index(\"s\") }"
          - "  |                        ^^^^^ --- Actual type: string"
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
