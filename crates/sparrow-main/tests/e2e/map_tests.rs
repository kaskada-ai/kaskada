//! e2e tests for map types.

use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::{fixture::DataFixture, QueryFixture};

/// Create a simple table with a collection type (map).
pub(crate) async fn map_data_fixture() -> DataFixture {
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
    insta::assert_snapshot!(QueryFixture::new("{ f1: get(\"f1\", Input.s_to_i64) }").run_to_csv(&map_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,18433805721903975440,1,0
    1996-12-19T16:40:57.000000000,0,18433805721903975440,1,1
    1996-12-19T16:40:59.000000000,0,18433805721903975440,1,5
    1996-12-19T16:41:57.000000000,0,18433805721903975440,1,
    1996-12-19T16:42:57.000000000,0,18433805721903975440,1,15
    "###);
}

#[tokio::test]
async fn test_string_to_i64_get_static_key_second_field() {
    insta::assert_snapshot!(QueryFixture::new("{ f2: Input.s_to_i64 | get(\"f2\") }").run_to_csv(&map_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f2
    1996-12-19T16:39:57.000000000,0,18433805721903975440,1,22
    1996-12-19T16:40:57.000000000,0,18433805721903975440,1,10
    1996-12-19T16:40:59.000000000,0,18433805721903975440,1,3
    1996-12-19T16:41:57.000000000,0,18433805721903975440,1,13
    1996-12-19T16:42:57.000000000,0,18433805721903975440,1,
    "###);
}

#[tokio::test]
async fn test_string_to_i64_get_dynamic_key() {
    insta::assert_snapshot!(QueryFixture::new("{ value: Input.s_to_i64 | get(Input.s_to_i64_key) }").run_to_csv(&map_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,value
    1996-12-19T16:39:57.000000000,0,18433805721903975440,1,0
    1996-12-19T16:40:57.000000000,0,18433805721903975440,1,10
    1996-12-19T16:40:59.000000000,0,18433805721903975440,1,
    1996-12-19T16:41:57.000000000,0,18433805721903975440,1,13
    1996-12-19T16:42:57.000000000,0,18433805721903975440,1,11
    "###);
}

#[tokio::test]
async fn test_i64_to_i64_get_static_key() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: get(1, Input.i64_to_i64) }").run_to_csv(&map_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,18433805721903975440,1,1
    1996-12-19T16:40:57.000000000,0,18433805721903975440,1,2
    1996-12-19T16:40:59.000000000,0,18433805721903975440,1,
    1996-12-19T16:41:57.000000000,0,18433805721903975440,1,
    1996-12-19T16:42:57.000000000,0,18433805721903975440,1,10
    "###);
}

#[tokio::test]
async fn test_u64_to_str_get_static_key() {
    // Ideally we don't have to specify `as u64`. See https://github.com/kaskada-ai/kaskada/issues/534
    insta::assert_snapshot!(QueryFixture::new("{ f1: get(4 as u64, Input.u64_to_s) }").run_to_csv(&map_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,18433805721903975440,1,
    1996-12-19T16:40:57.000000000,0,18433805721903975440,1,cat
    1996-12-19T16:40:59.000000000,0,18433805721903975440,1,
    1996-12-19T16:41:57.000000000,0,18433805721903975440,1,
    1996-12-19T16:42:57.000000000,0,18433805721903975440,1,plant
    "###);
}

#[tokio::test]
async fn test_u64_to_bool_get_static_key() {
    // Ideally we don't have to specify `as u64`. See https://github.com/kaskada-ai/kaskada/issues/534
    insta::assert_snapshot!(QueryFixture::new("{ f1: get(4 as u64, Input.u64_to_bool) }").run_to_csv(&map_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,18433805721903975440,1,
    1996-12-19T16:40:57.000000000,0,18433805721903975440,1,false
    1996-12-19T16:40:59.000000000,0,18433805721903975440,1,
    1996-12-19T16:41:57.000000000,0,18433805721903975440,1,
    1996-12-19T16:42:57.000000000,0,18433805721903975440,1,true
    "###);
}

#[tokio::test]
async fn test_bool_to_s_get_static_key() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: get(true, Input.bool_to_s) }").run_to_csv(&map_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-19T16:39:57.000000000,0,18433805721903975440,1,dog
    1996-12-19T16:40:57.000000000,0,18433805721903975440,1,cat
    1996-12-19T16:40:59.000000000,0,18433805721903975440,1,
    1996-12-19T16:41:57.000000000,0,18433805721903975440,1,bird
    1996-12-19T16:42:57.000000000,0,18433805721903975440,1,plant
    "###);
}

#[tokio::test]
async fn test_s_to_i64_get_with_first_last_agg() {
    // Note that the last_f2 is empty. This is expected because the last() aggregation
    // is applied over the _map_ value, which does not necessarily hold an "f2" key.
    insta::assert_snapshot!(QueryFixture::new("{ first_f2: Input.s_to_i64 | first() | get(\"f2\"), last_f2: Input.s_to_i64 | last() | get(\"f2\") }").run_to_csv(&map_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,first_f2,last_f2
    1996-12-19T16:39:57.000000000,0,18433805721903975440,1,22,22
    1996-12-19T16:40:57.000000000,0,18433805721903975440,1,22,10
    1996-12-19T16:40:59.000000000,0,18433805721903975440,1,22,3
    1996-12-19T16:41:57.000000000,0,18433805721903975440,1,22,13
    1996-12-19T16:42:57.000000000,0,18433805721903975440,1,22,
    "###);
}

#[tokio::test]
async fn test_map_output_into_sum_aggregation() {
    insta::assert_snapshot!(QueryFixture::new("{ sum: Input.s_to_i64 | get(\"f1\") | sum(), value: Input.s_to_i64 | get(Input.s_to_i64_key) } | with_key(Input.s_to_i64_key)").run_to_csv(&map_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,sum,value
    1996-12-19T16:39:57.000000000,0,16639162690259065874,f1,0,0
    1996-12-19T16:40:57.000000000,0,10324241474229410792,f2,1,10
    1996-12-19T16:40:59.000000000,0,1256917131994086784,f3,6,
    1996-12-19T16:41:57.000000000,0,10324241474229410792,f2,6,13
    1996-12-19T16:42:57.000000000,0,1256917131994086784,f3,21,11
    "###);
}

#[tokio::test]
#[ignore = "https://docs.rs/arrow-ord/44.0.0/src/arrow_ord/comparison.rs.html#1746"]
async fn test_map_equality() {
    insta::assert_snapshot!(QueryFixture::new("{ first_eq: Input.s_to_i64 | first() == Input.s_to_i64, last_eq: Input.s_to_i64 | last() == Input.s_to_i64 }").run_to_csv(&map_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    "###);
}

#[tokio::test]
async fn test_query_with_merge_and_map_output() {
    // This query produces a `merge` operations with `map` inputs, verifying
    // we support maps within the _unlatched_ `spread` operation as well.
    // Note that _latched_ spread is a separate implementation.
    //
    // It also produces a `map` as an output, verifying we can write maps to parquet.
    let hash = QueryFixture::new(
        "{ map: Input.s_to_i64, value: Input.s_to_i64 | get(Input.s_to_i64_key), lookup: lookup(Input.s_to_i64_key as u64, Input) }",
    )
    .run_to_parquet_hash(&map_data_fixture().await)
    .await
    .unwrap();

    assert_eq!(
        "E28AF590FCD81C1F47F79355DBCADDCBB76CF0B15091EB4D07F2E900",
        hash
    );
}

#[tokio::test]
async fn test_first_last_map() {
    // The csv writer does not support map types currently, so the output has been verified
    // manually and now just compared as the hash of the parquet output.
    let hash =
        QueryFixture::new("{ first: Input.s_to_i64 | first(), last: Input.s_to_i64 | last() }")
            .run_to_parquet_hash(&map_data_fixture().await)
            .await
            .unwrap();

    let expected = "D88A77A7F5172527642A4784A7482D17303FBE510435AA5E6BD3965C";
    assert_eq!(hash, expected);
}

#[tokio::test]
async fn test_swapped_args_for_get_map() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ f1: get(Input.s_to_i64, \"f1\") }")
        .run_to_csv(&map_data_fixture().await).await.unwrap_err(), @r###"
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
        .run_to_csv(&map_data_fixture().await).await.unwrap_err(), @r###"
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

#[tokio::test]
async fn test_using_map_in_index_fails() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ f1: Input.i64_to_i64 | index(0) }")
        .run_to_csv(&map_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:26"
          - "  |"
          - "1 | { f1: Input.i64_to_i64 | index(0) }"
          - "  |                          ^^^^^ Invalid types for parameter 'list' in call to 'index'"
          - "  |"
          - "  --> internal:1:1"
          - "  |"
          - 1 | $input
          - "  | ------ Actual type: map<i64, i64>"
          - "  |"
          - "  --> built-in signature 'index<T: any>(i: i64, list: list<T>) -> T':1:29"
          - "  |"
          - "1 | index<T: any>(i: i64, list: list<T>) -> T"
          - "  |                             ------- Expected type: list<T>"
          - ""
          - ""
    "###);
}
