use indoc::indoc;
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::{DataFixture, QueryFixture};

/// Create a simple table with some columns suitable for use as foreign keys.
///
/// `foreign_key_i64` is either 0, 1 or 2 (never `null`).
/// `foreign_key_str` is either `A`, `B` or `C` (or `null`).
///
/// ```csv
/// time,subsort,key,foreign_key_i64,foreign_key_str,n
/// 1996-12-19T16:39:57-08:00,0,A,0,B,0
/// 1996-12-19T16:39:58-08:00,0,B,1,A,1
/// 1996-12-19T16:39:59-08:00,0,A,2,,
/// 1996-12-19T16:40:00-08:00,0,A,2,C,2
/// 1996-12-19T16:40:01-08:00,0,A,1,A,3
/// 1996-12-19T16:40:02-08:00,0,A,0,B,4
/// ```
pub(crate) async fn with_key_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new_with_table_source(
                "Table",
                &Uuid::new_v4(),
                "time",
                Some("subsort"),
                "key",
                "",
            ),
            indoc! {"
    time,subsort,key,foreign_key_i64,foreign_key_str,n
    1996-12-19T16:39:57-08:00,0,A,0,B,0
    1996-12-19T16:39:58-08:00,0,B,1,A,1
    1996-12-19T16:39:59-08:00,0,A,2,,
    1996-12-19T16:40:00-08:00,0,A,2,C,2
    1996-12-19T16:40:01-08:00,0,A,1,A,3
    1996-12-19T16:40:02-08:00,0,A,0,B,4
    "},
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_with_key_i64_pipe() {
    insta::assert_snapshot!(QueryFixture::new("Table | with_key($input.foreign_key_i64)").run_to_csv(&with_key_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,time,subsort,key,foreign_key_i64,foreign_key_str,n
    1996-12-20T00:39:57.000000000,9223372036854775808,14253486467890685049,0,1996-12-20T00:39:57.000000000,0,A,0,B,0
    1996-12-20T00:39:58.000000000,9223372036854775808,2359047937476779835,1,1996-12-20T00:39:58.000000000,0,B,1,A,1
    1996-12-20T00:39:59.000000000,9223372036854775808,1575016611515860288,2,1996-12-20T00:39:59.000000000,0,A,2,,
    1996-12-20T00:40:00.000000000,9223372036854775808,1575016611515860288,2,1996-12-20T00:40:00.000000000,0,A,2,C,2
    1996-12-20T00:40:01.000000000,9223372036854775808,2359047937476779835,1,1996-12-20T00:40:01.000000000,0,A,1,A,3
    1996-12-20T00:40:02.000000000,9223372036854775808,14253486467890685049,0,1996-12-20T00:40:02.000000000,0,A,0,B,4
    "###);
}

#[tokio::test]
async fn test_with_key_lookup_select() {
    insta::assert_snapshot!(QueryFixture::new("Table | with_key($input.foreign_key_i64) | last() | lookup(Table.foreign_key_i64) | when($input.foreign_key_i64 > 0)").run_to_csv(&with_key_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,time,subsort,key,foreign_key_i64,foreign_key_str,n
    1996-12-20T00:39:58.000000000,9223372036854775808,11753611437813598533,B,1996-12-20T00:39:58.000000000,0,B,1,A,1
    1996-12-20T00:39:59.000000000,9223372036854775808,3650215962958587783,A,1996-12-20T00:39:59.000000000,0,A,2,,
    1996-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,1996-12-20T00:40:00.000000000,0,A,2,C,2
    1996-12-20T00:40:01.000000000,9223372036854775808,3650215962958587783,A,1996-12-20T00:40:01.000000000,0,A,1,A,3
    "###);
}

#[tokio::test]
async fn test_with_key_i64() {
    insta::assert_snapshot!(QueryFixture::new("with_key(Table.foreign_key_i64, Table)").run_to_csv(&with_key_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,time,subsort,key,foreign_key_i64,foreign_key_str,n
    1996-12-20T00:39:57.000000000,9223372036854775808,14253486467890685049,0,1996-12-20T00:39:57.000000000,0,A,0,B,0
    1996-12-20T00:39:58.000000000,9223372036854775808,2359047937476779835,1,1996-12-20T00:39:58.000000000,0,B,1,A,1
    1996-12-20T00:39:59.000000000,9223372036854775808,1575016611515860288,2,1996-12-20T00:39:59.000000000,0,A,2,,
    1996-12-20T00:40:00.000000000,9223372036854775808,1575016611515860288,2,1996-12-20T00:40:00.000000000,0,A,2,C,2
    1996-12-20T00:40:01.000000000,9223372036854775808,2359047937476779835,1,1996-12-20T00:40:01.000000000,0,A,1,A,3
    1996-12-20T00:40:02.000000000,9223372036854775808,14253486467890685049,0,1996-12-20T00:40:02.000000000,0,A,0,B,4
    "###);
}

#[tokio::test]
async fn test_with_key_aggregate_select() {
    // This tests the case where the value is in a different operation than the key.
    // Specifically, the `value` comes from a `select` (`when` applied to the scan)
    // while the key is directly from the `scan`.
    insta::assert_snapshot!(QueryFixture::new(
        "{ sum: Table.n | when(Table.key == 'A') | sum() | with_key(Table.foreign_key_i64) }"
    ).run_to_csv(&with_key_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,sum
    1996-12-20T00:39:57.000000000,9223372036854775808,14253486467890685049,0,0
    1996-12-20T00:39:58.000000000,9223372036854775808,2359047937476779835,1,
    1996-12-20T00:39:59.000000000,9223372036854775808,1575016611515860288,2,0
    1996-12-20T00:40:00.000000000,9223372036854775808,1575016611515860288,2,2
    1996-12-20T00:40:01.000000000,9223372036854775808,2359047937476779835,1,5
    1996-12-20T00:40:02.000000000,9223372036854775808,14253486467890685049,0,9
    "###)
}

#[tokio::test]
async fn test_with_key_i64_parquet_output() {
    // NOTE: Parquet output changes when the Parquet writer version changes.
    insta::assert_snapshot!(
        QueryFixture::new("with_key(Table.foreign_key_i64, Table)")
            .run_to_parquet_hash(&with_key_data_fixture().await)
            .await
            .unwrap(),
        @"CBDFADE9439A36B3400C3C3E4539F6045A3E59EE7B7B09A23DBD6AD3"
    )
}

#[tokio::test]
async fn test_with_computed_key_i64() {
    insta::assert_snapshot!(QueryFixture::new("with_key(Table.foreign_key_i64 + 1, Table)").run_to_csv(&with_key_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,time,subsort,key,foreign_key_i64,foreign_key_str,n
    1996-12-20T00:39:57.000000000,9223372036854775808,2359047937476779835,1,1996-12-20T00:39:57.000000000,0,A,0,B,0
    1996-12-20T00:39:58.000000000,9223372036854775808,1575016611515860288,2,1996-12-20T00:39:58.000000000,0,B,1,A,1
    1996-12-20T00:39:59.000000000,9223372036854775808,14956259290599888306,3,1996-12-20T00:39:59.000000000,0,A,2,,
    1996-12-20T00:40:00.000000000,9223372036854775808,14956259290599888306,3,1996-12-20T00:40:00.000000000,0,A,2,C,2
    1996-12-20T00:40:01.000000000,9223372036854775808,1575016611515860288,2,1996-12-20T00:40:01.000000000,0,A,1,A,3
    1996-12-20T00:40:02.000000000,9223372036854775808,2359047937476779835,1,1996-12-20T00:40:02.000000000,0,A,0,B,4
    "###);
}

#[tokio::test]
async fn test_with_computed_key_str() {
    insta::assert_snapshot!(QueryFixture::new("with_key(Table.foreign_key_str, Table)").run_to_csv(&with_key_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,time,subsort,key,foreign_key_i64,foreign_key_str,n
    1996-12-20T00:39:57.000000000,9223372036854775808,11753611437813598533,B,1996-12-20T00:39:57.000000000,0,A,0,B,0
    1996-12-20T00:39:58.000000000,9223372036854775808,3650215962958587783,A,1996-12-20T00:39:58.000000000,0,B,1,A,1
    1996-12-20T00:39:59.000000000,9223372036854775808,8429509363638065888,,1996-12-20T00:39:59.000000000,0,A,2,,
    1996-12-20T00:40:00.000000000,9223372036854775808,9192031977313001967,C,1996-12-20T00:40:00.000000000,0,A,2,C,2
    1996-12-20T00:40:01.000000000,9223372036854775808,3650215962958587783,A,1996-12-20T00:40:01.000000000,0,A,1,A,3
    1996-12-20T00:40:02.000000000,9223372036854775808,11753611437813598533,B,1996-12-20T00:40:02.000000000,0,A,0,B,4
    "###);
}

#[tokio::test]
async fn test_with_key_error_key() {
    insta::assert_yaml_snapshot!(QueryFixture::new("with_key(unbound_key, Table)").run_to_csv(&with_key_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0006
        message: Unbound reference
        formatted:
          - "error[E0006]: Unbound reference"
          - "  --> Query:1:10"
          - "  |"
          - "1 | with_key(unbound_key, Table)"
          - "  |          ^^^^^^^^^^^ No reference named 'unbound_key'"
          - "  |"
          - "  = Nearest matches: 'Table'"
          - ""
          - ""
    "###);
}

#[tokio::test]
#[ignore = "Implement test for with key with slicing"]
async fn test_with_key_slicing() {
    todo!()
}

#[tokio::test]
#[ignore = "Implement test for with key with null handling"]
async fn test_with_null_key() {
    todo!()
}

#[tokio::test]
#[ignore = "Implement test for with key with collisions"]
async fn test_with_colliding_keys() {
    todo!()
}
