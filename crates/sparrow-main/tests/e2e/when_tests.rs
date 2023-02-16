//! Basic e2e tests for the tick functions.

use indoc::indoc;
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::{DataFixture, QueryFixture};

/// Fixture for testing when.
///
/// Includes a column of every type being and a condition column.
fn when_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new(
                "WhenFixture",
                &Uuid::new_v4(),
                "time",
                Some("subsort"),
                "key",
                "",
            ),
            indoc! {"
    time,subsort,key,cond,bool,i64,string
    1996-12-19T16:39:57-08:00,0,A,true,false,57,hello
    1996-12-19T16:39:58-08:00,0,A,false,true,58,world
    1996-12-19T16:39:59-08:00,0,A,,true,59,world
    1996-12-19T16:40:00-08:00,0,A,true,,,
    1996-12-19T16:40:01-08:00,0,A,false,,,
    1996-12-19T16:40:02-08:00,0,A,true,,02,hello
    "},
        )
        .unwrap()
}

#[tokio::test]
async fn test_boolean_when() {
    insta::assert_snapshot!(QueryFixture::new("{ when: WhenFixture.bool | when(WhenFixture.cond) }").run_to_csv(&when_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,when
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,false
    1996-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,
    1996-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,
    "###);
}

#[tokio::test]
async fn test_when_cond() {
    // This relied on `i64 == 2` being a predicate that only matched
    // rows in the last slice of the input.
    insta::assert_snapshot!(QueryFixture::new("WhenFixture | when(WhenFixture.i64 == 2)").run_to_csv(&when_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,time,subsort,key,cond,bool,i64,string
    1996-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,1996-12-19T16:40:02-08:00,0,A,true,,2,hello
    "###)
}

#[tokio::test]
async fn test_i64_when() {
    insta::assert_snapshot!(QueryFixture::new("{ when: WhenFixture.i64 | when(WhenFixture.cond) }").run_to_csv(&when_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,when
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,57
    1996-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,
    1996-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,2
    "###);
}

#[tokio::test]
async fn test_timestamp_when() {
    insta::assert_snapshot!(QueryFixture::new("{ when: WhenFixture.time | when(WhenFixture.cond) }").run_to_csv(&when_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,when
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,1996-12-19T16:39:57-08:00
    1996-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,1996-12-19T16:40:00-08:00
    1996-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,1996-12-19T16:40:02-08:00
    "###);
}

#[tokio::test]
async fn test_string_when() {
    insta::assert_snapshot!(QueryFixture::new("{ when: WhenFixture.string | when(WhenFixture.cond) }").run_to_csv(&when_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,when
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,hello
    1996-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,
    1996-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,hello
    "###);
}

#[tokio::test]
async fn test_record_when() {
    insta::assert_snapshot!(QueryFixture::new("WhenFixture | when($input.cond)").run_to_csv(&when_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,time,subsort,key,cond,bool,i64,string
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,1996-12-19T16:39:57-08:00,0,A,true,false,57,hello
    1996-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,1996-12-19T16:40:00-08:00,0,A,true,,,
    1996-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,1996-12-19T16:40:02-08:00,0,A,true,,2,hello
    "###);
}

#[tokio::test]
async fn test_record_when_chained() {
    insta::assert_snapshot!(QueryFixture::new("WhenFixture | when($input.cond) | when(WhenFixture.cond)").run_to_csv(&when_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,time,subsort,key,cond,bool,i64,string
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,1996-12-19T16:39:57-08:00,0,A,true,false,57,hello
    1996-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,1996-12-19T16:40:00-08:00,0,A,true,,,
    1996-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,1996-12-19T16:40:02-08:00,0,A,true,,2,hello
    "###);
}

#[tokio::test]
async fn test_when_false() {
    // Tests the case when all rows are filtered out by `when`.
    // This *may* have interesting behaviors (empty stages).
    // This isn't a perfect test though, since the optimizer may
    // realize this is a no-op and eliminate it.
    insta::assert_snapshot!(QueryFixture::new("WhenFixture | when(false)").run_to_csv(&when_data_fixture()).await.unwrap(), @"_time,_subsort,_key_hash,_key,time,subsort,key,cond,bool,i64,string
");
}
