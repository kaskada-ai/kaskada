//! e2e tests for collect to list function

use indoc::indoc;
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::{fixture::DataFixture, fixtures::i64_data_fixture, QueryFixture};

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

pub(crate) async fn collect_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new_with_table_source(
                "Collect",
                &Uuid::new_v4(),
                "time",
                Some("subsort"),
                "key",
                "",
            ),
            indoc! {"
    time,subsort,key,s,n,b
    1996-12-19T16:39:57-08:00,0,A,hEllo,0,true
    1996-12-19T16:40:57-08:00,0,A,hi,2,false
    1996-12-19T16:41:57-08:00,0,A,hey,9,,
    1996-12-19T16:42:57-08:00,0,A,ay,-1,true
    1996-12-19T16:43:57-08:00,0,A,hIlo,10,true
    1996-12-20T16:40:57-08:00,0,B,h,5,false
    1996-12-20T16:41:57-08:00,0,B,he,-2,,
    1996-12-20T16:42:57-08:00,0,B,,-2,true
    1996-12-20T16:43:57-08:00,0,B,hel,2,false
    1996-12-20T16:44:57-08:00,0,B,,true
    1996-12-21T16:44:57-08:00,0,C,g,true
    1996-12-21T16:45:57-08:00,0,C,go,true
    1996-12-21T16:46:57-08:00,0,C,goo,true
    1996-12-21T16:47:57-08:00,0,C,good,true
    "},
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_collect_to_list_i64() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.n | collect(10) | index(0) }").with_dump_dot("asdf").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,5
    1996-12-20T00:39:58.000000000,9223372036854775808,2867199309159137213,B,24
    1996-12-20T00:39:59.000000000,9223372036854775808,12960666915911099378,A,5
    1996-12-20T00:40:00.000000000,9223372036854775808,12960666915911099378,A,5
    1996-12-20T00:40:01.000000000,9223372036854775808,12960666915911099378,A,5
    1996-12-20T00:40:02.000000000,9223372036854775808,12960666915911099378,A,5
    "###);
}
