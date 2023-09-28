//! Basic e2e tests for testing output with entity keys
use indoc::indoc;
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::fixture::DataFixture;
use crate::fixtures::i64_data_fixture;
use crate::QueryFixture;

pub(crate) async fn multiple_table_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new_with_table_source(
                "Numbers",
                &Uuid::new_v4(),
                "time",
                Some("subsort"),
                "key",
                "numbers",
            ),
            indoc! {"
    time,subsort,key,m,n
    1996-12-19T16:39:57-08:00,0,A,5.2,10
    1996-12-19T16:39:58-08:00,0,B,24.3,3.9
    1996-12-19T16:39:59-08:00,0,A,17.6,6.2
    1996-12-19T16:40:00-08:00,0,A,,9.25
    1996-12-19T16:40:01-08:00,0,A,12.4,
    1996-12-19T16:40:02-08:00,0,A,,
    "},
        )
        .await
        .unwrap()
        .with_table_from_csv(
            TableConfig::new_with_table_source(
                "Numbers2",
                &Uuid::new_v4(),
                "time",
                Some("subsort"),
                "key",
                "numbers",
            ),
            indoc! {"
    time,subsort,key,m,n
    1996-12-19T16:40:03-08:00,0,C,5.2,10
    1996-12-19T16:40:04-08:00,0,D,24.3,3.9
    1996-12-19T16:40:05-08:00,0,C,17.6,6.2
    1996-12-19T16:40:06-08:00,0,C,,9.25
    1996-12-19T16:40:07-08:00,0,C,12.4,
    1996-12-19T16:40:08-08:00,0,C,,
    "},
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_entity_keys_numbers() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,
    "###);
}

#[tokio::test]
async fn test_multiple_tables_entity_keys() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, n: Numbers2.n }").run_to_csv(&multiple_table_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5.2,
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24.3,
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17.6,
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12.4,
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,
    1996-12-20T00:40:03.000000000,0,2521269998124177631,C,,10.0
    1996-12-20T00:40:04.000000000,0,1021973589662386405,D,,3.9
    1996-12-20T00:40:05.000000000,0,2521269998124177631,C,,6.2
    1996-12-20T00:40:06.000000000,0,2521269998124177631,C,,9.25
    1996-12-20T00:40:07.000000000,0,2521269998124177631,C,,
    1996-12-20T00:40:08.000000000,0,2521269998124177631,C,,
    "###);
}

#[tokio::test]
async fn test_lookup_entity_keys() {
    insta::assert_snapshot!(QueryFixture::new("{ m: lookup(Numbers.key, sum(Numbers2.n)) }").run_to_csv(&multiple_table_fixture().await).await
        .unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,
    "###);
}

#[tokio::test]
async fn test_with_key() {
    insta::assert_snapshot!(QueryFixture::new("Numbers | with_key($input.n, grouping='other_key')").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,time,subsort,key,m,n
    1996-12-20T00:39:57.000000000,0,10780876405615667760,10,1996-12-20T00:39:57.000000000,0,A,5,10
    1996-12-20T00:39:58.000000000,0,5496774745203840792,3,1996-12-20T00:39:58.000000000,0,B,24,3
    1996-12-20T00:39:59.000000000,0,1360592371395427998,6,1996-12-20T00:39:59.000000000,0,A,17,6
    1996-12-20T00:40:00.000000000,0,15653042715643359010,9,1996-12-20T00:40:00.000000000,0,A,,9
    1996-12-20T00:40:01.000000000,0,0,,1996-12-20T00:40:01.000000000,0,A,12,
    1996-12-20T00:40:02.000000000,0,0,,1996-12-20T00:40:02.000000000,0,A,,
    "###);
}

#[tokio::test]
async fn test_lookup_with_key_entity_keys() {
    insta::assert_snapshot!(QueryFixture::new("{ m: lookup(Numbers.key, with_key(Numbers.key, sum(Numbers.m))) }").run_to_csv(&multiple_table_fixture().await).await
        .unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5.2
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24.3
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,22.8
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,22.8
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,35.2
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,35.2
    "###);
}
