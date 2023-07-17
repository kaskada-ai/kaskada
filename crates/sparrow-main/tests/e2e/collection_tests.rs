//! e2e tests for the collection operators.

use crate::{
    fixtures::{collection_data_fixture, supplant_data_fixture},
    QueryFixture,
};

#[tokio::test]
async fn test_get_static_key() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: get(\"f1\", Input.e0) }").run_to_csv(&collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1970-01-01T00:00:00.000002000,5039430902799166705,2359047937476779835,1,0
    1970-01-01T00:00:00.000003000,5039430902799166706,2359047937476779835,1,1
    1970-01-01T00:00:00.000003000,5039430902799166707,2359047937476779835,1,5
    1970-01-01T00:00:00.000003000,5039430902799166708,2359047937476779835,1,
    1970-01-01T00:00:00.000004000,5039430902799166709,2359047937476779835,1,15
    "###);
}

#[tokio::test]
async fn test_get_static_key_second_field() {
    insta::assert_snapshot!(QueryFixture::new("{ f2: Input.e0 | get(\"f2\") }").run_to_csv(&collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f2
    1970-01-01T00:00:00.000002000,5039430902799166705,2359047937476779835,1,22
    1970-01-01T00:00:00.000003000,5039430902799166706,2359047937476779835,1,10
    1970-01-01T00:00:00.000003000,5039430902799166707,2359047937476779835,1,3
    1970-01-01T00:00:00.000003000,5039430902799166708,2359047937476779835,1,13
    1970-01-01T00:00:00.000004000,5039430902799166709,2359047937476779835,1,
    "###);
}

#[tokio::test]
async fn test_get_dynamic_key() {
    insta::assert_snapshot!(QueryFixture::new("{ value: Input.e0 | get(Input.e3) }").run_to_csv(&collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,value
    1970-01-01T00:00:00.000002000,5039430902799166705,2359047937476779835,1,0
    1970-01-01T00:00:00.000003000,5039430902799166706,2359047937476779835,1,10
    1970-01-01T00:00:00.000003000,5039430902799166707,2359047937476779835,1,
    1970-01-01T00:00:00.000003000,5039430902799166708,2359047937476779835,1,13
    1970-01-01T00:00:00.000004000,5039430902799166709,2359047937476779835,1,11
    "###);
}

#[tokio::test]
async fn test_supplant() {
    insta::assert_snapshot!(QueryFixture::new("{ value: get(\"airTemperature\", Input.readings) | when(is_valid($input)) } ").with_dump_dot("asdf").run_to_csv(&supplant_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,value
    "###);
}
