//! e2e tests for collect function

use indoc::indoc;
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::{fixture::DataFixture, QueryFixture};

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
    time,subsort,key,s,n,b,index
    1996-12-19T16:39:57-08:00,0,A,hEllo,0,true,0
    1996-12-19T16:40:57-08:00,0,A,hi,2,false,1
    1996-12-19T16:41:57-08:00,0,A,hey,9,,2
    1996-12-19T16:42:00-08:00,0,A,heylo,-7,false,2
    1996-12-19T16:42:57-08:00,0,A,ay,-1,true,1
    1996-12-19T16:43:57-08:00,0,A,hIlo,10,true,
    1996-12-20T16:40:57-08:00,0,B,h,5,false,0
    1996-12-20T16:41:57-08:00,0,B,he,-2,,1
    1996-12-20T16:42:57-08:00,0,B,,,true,2
    1996-12-20T16:43:57-08:00,0,B,hel,2,false,1
    1996-12-20T16:44:57-08:00,0,B,,,true,1
    1996-12-20T17:44:57-08:00,0,B,hello,5,true,0
    1996-12-21T16:44:57-08:00,0,C,g,1,true,2
    1996-12-21T16:45:57-08:00,0,C,go,2,true,0
    1996-12-21T16:46:57-08:00,0,C,goo,3,true,
    1996-12-21T16:47:57-08:00,0,C,good,4,true,1
    "},
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_collect_with_null_max() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.n | collect(max = null) | index(0), f2: Collect.b | collect(max = null) | index(0), f3: Collect.s | collect(max = null) | index(0) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1,f2,f3
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,0,true,hEllo
    1996-12-20T00:40:57.000000000,0,12960666915911099378,A,0,true,hEllo
    1996-12-20T00:41:57.000000000,0,12960666915911099378,A,0,true,hEllo
    1996-12-20T00:42:00.000000000,0,12960666915911099378,A,0,true,hEllo
    1996-12-20T00:42:57.000000000,0,12960666915911099378,A,0,true,hEllo
    1996-12-20T00:43:57.000000000,0,12960666915911099378,A,0,true,hEllo
    1996-12-21T00:40:57.000000000,0,2867199309159137213,B,5,false,h
    1996-12-21T00:41:57.000000000,0,2867199309159137213,B,5,false,h
    1996-12-21T00:42:57.000000000,0,2867199309159137213,B,5,false,h
    1996-12-21T00:43:57.000000000,0,2867199309159137213,B,5,false,h
    1996-12-21T00:44:57.000000000,0,2867199309159137213,B,5,false,h
    1996-12-21T01:44:57.000000000,0,2867199309159137213,B,5,false,h
    1996-12-22T00:44:57.000000000,0,2521269998124177631,C,1,true,g
    1996-12-22T00:45:57.000000000,0,2521269998124177631,C,1,true,g
    1996-12-22T00:46:57.000000000,0,2521269998124177631,C,1,true,g
    1996-12-22T00:47:57.000000000,0,2521269998124177631,C,1,true,g
    "###);
}

#[tokio::test]
async fn test_collect_to_list_i64() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.n | collect(max=10) | index(0) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,0
    1996-12-20T00:40:57.000000000,0,12960666915911099378,A,0
    1996-12-20T00:41:57.000000000,0,12960666915911099378,A,0
    1996-12-20T00:42:00.000000000,0,12960666915911099378,A,0
    1996-12-20T00:42:57.000000000,0,12960666915911099378,A,0
    1996-12-20T00:43:57.000000000,0,12960666915911099378,A,0
    1996-12-21T00:40:57.000000000,0,2867199309159137213,B,5
    1996-12-21T00:41:57.000000000,0,2867199309159137213,B,5
    1996-12-21T00:42:57.000000000,0,2867199309159137213,B,5
    1996-12-21T00:43:57.000000000,0,2867199309159137213,B,5
    1996-12-21T00:44:57.000000000,0,2867199309159137213,B,5
    1996-12-21T01:44:57.000000000,0,2867199309159137213,B,5
    1996-12-22T00:44:57.000000000,0,2521269998124177631,C,1
    1996-12-22T00:45:57.000000000,0,2521269998124177631,C,1
    1996-12-22T00:46:57.000000000,0,2521269998124177631,C,1
    1996-12-22T00:47:57.000000000,0,2521269998124177631,C,1
    "###);
}

#[tokio::test]
async fn test_collect_to_list_i64_dynamic() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.n | collect(max=10) | index(Collect.index) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,0
    1996-12-20T00:40:57.000000000,0,12960666915911099378,A,2
    1996-12-20T00:41:57.000000000,0,12960666915911099378,A,9
    1996-12-20T00:42:00.000000000,0,12960666915911099378,A,9
    1996-12-20T00:42:57.000000000,0,12960666915911099378,A,2
    1996-12-20T00:43:57.000000000,0,12960666915911099378,A,
    1996-12-21T00:40:57.000000000,0,2867199309159137213,B,5
    1996-12-21T00:41:57.000000000,0,2867199309159137213,B,-2
    1996-12-21T00:42:57.000000000,0,2867199309159137213,B,
    1996-12-21T00:43:57.000000000,0,2867199309159137213,B,-2
    1996-12-21T00:44:57.000000000,0,2867199309159137213,B,-2
    1996-12-21T01:44:57.000000000,0,2867199309159137213,B,5
    1996-12-22T00:44:57.000000000,0,2521269998124177631,C,
    1996-12-22T00:45:57.000000000,0,2521269998124177631,C,1
    1996-12-22T00:46:57.000000000,0,2521269998124177631,C,
    1996-12-22T00:47:57.000000000,0,2521269998124177631,C,2
    "###);
}

#[tokio::test]
async fn test_collect_to_small_list_i64() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.n | collect(max=2) | index(Collect.index) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,0
    1996-12-20T00:40:57.000000000,0,12960666915911099378,A,2
    1996-12-20T00:41:57.000000000,0,12960666915911099378,A,
    1996-12-20T00:42:00.000000000,0,12960666915911099378,A,
    1996-12-20T00:42:57.000000000,0,12960666915911099378,A,-1
    1996-12-20T00:43:57.000000000,0,12960666915911099378,A,
    1996-12-21T00:40:57.000000000,0,2867199309159137213,B,5
    1996-12-21T00:41:57.000000000,0,2867199309159137213,B,-2
    1996-12-21T00:42:57.000000000,0,2867199309159137213,B,
    1996-12-21T00:43:57.000000000,0,2867199309159137213,B,2
    1996-12-21T00:44:57.000000000,0,2867199309159137213,B,2
    1996-12-21T01:44:57.000000000,0,2867199309159137213,B,2
    1996-12-22T00:44:57.000000000,0,2521269998124177631,C,
    1996-12-22T00:45:57.000000000,0,2521269998124177631,C,1
    1996-12-22T00:46:57.000000000,0,2521269998124177631,C,
    1996-12-22T00:47:57.000000000,0,2521269998124177631,C,4
    "###);
}

#[tokio::test]
async fn test_collect_to_list_string() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.s | collect(max=10) | index(0) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo
    1996-12-20T00:40:57.000000000,0,12960666915911099378,A,hEllo
    1996-12-20T00:41:57.000000000,0,12960666915911099378,A,hEllo
    1996-12-20T00:42:00.000000000,0,12960666915911099378,A,hEllo
    1996-12-20T00:42:57.000000000,0,12960666915911099378,A,hEllo
    1996-12-20T00:43:57.000000000,0,12960666915911099378,A,hEllo
    1996-12-21T00:40:57.000000000,0,2867199309159137213,B,h
    1996-12-21T00:41:57.000000000,0,2867199309159137213,B,h
    1996-12-21T00:42:57.000000000,0,2867199309159137213,B,h
    1996-12-21T00:43:57.000000000,0,2867199309159137213,B,h
    1996-12-21T00:44:57.000000000,0,2867199309159137213,B,h
    1996-12-21T01:44:57.000000000,0,2867199309159137213,B,h
    1996-12-22T00:44:57.000000000,0,2521269998124177631,C,g
    1996-12-22T00:45:57.000000000,0,2521269998124177631,C,g
    1996-12-22T00:46:57.000000000,0,2521269998124177631,C,g
    1996-12-22T00:47:57.000000000,0,2521269998124177631,C,g
    "###);
}

#[tokio::test]
async fn test_collect_to_list_string_dynamic() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.s | collect(max=10) | index(Collect.index) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo
    1996-12-20T00:40:57.000000000,0,12960666915911099378,A,hi
    1996-12-20T00:41:57.000000000,0,12960666915911099378,A,hey
    1996-12-20T00:42:00.000000000,0,12960666915911099378,A,hey
    1996-12-20T00:42:57.000000000,0,12960666915911099378,A,hi
    1996-12-20T00:43:57.000000000,0,12960666915911099378,A,
    1996-12-21T00:40:57.000000000,0,2867199309159137213,B,h
    1996-12-21T00:41:57.000000000,0,2867199309159137213,B,he
    1996-12-21T00:42:57.000000000,0,2867199309159137213,B,
    1996-12-21T00:43:57.000000000,0,2867199309159137213,B,he
    1996-12-21T00:44:57.000000000,0,2867199309159137213,B,he
    1996-12-21T01:44:57.000000000,0,2867199309159137213,B,h
    1996-12-22T00:44:57.000000000,0,2521269998124177631,C,
    1996-12-22T00:45:57.000000000,0,2521269998124177631,C,g
    1996-12-22T00:46:57.000000000,0,2521269998124177631,C,
    1996-12-22T00:47:57.000000000,0,2521269998124177631,C,go
    "###);
}

#[tokio::test]
async fn test_collect_to_small_list_string() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.s | collect(max=2) | index(Collect.index) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo
    1996-12-20T00:40:57.000000000,0,12960666915911099378,A,hi
    1996-12-20T00:41:57.000000000,0,12960666915911099378,A,
    1996-12-20T00:42:00.000000000,0,12960666915911099378,A,
    1996-12-20T00:42:57.000000000,0,12960666915911099378,A,ay
    1996-12-20T00:43:57.000000000,0,12960666915911099378,A,
    1996-12-21T00:40:57.000000000,0,2867199309159137213,B,h
    1996-12-21T00:41:57.000000000,0,2867199309159137213,B,he
    1996-12-21T00:42:57.000000000,0,2867199309159137213,B,
    1996-12-21T00:43:57.000000000,0,2867199309159137213,B,hel
    1996-12-21T00:44:57.000000000,0,2867199309159137213,B,
    1996-12-21T01:44:57.000000000,0,2867199309159137213,B,
    1996-12-22T00:44:57.000000000,0,2521269998124177631,C,
    1996-12-22T00:45:57.000000000,0,2521269998124177631,C,g
    1996-12-22T00:46:57.000000000,0,2521269998124177631,C,
    1996-12-22T00:47:57.000000000,0,2521269998124177631,C,good
    "###);
}

#[tokio::test]
async fn test_collect_to_list_boolean() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.b | collect(max=10) | index(0) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true
    1996-12-20T00:40:57.000000000,0,12960666915911099378,A,true
    1996-12-20T00:41:57.000000000,0,12960666915911099378,A,true
    1996-12-20T00:42:00.000000000,0,12960666915911099378,A,true
    1996-12-20T00:42:57.000000000,0,12960666915911099378,A,true
    1996-12-20T00:43:57.000000000,0,12960666915911099378,A,true
    1996-12-21T00:40:57.000000000,0,2867199309159137213,B,false
    1996-12-21T00:41:57.000000000,0,2867199309159137213,B,false
    1996-12-21T00:42:57.000000000,0,2867199309159137213,B,false
    1996-12-21T00:43:57.000000000,0,2867199309159137213,B,false
    1996-12-21T00:44:57.000000000,0,2867199309159137213,B,false
    1996-12-21T01:44:57.000000000,0,2867199309159137213,B,false
    1996-12-22T00:44:57.000000000,0,2521269998124177631,C,true
    1996-12-22T00:45:57.000000000,0,2521269998124177631,C,true
    1996-12-22T00:46:57.000000000,0,2521269998124177631,C,true
    1996-12-22T00:47:57.000000000,0,2521269998124177631,C,true
    "###);
}

#[tokio::test]
async fn test_collect_to_list_boolean_dynamic() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.b | collect(max=10) | index(Collect.index) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true
    1996-12-20T00:40:57.000000000,0,12960666915911099378,A,false
    1996-12-20T00:41:57.000000000,0,12960666915911099378,A,
    1996-12-20T00:42:00.000000000,0,12960666915911099378,A,false
    1996-12-20T00:42:57.000000000,0,12960666915911099378,A,false
    1996-12-20T00:43:57.000000000,0,12960666915911099378,A,
    1996-12-21T00:40:57.000000000,0,2867199309159137213,B,false
    1996-12-21T00:41:57.000000000,0,2867199309159137213,B,
    1996-12-21T00:42:57.000000000,0,2867199309159137213,B,
    1996-12-21T00:43:57.000000000,0,2867199309159137213,B,true
    1996-12-21T00:44:57.000000000,0,2867199309159137213,B,true
    1996-12-21T01:44:57.000000000,0,2867199309159137213,B,false
    1996-12-22T00:44:57.000000000,0,2521269998124177631,C,
    1996-12-22T00:45:57.000000000,0,2521269998124177631,C,true
    1996-12-22T00:46:57.000000000,0,2521269998124177631,C,
    1996-12-22T00:47:57.000000000,0,2521269998124177631,C,true
    "###);
}

#[tokio::test]
async fn test_collect_to_small_list_boolean() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.b | collect(max=2) | index(Collect.index) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true
    1996-12-20T00:40:57.000000000,0,12960666915911099378,A,false
    1996-12-20T00:41:57.000000000,0,12960666915911099378,A,
    1996-12-20T00:42:00.000000000,0,12960666915911099378,A,
    1996-12-20T00:42:57.000000000,0,12960666915911099378,A,true
    1996-12-20T00:43:57.000000000,0,12960666915911099378,A,
    1996-12-21T00:40:57.000000000,0,2867199309159137213,B,false
    1996-12-21T00:41:57.000000000,0,2867199309159137213,B,
    1996-12-21T00:42:57.000000000,0,2867199309159137213,B,
    1996-12-21T00:43:57.000000000,0,2867199309159137213,B,false
    1996-12-21T00:44:57.000000000,0,2867199309159137213,B,true
    1996-12-21T01:44:57.000000000,0,2867199309159137213,B,true
    1996-12-22T00:44:57.000000000,0,2521269998124177631,C,
    1996-12-22T00:45:57.000000000,0,2521269998124177631,C,true
    1996-12-22T00:46:57.000000000,0,2521269998124177631,C,
    1996-12-22T00:47:57.000000000,0,2521269998124177631,C,true
    "###);
}

#[tokio::test]
async fn test_collect_structs() {
    insta::assert_snapshot!(QueryFixture::new("{
        s0: { s: Collect.s, n: Collect.n, b: Collect.b } | collect(max = null) | index(0) | $input.s,
        s1: { s: Collect.s, n: Collect.n, b: Collect.b } | collect(max = null) | index(1) | $input.s,
        s2: { s: Collect.s, n: Collect.n, b: Collect.b } | collect(max = null) | index(2) | $input.s,
        s3: { s: Collect.s, n: Collect.n, b: Collect.b } | collect(max = null) | index(3) | $input.s,
        s4: { s: Collect.s, n: Collect.n, b: Collect.b } | collect(max = null) | index(4) | $input.s
    }
     ").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,s0,s1,s2,s3,s4
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo,,,,
    1996-12-20T00:40:57.000000000,0,12960666915911099378,A,hEllo,hi,,,
    1996-12-20T00:41:57.000000000,0,12960666915911099378,A,hEllo,hi,hey,,
    1996-12-20T00:42:00.000000000,0,12960666915911099378,A,hEllo,hi,hey,heylo,
    1996-12-20T00:42:57.000000000,0,12960666915911099378,A,hEllo,hi,hey,heylo,ay
    1996-12-20T00:43:57.000000000,0,12960666915911099378,A,hEllo,hi,hey,heylo,ay
    1996-12-21T00:40:57.000000000,0,2867199309159137213,B,h,,,,
    1996-12-21T00:41:57.000000000,0,2867199309159137213,B,h,he,,,
    1996-12-21T00:42:57.000000000,0,2867199309159137213,B,h,he,,,
    1996-12-21T00:43:57.000000000,0,2867199309159137213,B,h,he,,hel,
    1996-12-21T00:44:57.000000000,0,2867199309159137213,B,h,he,,hel,
    1996-12-21T01:44:57.000000000,0,2867199309159137213,B,h,he,,hel,
    1996-12-22T00:44:57.000000000,0,2521269998124177631,C,g,,,,
    1996-12-22T00:45:57.000000000,0,2521269998124177631,C,g,go,,,
    1996-12-22T00:46:57.000000000,0,2521269998124177631,C,g,go,goo,,
    1996-12-22T00:47:57.000000000,0,2521269998124177631,C,g,go,goo,good,
    "###);
}

#[tokio::test]
async fn test_collect_with_minimum() {
    insta::assert_snapshot!(QueryFixture::new("{
        min0: Collect.s | collect(max=10) | index(0),
        min1: Collect.s | collect(min=2, max=10) | index(0),
        min2: Collect.s | collect(min=3, max=10) | index(0)
    }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,min0,min1,min2
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo,,
    1996-12-20T00:40:57.000000000,0,12960666915911099378,A,hEllo,hEllo,
    1996-12-20T00:41:57.000000000,0,12960666915911099378,A,hEllo,hEllo,hEllo
    1996-12-20T00:42:00.000000000,0,12960666915911099378,A,hEllo,hEllo,hEllo
    1996-12-20T00:42:57.000000000,0,12960666915911099378,A,hEllo,hEllo,hEllo
    1996-12-20T00:43:57.000000000,0,12960666915911099378,A,hEllo,hEllo,hEllo
    1996-12-21T00:40:57.000000000,0,2867199309159137213,B,h,,
    1996-12-21T00:41:57.000000000,0,2867199309159137213,B,h,h,
    1996-12-21T00:42:57.000000000,0,2867199309159137213,B,h,h,h
    1996-12-21T00:43:57.000000000,0,2867199309159137213,B,h,h,h
    1996-12-21T00:44:57.000000000,0,2867199309159137213,B,h,h,h
    1996-12-21T01:44:57.000000000,0,2867199309159137213,B,h,h,h
    1996-12-22T00:44:57.000000000,0,2521269998124177631,C,g,,
    1996-12-22T00:45:57.000000000,0,2521269998124177631,C,g,g,
    1996-12-22T00:46:57.000000000,0,2521269998124177631,C,g,g,g
    1996-12-22T00:47:57.000000000,0,2521269998124177631,C,g,g,g
    "###);
}

#[tokio::test]
async fn test_collect_structs_map() {
    insta::assert_snapshot!(QueryFixture::new("
        let x = Collect | collect(max=10) | $input.s
        in { x: x | index(0), y: x | index(1) }
        ").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,x,y
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo,
    1996-12-20T00:40:57.000000000,0,12960666915911099378,A,hEllo,hi
    1996-12-20T00:41:57.000000000,0,12960666915911099378,A,hEllo,hi
    1996-12-20T00:42:00.000000000,0,12960666915911099378,A,hEllo,hi
    1996-12-20T00:42:57.000000000,0,12960666915911099378,A,hEllo,hi
    1996-12-20T00:43:57.000000000,0,12960666915911099378,A,hEllo,hi
    1996-12-21T00:40:57.000000000,0,2867199309159137213,B,h,
    1996-12-21T00:41:57.000000000,0,2867199309159137213,B,h,he
    1996-12-21T00:42:57.000000000,0,2867199309159137213,B,h,he
    1996-12-21T00:43:57.000000000,0,2867199309159137213,B,h,he
    1996-12-21T00:44:57.000000000,0,2867199309159137213,B,h,he
    1996-12-21T01:44:57.000000000,0,2867199309159137213,B,h,he
    1996-12-22T00:44:57.000000000,0,2521269998124177631,C,g,
    1996-12-22T00:45:57.000000000,0,2521269998124177631,C,g,go
    1996-12-22T00:46:57.000000000,0,2521269998124177631,C,g,go
    1996-12-22T00:47:57.000000000,0,2521269998124177631,C,g,go
    "###);
}

#[tokio::test]
#[ignore = "unsupported via fenl"]
async fn test_collect_lists() {
    insta::assert_snapshot!(QueryFixture::new("
        let x = Collect.s | collect(max=10) | collect(max=10)
        in { x }
        ").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,x,y
    "###);
}

#[tokio::test]
async fn test_collect_lag_equality() {
    // Lag is implemented with collect now, so these _better_ be the same.
    insta::assert_snapshot!(QueryFixture::new("{
        collect: Collect.n | collect(min=3, max=3) | index(0),
        lag: Collect.n | lag(2)
    }").with_dump_dot("asdf").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,collect,lag
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,,
    1996-12-20T00:40:57.000000000,0,12960666915911099378,A,,
    1996-12-20T00:41:57.000000000,0,12960666915911099378,A,0,0
    1996-12-20T00:42:00.000000000,0,12960666915911099378,A,2,2
    1996-12-20T00:42:57.000000000,0,12960666915911099378,A,9,9
    1996-12-20T00:43:57.000000000,0,12960666915911099378,A,-7,-7
    1996-12-21T00:40:57.000000000,0,2867199309159137213,B,,
    1996-12-21T00:41:57.000000000,0,2867199309159137213,B,,
    1996-12-21T00:42:57.000000000,0,2867199309159137213,B,,
    1996-12-21T00:43:57.000000000,0,2867199309159137213,B,5,5
    1996-12-21T00:44:57.000000000,0,2867199309159137213,B,5,5
    1996-12-21T01:44:57.000000000,0,2867199309159137213,B,-2,-2
    1996-12-22T00:44:57.000000000,0,2521269998124177631,C,,
    1996-12-22T00:45:57.000000000,0,2521269998124177631,C,,
    1996-12-22T00:46:57.000000000,0,2521269998124177631,C,1,1
    1996-12-22T00:47:57.000000000,0,2521269998124177631,C,2,2
    "###);
}

#[tokio::test]
async fn test_collect_primitive_since_minutely() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.n | collect(max=10, window=since(minutely())) | index(0) | when(is_valid($input))}").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,0
    1996-12-20T00:40:00.000000000,18446744073709551615,12960666915911099378,A,0
    1996-12-20T00:40:57.000000000,0,12960666915911099378,A,2
    1996-12-20T00:41:00.000000000,18446744073709551615,12960666915911099378,A,2
    1996-12-20T00:41:57.000000000,0,12960666915911099378,A,9
    1996-12-20T00:42:00.000000000,0,12960666915911099378,A,9
    1996-12-20T00:42:00.000000000,18446744073709551615,12960666915911099378,A,9
    1996-12-20T00:42:57.000000000,0,12960666915911099378,A,-1
    1996-12-20T00:43:00.000000000,18446744073709551615,12960666915911099378,A,-1
    1996-12-20T00:43:57.000000000,0,12960666915911099378,A,10
    1996-12-20T00:44:00.000000000,18446744073709551615,12960666915911099378,A,10
    1996-12-21T00:40:57.000000000,0,2867199309159137213,B,5
    1996-12-21T00:41:00.000000000,18446744073709551615,2867199309159137213,B,5
    1996-12-21T00:41:57.000000000,0,2867199309159137213,B,-2
    1996-12-21T00:42:00.000000000,18446744073709551615,2867199309159137213,B,-2
    1996-12-21T00:43:57.000000000,0,2867199309159137213,B,2
    1996-12-21T00:44:00.000000000,18446744073709551615,2867199309159137213,B,2
    1996-12-21T01:44:57.000000000,0,2867199309159137213,B,5
    1996-12-21T01:45:00.000000000,18446744073709551615,2867199309159137213,B,5
    1996-12-22T00:44:57.000000000,0,2521269998124177631,C,1
    1996-12-22T00:45:00.000000000,18446744073709551615,2521269998124177631,C,1
    1996-12-22T00:45:57.000000000,0,2521269998124177631,C,2
    1996-12-22T00:46:00.000000000,18446744073709551615,2521269998124177631,C,2
    1996-12-22T00:46:57.000000000,0,2521269998124177631,C,3
    1996-12-22T00:47:00.000000000,18446744073709551615,2521269998124177631,C,3
    1996-12-22T00:47:57.000000000,0,2521269998124177631,C,4
    "###);
}

#[tokio::test]
async fn test_collect_primitive_since_minutely_1() {
    // Only two rows in this set exist within the same minute, hence these results when
    // getting the second item.
    insta::assert_snapshot!(QueryFixture::new("{
        f1: Collect.n | collect(max=10, window=since(minutely())) | index(1) | when(is_valid($input)),
        f1_with_min: Collect.n | collect(min=3, max=10, window=since(minutely())) | index(1) | when(is_valid($input))
    }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1,f1_with_min
    1996-12-20T00:42:00.000000000,0,12960666915911099378,A,-7,
    1996-12-20T00:42:00.000000000,18446744073709551615,12960666915911099378,A,-7,
    "###);
}

#[tokio::test]
async fn test_collect_string_since_hourly() {
    // note that `B` is empty because we collect `null` as a valid value in a list currently
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.s | collect(max=10, window=since(hourly())) | index(2) | when(is_valid($input)) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:41:57.000000000,0,12960666915911099378,A,hey
    1996-12-20T00:42:00.000000000,0,12960666915911099378,A,hey
    1996-12-20T00:42:57.000000000,0,12960666915911099378,A,hey
    1996-12-20T00:43:57.000000000,0,12960666915911099378,A,hey
    1996-12-20T01:00:00.000000000,18446744073709551615,12960666915911099378,A,hey
    1996-12-21T00:42:57.000000000,0,2867199309159137213,B,
    1996-12-21T00:43:57.000000000,0,2867199309159137213,B,
    1996-12-21T00:44:57.000000000,0,2867199309159137213,B,
    1996-12-21T01:00:00.000000000,18446744073709551615,2867199309159137213,B,
    1996-12-22T00:46:57.000000000,0,2521269998124177631,C,goo
    1996-12-22T00:47:57.000000000,0,2521269998124177631,C,goo
    "###);
}

#[tokio::test]
async fn test_collect_boolean_since_hourly() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.b | collect(max=10, window=since(hourly())) | index(3) | when(is_valid($input)) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:42:57.000000000,0,12960666915911099378,A,true
    1996-12-20T00:43:57.000000000,0,12960666915911099378,A,true
    1996-12-20T01:00:00.000000000,18446744073709551615,12960666915911099378,A,true
    1996-12-21T00:44:57.000000000,0,2867199309159137213,B,true
    1996-12-21T01:00:00.000000000,18446744073709551615,2867199309159137213,B,true
    1996-12-22T00:47:57.000000000,0,2521269998124177631,C,true
    "###);
}

#[tokio::test]
async fn test_collect_struct_since_hourly() {
    // TODO: The results here are weird, because `collect` is latched. I don't think I'd expect
    // the results we have here, but it's possible they're technically in line with what we expect
    // given our continuity rules. We should revisit this.
    // https://github.com/kaskada-ai/kaskada/issues/648
    insta::assert_snapshot!(QueryFixture::new("{ 
        b: Collect.b,
        f0: ({b: Collect.b} | collect(max=10, window=since(hourly())) | index(0)).b | when(is_valid($input)),
        f1: ({b: Collect.b} | collect(max=10, window=since(hourly())) | index(1)).b | when(is_valid($input)),
        f2: ({b: Collect.b} | collect(max=10, window=since(hourly())) | index(2)).b | when(is_valid($input)),
        f3: ({b: Collect.b} | collect(max=10, window=since(hourly())) | index(3)).b | when(is_valid($input)),
        f4: ({b: Collect.b} | collect(max=10, window=since(hourly())) | index(4)).b | when(is_valid($input))
    }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,b,f0,f1,f2,f3,f4
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,true,,,,
    1996-12-20T00:40:57.000000000,0,12960666915911099378,A,false,true,false,,,
    1996-12-20T00:41:57.000000000,0,12960666915911099378,A,,true,false,,,
    1996-12-20T00:42:00.000000000,0,12960666915911099378,A,false,true,false,,false,
    1996-12-20T00:42:57.000000000,0,12960666915911099378,A,true,true,false,,false,true
    1996-12-20T00:43:57.000000000,0,12960666915911099378,A,true,true,false,,false,true
    1996-12-20T01:00:00.000000000,18446744073709551615,12960666915911099378,A,,true,false,,false,true
    1996-12-21T00:40:57.000000000,0,2867199309159137213,B,false,false,,,,
    1996-12-21T00:41:57.000000000,0,2867199309159137213,B,,false,,,,
    1996-12-21T00:42:57.000000000,0,2867199309159137213,B,true,false,,true,,
    1996-12-21T00:43:57.000000000,0,2867199309159137213,B,false,false,,true,false,
    1996-12-21T00:44:57.000000000,0,2867199309159137213,B,true,false,,true,false,true
    1996-12-21T01:00:00.000000000,18446744073709551615,2867199309159137213,B,,false,,true,false,true
    1996-12-21T01:44:57.000000000,0,2867199309159137213,B,true,true,,,,
    1996-12-21T02:00:00.000000000,18446744073709551615,2867199309159137213,B,,true,,,,
    1996-12-22T00:44:57.000000000,0,2521269998124177631,C,true,true,,,,
    1996-12-22T00:45:57.000000000,0,2521269998124177631,C,true,true,true,,,
    1996-12-22T00:46:57.000000000,0,2521269998124177631,C,true,true,true,true,,
    1996-12-22T00:47:57.000000000,0,2521269998124177631,C,true,true,true,true,true,
    "###);
}

#[tokio::test]
async fn test_require_literal_max() {
    // TODO: We should figure out how to not report the second error -- type variables with
    // error propagation needs some fixing.
    insta::assert_yaml_snapshot!(QueryFixture::new("{ f1: Collect.s | collect(max=Collect.index) | index(1) }")
        .run_to_csv(&collect_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 2 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0014
        message: Invalid non-constant argument
        formatted:
          - "error[E0014]: Invalid non-constant argument"
          - "  --> Query:1:31"
          - "  |"
          - "1 | { f1: Collect.s | collect(max=Collect.index) | index(1) }"
          - "  |                               ^^^^^^^^^^^^^ Argument 'max' to 'collect' must be constant, but was not"
          - ""
          - ""
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:48"
          - "  |"
          - "1 | { f1: Collect.s | collect(max=Collect.index) | index(1) }"
          - "  |                                                ^^^^^ Invalid types for parameter 'list' in call to 'index'"
          - "  |"
          - "  --> internal:1:1"
          - "  |"
          - 1 | $input
          - "  | ------ Actual type: error"
          - "  |"
          - "  --> built-in signature 'index<T: any>(i: i64, list: list<T>) -> T':1:29"
          - "  |"
          - "1 | index<T: any>(i: i64, list: list<T>) -> T"
          - "  |                             ------- Expected type: list<T>"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_require_literal_min() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ f1: Collect.s | collect(min=Collect.index, max=10) | index(1) }")
        .run_to_csv(&collect_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 2 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0014
        message: Invalid non-constant argument
        formatted:
          - "error[E0014]: Invalid non-constant argument"
          - "  --> Query:1:31"
          - "  |"
          - "1 | { f1: Collect.s | collect(min=Collect.index, max=10) | index(1) }"
          - "  |                               ^^^^^^^^^^^^^ Argument 'min' to 'collect' must be constant, but was not"
          - ""
          - ""
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:56"
          - "  |"
          - "1 | { f1: Collect.s | collect(min=Collect.index, max=10) | index(1) }"
          - "  |                                                        ^^^^^ Invalid types for parameter 'list' in call to 'index'"
          - "  |"
          - "  --> internal:1:1"
          - "  |"
          - 1 | $input
          - "  | ------ Actual type: error"
          - "  |"
          - "  --> built-in signature 'index<T: any>(i: i64, list: list<T>) -> T':1:29"
          - "  |"
          - "1 | index<T: any>(i: i64, list: list<T>) -> T"
          - "  |                             ------- Expected type: list<T>"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_min_must_be_lte_max() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ f1: Collect.s | collect(min=10, max=0) | index(1) }")
        .run_to_csv(&collect_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0002
        message: Illegal cast
        formatted:
          - "error[E0002]: Illegal cast"
          - "  --> Query:1:31"
          - "  |"
          - "1 | { f1: Collect.s | collect(min=10, max=0) | index(1) }"
          - "  |                               ^^ min '10' must be less than or equal to max '0'"
          - ""
          - ""
    "###);
}
