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
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,0,true,hEllo
    1996-12-20T00:40:57.000000000,9223372036854775808,12960666915911099378,A,0,true,hEllo
    1996-12-20T00:41:57.000000000,9223372036854775808,12960666915911099378,A,0,true,hEllo
    1996-12-20T00:42:00.000000000,9223372036854775808,12960666915911099378,A,0,true,hEllo
    1996-12-20T00:42:57.000000000,9223372036854775808,12960666915911099378,A,0,true,hEllo
    1996-12-20T00:43:57.000000000,9223372036854775808,12960666915911099378,A,0,true,hEllo
    1996-12-21T00:40:57.000000000,9223372036854775808,2867199309159137213,B,5,false,h
    1996-12-21T00:41:57.000000000,9223372036854775808,2867199309159137213,B,5,false,h
    1996-12-21T00:42:57.000000000,9223372036854775808,2867199309159137213,B,5,false,h
    1996-12-21T00:43:57.000000000,9223372036854775808,2867199309159137213,B,5,false,h
    1996-12-21T00:44:57.000000000,9223372036854775808,2867199309159137213,B,5,false,h
    1996-12-22T00:44:57.000000000,9223372036854775808,2521269998124177631,C,1,true,g
    1996-12-22T00:45:57.000000000,9223372036854775808,2521269998124177631,C,1,true,g
    1996-12-22T00:46:57.000000000,9223372036854775808,2521269998124177631,C,1,true,g
    1996-12-22T00:47:57.000000000,9223372036854775808,2521269998124177631,C,1,true,g
    "###);
}

#[tokio::test]
async fn test_collect_to_list_i64() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.n | collect(10) | index(0) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,0
    1996-12-20T00:40:57.000000000,9223372036854775808,12960666915911099378,A,0
    1996-12-20T00:41:57.000000000,9223372036854775808,12960666915911099378,A,0
    1996-12-20T00:42:00.000000000,9223372036854775808,12960666915911099378,A,0
    1996-12-20T00:42:57.000000000,9223372036854775808,12960666915911099378,A,0
    1996-12-20T00:43:57.000000000,9223372036854775808,12960666915911099378,A,0
    1996-12-21T00:40:57.000000000,9223372036854775808,2867199309159137213,B,5
    1996-12-21T00:41:57.000000000,9223372036854775808,2867199309159137213,B,5
    1996-12-21T00:42:57.000000000,9223372036854775808,2867199309159137213,B,5
    1996-12-21T00:43:57.000000000,9223372036854775808,2867199309159137213,B,5
    1996-12-21T00:44:57.000000000,9223372036854775808,2867199309159137213,B,5
    1996-12-22T00:44:57.000000000,9223372036854775808,2521269998124177631,C,1
    1996-12-22T00:45:57.000000000,9223372036854775808,2521269998124177631,C,1
    1996-12-22T00:46:57.000000000,9223372036854775808,2521269998124177631,C,1
    1996-12-22T00:47:57.000000000,9223372036854775808,2521269998124177631,C,1
    "###);
}

#[tokio::test]
async fn test_collect_to_list_i64_dynamic() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.n | collect(10) | index(Collect.index) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,0
    1996-12-20T00:40:57.000000000,9223372036854775808,12960666915911099378,A,2
    1996-12-20T00:41:57.000000000,9223372036854775808,12960666915911099378,A,9
    1996-12-20T00:42:00.000000000,9223372036854775808,12960666915911099378,A,9
    1996-12-20T00:42:57.000000000,9223372036854775808,12960666915911099378,A,2
    1996-12-20T00:43:57.000000000,9223372036854775808,12960666915911099378,A,
    1996-12-21T00:40:57.000000000,9223372036854775808,2867199309159137213,B,5
    1996-12-21T00:41:57.000000000,9223372036854775808,2867199309159137213,B,-2
    1996-12-21T00:42:57.000000000,9223372036854775808,2867199309159137213,B,
    1996-12-21T00:43:57.000000000,9223372036854775808,2867199309159137213,B,-2
    1996-12-21T00:44:57.000000000,9223372036854775808,2867199309159137213,B,-2
    1996-12-22T00:44:57.000000000,9223372036854775808,2521269998124177631,C,
    1996-12-22T00:45:57.000000000,9223372036854775808,2521269998124177631,C,1
    1996-12-22T00:46:57.000000000,9223372036854775808,2521269998124177631,C,
    1996-12-22T00:47:57.000000000,9223372036854775808,2521269998124177631,C,2
    "###);
}

#[tokio::test]
async fn test_collect_to_small_list_i64() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.n | collect(2) | index(Collect.index) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,0
    1996-12-20T00:40:57.000000000,9223372036854775808,12960666915911099378,A,2
    1996-12-20T00:41:57.000000000,9223372036854775808,12960666915911099378,A,
    1996-12-20T00:42:00.000000000,9223372036854775808,12960666915911099378,A,
    1996-12-20T00:42:57.000000000,9223372036854775808,12960666915911099378,A,-1
    1996-12-20T00:43:57.000000000,9223372036854775808,12960666915911099378,A,
    1996-12-21T00:40:57.000000000,9223372036854775808,2867199309159137213,B,5
    1996-12-21T00:41:57.000000000,9223372036854775808,2867199309159137213,B,-2
    1996-12-21T00:42:57.000000000,9223372036854775808,2867199309159137213,B,
    1996-12-21T00:43:57.000000000,9223372036854775808,2867199309159137213,B,2
    1996-12-21T00:44:57.000000000,9223372036854775808,2867199309159137213,B,
    1996-12-22T00:44:57.000000000,9223372036854775808,2521269998124177631,C,
    1996-12-22T00:45:57.000000000,9223372036854775808,2521269998124177631,C,1
    1996-12-22T00:46:57.000000000,9223372036854775808,2521269998124177631,C,
    1996-12-22T00:47:57.000000000,9223372036854775808,2521269998124177631,C,4
    "###);
}

#[tokio::test]
async fn test_collect_to_list_string() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.s | collect(10) | index(0) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,hEllo
    1996-12-20T00:40:57.000000000,9223372036854775808,12960666915911099378,A,hEllo
    1996-12-20T00:41:57.000000000,9223372036854775808,12960666915911099378,A,hEllo
    1996-12-20T00:42:00.000000000,9223372036854775808,12960666915911099378,A,hEllo
    1996-12-20T00:42:57.000000000,9223372036854775808,12960666915911099378,A,hEllo
    1996-12-20T00:43:57.000000000,9223372036854775808,12960666915911099378,A,hEllo
    1996-12-21T00:40:57.000000000,9223372036854775808,2867199309159137213,B,h
    1996-12-21T00:41:57.000000000,9223372036854775808,2867199309159137213,B,h
    1996-12-21T00:42:57.000000000,9223372036854775808,2867199309159137213,B,h
    1996-12-21T00:43:57.000000000,9223372036854775808,2867199309159137213,B,h
    1996-12-21T00:44:57.000000000,9223372036854775808,2867199309159137213,B,h
    1996-12-22T00:44:57.000000000,9223372036854775808,2521269998124177631,C,g
    1996-12-22T00:45:57.000000000,9223372036854775808,2521269998124177631,C,g
    1996-12-22T00:46:57.000000000,9223372036854775808,2521269998124177631,C,g
    1996-12-22T00:47:57.000000000,9223372036854775808,2521269998124177631,C,g
    "###);
}

#[tokio::test]
async fn test_collect_to_list_string_dynamic() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.s | collect(10) | index(Collect.index) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,hEllo
    1996-12-20T00:40:57.000000000,9223372036854775808,12960666915911099378,A,hi
    1996-12-20T00:41:57.000000000,9223372036854775808,12960666915911099378,A,hey
    1996-12-20T00:42:00.000000000,9223372036854775808,12960666915911099378,A,hey
    1996-12-20T00:42:57.000000000,9223372036854775808,12960666915911099378,A,hi
    1996-12-20T00:43:57.000000000,9223372036854775808,12960666915911099378,A,
    1996-12-21T00:40:57.000000000,9223372036854775808,2867199309159137213,B,h
    1996-12-21T00:41:57.000000000,9223372036854775808,2867199309159137213,B,he
    1996-12-21T00:42:57.000000000,9223372036854775808,2867199309159137213,B,
    1996-12-21T00:43:57.000000000,9223372036854775808,2867199309159137213,B,he
    1996-12-21T00:44:57.000000000,9223372036854775808,2867199309159137213,B,he
    1996-12-22T00:44:57.000000000,9223372036854775808,2521269998124177631,C,
    1996-12-22T00:45:57.000000000,9223372036854775808,2521269998124177631,C,g
    1996-12-22T00:46:57.000000000,9223372036854775808,2521269998124177631,C,
    1996-12-22T00:47:57.000000000,9223372036854775808,2521269998124177631,C,go
    "###);
}

#[tokio::test]
async fn test_collect_to_small_list_string() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.s | collect(2) | index(Collect.index) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,hEllo
    1996-12-20T00:40:57.000000000,9223372036854775808,12960666915911099378,A,hi
    1996-12-20T00:41:57.000000000,9223372036854775808,12960666915911099378,A,
    1996-12-20T00:42:00.000000000,9223372036854775808,12960666915911099378,A,
    1996-12-20T00:42:57.000000000,9223372036854775808,12960666915911099378,A,ay
    1996-12-20T00:43:57.000000000,9223372036854775808,12960666915911099378,A,
    1996-12-21T00:40:57.000000000,9223372036854775808,2867199309159137213,B,h
    1996-12-21T00:41:57.000000000,9223372036854775808,2867199309159137213,B,he
    1996-12-21T00:42:57.000000000,9223372036854775808,2867199309159137213,B,
    1996-12-21T00:43:57.000000000,9223372036854775808,2867199309159137213,B,hel
    1996-12-21T00:44:57.000000000,9223372036854775808,2867199309159137213,B,
    1996-12-22T00:44:57.000000000,9223372036854775808,2521269998124177631,C,
    1996-12-22T00:45:57.000000000,9223372036854775808,2521269998124177631,C,g
    1996-12-22T00:46:57.000000000,9223372036854775808,2521269998124177631,C,
    1996-12-22T00:47:57.000000000,9223372036854775808,2521269998124177631,C,good
    "###);
}

#[tokio::test]
async fn test_collect_to_list_boolean() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.b | collect(10) | index(0) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,true
    1996-12-20T00:40:57.000000000,9223372036854775808,12960666915911099378,A,true
    1996-12-20T00:41:57.000000000,9223372036854775808,12960666915911099378,A,true
    1996-12-20T00:42:00.000000000,9223372036854775808,12960666915911099378,A,true
    1996-12-20T00:42:57.000000000,9223372036854775808,12960666915911099378,A,true
    1996-12-20T00:43:57.000000000,9223372036854775808,12960666915911099378,A,true
    1996-12-21T00:40:57.000000000,9223372036854775808,2867199309159137213,B,false
    1996-12-21T00:41:57.000000000,9223372036854775808,2867199309159137213,B,false
    1996-12-21T00:42:57.000000000,9223372036854775808,2867199309159137213,B,false
    1996-12-21T00:43:57.000000000,9223372036854775808,2867199309159137213,B,false
    1996-12-21T00:44:57.000000000,9223372036854775808,2867199309159137213,B,false
    1996-12-22T00:44:57.000000000,9223372036854775808,2521269998124177631,C,true
    1996-12-22T00:45:57.000000000,9223372036854775808,2521269998124177631,C,true
    1996-12-22T00:46:57.000000000,9223372036854775808,2521269998124177631,C,true
    1996-12-22T00:47:57.000000000,9223372036854775808,2521269998124177631,C,true
    "###);
}

#[tokio::test]
async fn test_collect_to_list_boolean_dynamic() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.b | collect(10) | index(Collect.index) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,true
    1996-12-20T00:40:57.000000000,9223372036854775808,12960666915911099378,A,false
    1996-12-20T00:41:57.000000000,9223372036854775808,12960666915911099378,A,
    1996-12-20T00:42:00.000000000,9223372036854775808,12960666915911099378,A,
    1996-12-20T00:42:57.000000000,9223372036854775808,12960666915911099378,A,false
    1996-12-20T00:43:57.000000000,9223372036854775808,12960666915911099378,A,
    1996-12-21T00:40:57.000000000,9223372036854775808,2867199309159137213,B,false
    1996-12-21T00:41:57.000000000,9223372036854775808,2867199309159137213,B,
    1996-12-21T00:42:57.000000000,9223372036854775808,2867199309159137213,B,true
    1996-12-21T00:43:57.000000000,9223372036854775808,2867199309159137213,B,
    1996-12-21T00:44:57.000000000,9223372036854775808,2867199309159137213,B,
    1996-12-22T00:44:57.000000000,9223372036854775808,2521269998124177631,C,
    1996-12-22T00:45:57.000000000,9223372036854775808,2521269998124177631,C,true
    1996-12-22T00:46:57.000000000,9223372036854775808,2521269998124177631,C,
    1996-12-22T00:47:57.000000000,9223372036854775808,2521269998124177631,C,true
    "###);
}

#[tokio::test]
async fn test_collect_to_small_list_boolean() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.b | collect(2) | index(Collect.index) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,true
    1996-12-20T00:40:57.000000000,9223372036854775808,12960666915911099378,A,false
    1996-12-20T00:41:57.000000000,9223372036854775808,12960666915911099378,A,
    1996-12-20T00:42:00.000000000,9223372036854775808,12960666915911099378,A,
    1996-12-20T00:42:57.000000000,9223372036854775808,12960666915911099378,A,true
    1996-12-20T00:43:57.000000000,9223372036854775808,12960666915911099378,A,
    1996-12-21T00:40:57.000000000,9223372036854775808,2867199309159137213,B,false
    1996-12-21T00:41:57.000000000,9223372036854775808,2867199309159137213,B,
    1996-12-21T00:42:57.000000000,9223372036854775808,2867199309159137213,B,
    1996-12-21T00:43:57.000000000,9223372036854775808,2867199309159137213,B,false
    1996-12-21T00:44:57.000000000,9223372036854775808,2867199309159137213,B,true
    1996-12-22T00:44:57.000000000,9223372036854775808,2521269998124177631,C,
    1996-12-22T00:45:57.000000000,9223372036854775808,2521269998124177631,C,true
    1996-12-22T00:46:57.000000000,9223372036854775808,2521269998124177631,C,
    1996-12-22T00:47:57.000000000,9223372036854775808,2521269998124177631,C,true
    "###);
}

#[tokio::test]
async fn test_collect_structs() {
    insta::assert_snapshot!(QueryFixture::new("
         { s: Collect.s, b: Collect.b } | collect(max = 1) | index(0)
     ").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    "###);
}

#[tokio::test]
async fn test_collect_primitive_since_minutely() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.n | collect(10, window=since(minutely())) | index(0) | when(is_valid($input)) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,0
    1996-12-20T00:40:00.000000000,18446744073709551615,12960666915911099378,A,0
    1996-12-20T00:40:57.000000000,9223372036854775808,12960666915911099378,A,2
    1996-12-20T00:41:00.000000000,18446744073709551615,12960666915911099378,A,2
    1996-12-20T00:41:57.000000000,9223372036854775808,12960666915911099378,A,9
    1996-12-20T00:42:00.000000000,9223372036854775808,12960666915911099378,A,9
    1996-12-20T00:42:00.000000000,18446744073709551615,12960666915911099378,A,9
    1996-12-20T00:42:57.000000000,9223372036854775808,12960666915911099378,A,-1
    1996-12-20T00:43:00.000000000,18446744073709551615,12960666915911099378,A,-1
    1996-12-20T00:43:57.000000000,9223372036854775808,12960666915911099378,A,10
    1996-12-20T00:44:00.000000000,18446744073709551615,12960666915911099378,A,10
    1996-12-21T00:40:57.000000000,9223372036854775808,2867199309159137213,B,5
    1996-12-21T00:41:00.000000000,18446744073709551615,2867199309159137213,B,5
    1996-12-21T00:41:57.000000000,9223372036854775808,2867199309159137213,B,-2
    1996-12-21T00:42:00.000000000,18446744073709551615,2867199309159137213,B,-2
    1996-12-21T00:43:57.000000000,9223372036854775808,2867199309159137213,B,2
    1996-12-21T00:44:00.000000000,18446744073709551615,2867199309159137213,B,2
    1996-12-22T00:44:57.000000000,9223372036854775808,2521269998124177631,C,1
    1996-12-22T00:45:00.000000000,18446744073709551615,2521269998124177631,C,1
    1996-12-22T00:45:57.000000000,9223372036854775808,2521269998124177631,C,2
    1996-12-22T00:46:00.000000000,18446744073709551615,2521269998124177631,C,2
    1996-12-22T00:46:57.000000000,9223372036854775808,2521269998124177631,C,3
    1996-12-22T00:47:00.000000000,18446744073709551615,2521269998124177631,C,3
    1996-12-22T00:47:57.000000000,9223372036854775808,2521269998124177631,C,4
    "###);
}

#[tokio::test]
async fn test_collect_primitive_since_minutely_1() {
    // Only two rows in this set exist within the same minute, hence these results when
    // getting the second item.
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.n | collect(10, window=since(minutely())) | index(1) | when(is_valid($input)) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:42:00.000000000,9223372036854775808,12960666915911099378,A,-7
    1996-12-20T00:42:00.000000000,18446744073709551615,12960666915911099378,A,-7
    "###);
}

#[tokio::test]
async fn test_collect_string_since_hourly() {
    // note that `B` is empty because we collect `null` as a valid value in a list currently
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.s | collect(10, window=since(hourly())) | index(2) | when(is_valid($input)) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:41:57.000000000,9223372036854775808,12960666915911099378,A,hey
    1996-12-20T00:42:00.000000000,9223372036854775808,12960666915911099378,A,hey
    1996-12-20T00:42:57.000000000,9223372036854775808,12960666915911099378,A,hey
    1996-12-20T00:43:57.000000000,9223372036854775808,12960666915911099378,A,hey
    1996-12-20T01:00:00.000000000,18446744073709551615,12960666915911099378,A,hey
    1996-12-21T00:42:57.000000000,9223372036854775808,2867199309159137213,B,
    1996-12-21T00:43:57.000000000,9223372036854775808,2867199309159137213,B,
    1996-12-21T00:44:57.000000000,9223372036854775808,2867199309159137213,B,
    1996-12-21T01:00:00.000000000,18446744073709551615,2867199309159137213,B,
    1996-12-22T00:46:57.000000000,9223372036854775808,2521269998124177631,C,goo
    1996-12-22T00:47:57.000000000,9223372036854775808,2521269998124177631,C,goo
    "###);
}

#[tokio::test]
async fn test_collect_boolean_since_hourly() {
    insta::assert_snapshot!(QueryFixture::new("{ f1: Collect.b | collect(10, window=since(hourly())) | index(3) | when(is_valid($input)) }").run_to_csv(&collect_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,f1
    1996-12-20T00:42:00.000000000,9223372036854775808,12960666915911099378,A,false
    1996-12-20T00:42:57.000000000,9223372036854775808,12960666915911099378,A,false
    1996-12-20T00:43:57.000000000,9223372036854775808,12960666915911099378,A,false
    1996-12-20T01:00:00.000000000,18446744073709551615,12960666915911099378,A,false
    1996-12-21T00:43:57.000000000,9223372036854775808,2867199309159137213,B,false
    1996-12-21T00:44:57.000000000,9223372036854775808,2867199309159137213,B,false
    1996-12-21T01:00:00.000000000,18446744073709551615,2867199309159137213,B,false
    1996-12-22T00:47:57.000000000,9223372036854775808,2521269998124177631,C,true
    "###);
}

#[tokio::test]
async fn test_require_literal_max() {
    // TODO: We should figure out how to not report the second error -- type variables with
    // error propagation needs some fixing.
    insta::assert_yaml_snapshot!(QueryFixture::new("{ f1: Collect.s | collect(Collect.index) | index(1) }")
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
          - "  --> Query:1:27"
          - "  |"
          - "1 | { f1: Collect.s | collect(Collect.index) | index(1) }"
          - "  |                           ^^^^^^^^^^^^^ Argument 'max' to 'collect' must be constant, but was not"
          - ""
          - ""
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:44"
          - "  |"
          - "1 | { f1: Collect.s | collect(Collect.index) | index(1) }"
          - "  |                                            ^^^^^ Invalid types for parameter 'list' in call to 'index'"
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
