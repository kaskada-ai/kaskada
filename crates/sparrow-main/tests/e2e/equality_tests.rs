//! Basic e2e tests for the comparison operators.
// eq / neq: i64, f64, string, record, boolean, timestamp, literal

use crate::fixtures::{
    boolean_data_fixture, f64_data_fixture, i64_data_fixture, strings_data_fixture,
    timestamp_ns_data_fixture,
};
use crate::QueryFixture;

#[tokio::test]
async fn test_eq_i64() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, n: Numbers.n, eq: Numbers.m == Numbers.n }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,eq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,10,false
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24,3,false
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17,6,false
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,9,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12,,
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,,
    "###);
}

#[tokio::test]
async fn test_eq_f64() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, n: Numbers.n, eq: Numbers.m == Numbers.n }").run_to_csv(&f64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,eq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5.2,10.0,false
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24.3,3.9,false
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17.6,6.2,false
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,9.25,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12.4,,
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,,
    "###);
}

#[tokio::test]
async fn test_eq_timestamp_ns() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Times.m, n: Times.n, eq: (Times.m as timestamp_ns) == (Times.n as timestamp_ns) }").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,eq
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,4,2,false
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,3,4,false
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,,5,
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,,,
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,8,8,true
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,11,23,false
    "###);
}

#[tokio::test]
async fn test_eq_boolean() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, b: Booleans.b, eq: Booleans.a == Booleans.b }").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,b,eq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,true,true
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,false,false,true
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,,true,
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,true,false,false
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,false,true,false
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,false,,
    1996-12-20T00:45:57.000000000,0,2867199309159137213,B,,,
    "###);
}

#[tokio::test]
async fn test_eq_string() {
    insta::assert_snapshot!(QueryFixture::new("{ s: Strings.s, t: Strings.t, eq: Strings.s == Strings.t }").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,s,t,eq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo,hEllo,true
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,World,world,false
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,hello world,hello world,true
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,,greetings,false
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,,salutations,false
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,goodbye,,false
    "###);
}

#[tokio::test]
#[ignore = "https://gitlab.com/kaskada/kaskada/-/issues/127"]
async fn test_eq_record() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, n: Numbers.n, eq: { a: Numbers.m, b: Numbers.n } == { a: Numbers.n, b: Numbers.n } }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,gte
    1994-12-20T00:39:57.000000000,0,16072519723445549088,true
    1995-10-20T00:40:57.000000000,0,18113259709342437355,false
    1996-08-20T00:41:57.000000000,0,18113259709342437355,
    1997-12-12T00:42:57.000000000,0,18113259709342437355,
    1998-12-13T00:43:57.000000000,0,18113259709342437355,true
    2004-12-06T00:44:57.000000000,0,18113259709342437355,false
    "###);
}

#[tokio::test]
async fn test_eq_i64_literal() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, eq: Numbers.m == 10 }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,eq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,false
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24,false
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17,false
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12,false
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,
    "###);
}

#[tokio::test]
async fn test_eq_i64_literal_converse() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, eq: 10 == Numbers.m }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,eq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,false
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24,false
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17,false
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12,false
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,
    "###);
}

#[tokio::test]
async fn test_eq_i64_literal_null() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, eq: Numbers.m == null }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,eq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24,
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17,
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12,
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,
    "###);
}

#[tokio::test]
async fn test_eq_i64_literal_null_converse() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, eq: null == Numbers.m }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,eq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24,
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17,
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12,
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,
    "###);
}

#[tokio::test]
async fn test_eq_f64_literal() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, eq: Numbers.m == 24.3 }").run_to_csv(&f64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,eq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5.2,false
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24.3,true
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17.6,false
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12.4,false
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,
    "###);
}

#[tokio::test]
async fn test_eq_f64_literal_zero() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, eq: Numbers.m == 0.0 }").run_to_csv(&f64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,eq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5.2,false
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24.3,false
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17.6,false
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12.4,false
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,
    "###);
}

#[tokio::test]
async fn test_eq_timestamp_ns_literal() {
    insta::assert_snapshot!(QueryFixture::new("
        let m_time = Times.m as timestamp_ns
        let literal = \"1970-01-01T00:00:00.000000008Z\"
        in { m_time, literal_time: literal as timestamp_ns, lt: m_time == literal}").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m_time,literal_time,lt
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,1970-01-01T00:00:00.000000004,1970-01-01T00:00:00.000000008,false
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,1970-01-01T00:00:00.000000003,1970-01-01T00:00:00.000000008,false
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,,1970-01-01T00:00:00.000000008,
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,,1970-01-01T00:00:00.000000008,
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,1970-01-01T00:00:00.000000008,1970-01-01T00:00:00.000000008,true
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,1970-01-01T00:00:00.000000011,1970-01-01T00:00:00.000000008,false
    "###);
}

#[tokio::test]
async fn test_eq_boolean_literal() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, eq: Booleans.a == true }").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,eq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,true
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,false,false
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,true,true
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,false,false
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,false,false
    1996-12-20T00:45:57.000000000,0,2867199309159137213,B,,
    "###);
}

#[tokio::test]
async fn test_eq_boolean_literal_null() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, eq: Booleans.a == null }").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,eq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,false,
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,true,
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,false,
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,false,
    1996-12-20T00:45:57.000000000,0,2867199309159137213,B,,
    "###);
}

#[tokio::test]
async fn test_eq_boolean_literal_converse() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, eq: true == Booleans.a }").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,eq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,true
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,false,false
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,true,true
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,false,false
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,false,false
    1996-12-20T00:45:57.000000000,0,2867199309159137213,B,,
    "###);
}

#[tokio::test]
async fn test_eq_boolean_literal_null_converse() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, eq: null == Booleans.a }").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,eq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,false,
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,true,
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,false,
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,false,
    1996-12-20T00:45:57.000000000,0,2867199309159137213,B,,
    "###);
}

#[tokio::test]
async fn test_eq_string_literal() {
    insta::assert_snapshot!(QueryFixture::new("let eq = Strings.s == \"hello world\" in { s: Strings.s, eq, is_valid: is_valid(eq)}").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,s,eq,is_valid
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo,false,true
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,World,false,true
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,hello world,true,true
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,,false,true
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,,false,true
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,goodbye,false,true
    "###);
}

#[tokio::test]
async fn test_eq_string_literal_converse() {
    insta::assert_snapshot!(QueryFixture::new("let eq = \"hello world\" == Strings.s in { s: Strings.s, eq, is_valid: is_valid(eq)}").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,s,eq,is_valid
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo,false,true
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,World,false,true
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,hello world,true,true
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,,false,true
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,,false,true
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,goodbye,false,true
    "###);
}

#[tokio::test]
async fn test_eq_string_literal_null() {
    insta::assert_snapshot!(QueryFixture::new("{ s: Strings.s, eq: Strings.s == null }").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,s,eq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo,
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,World,
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,hello world,
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,goodbye,
    "###);
}

#[tokio::test]
async fn test_eq_string_literal_null_converse() {
    insta::assert_snapshot!(QueryFixture::new("{ s: Strings.s, eq: null == Strings.s }").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,s,eq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo,
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,World,
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,hello world,
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,goodbye,
    "###);
}

#[tokio::test]
#[ignore = "https://gitlab.com/kaskada/kaskada/-/issues/127"]
async fn test_eq_record_literal() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Numbers.m, b: Numbers.n, eq: { a: Numbers.m, b: Numbers.n } == { a: 5, b: 10 } }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,gte
    1994-12-20T00:39:57.000000000,0,16072519723445549088,true
    1995-10-20T00:40:57.000000000,0,18113259709342437355,false
    1996-08-20T00:41:57.000000000,0,18113259709342437355,
    1997-12-12T00:42:57.000000000,0,18113259709342437355,
    1998-12-13T00:43:57.000000000,0,18113259709342437355,true
    2004-12-06T00:44:57.000000000,0,18113259709342437355,false
    "###);
}

#[tokio::test]
async fn test_neq_i64_old() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, n: Numbers.n, neq: Numbers.m <> Numbers.n }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,neq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,10,true
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24,3,true
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17,6,true
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,9,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12,,
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,,
    "###);
}

#[tokio::test]
async fn test_neq_i64_new() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, n: Numbers.n, neq: Numbers.m != Numbers.n }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,neq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,10,true
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24,3,true
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17,6,true
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,9,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12,,
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,,
    "###);
}

#[tokio::test]
async fn test_neq_f64() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, n: Numbers.n, neq: Numbers.m <> Numbers.n}").run_to_csv(&f64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,neq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5.2,10.0,true
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24.3,3.9,true
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17.6,6.2,true
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,9.25,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12.4,,
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,,
    "###);
}

#[tokio::test]
async fn test_neq_timestamp_ns() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Times.m, n: Times.n, neq: (Times.m as timestamp_ns) <> (Times.n as timestamp_ns) }").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,neq
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,4,2,true
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,3,4,true
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,,5,
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,,,
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,8,8,false
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,11,23,true
    "###);
}

#[tokio::test]
async fn test_neq_boolean() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, b: Booleans.b, neq: Booleans.a <> Booleans.b}").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,b,neq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,true,false
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,false,false,false
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,,true,
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,true,false,true
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,false,true,true
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,false,,
    1996-12-20T00:45:57.000000000,0,2867199309159137213,B,,,
    "###);
}

#[tokio::test]
async fn test_neq_string() {
    insta::assert_snapshot!(QueryFixture::new("{ s: Strings.s, t: Strings.t, neq: Strings.s <> Strings.t}").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,s,t,neq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo,hEllo,false
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,World,world,true
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,hello world,hello world,false
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,,greetings,true
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,,salutations,true
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,goodbye,,true
    "###);
}

#[tokio::test]
#[ignore = "https://gitlab.com/kaskada/kaskada/-/issues/127"]
async fn test_neq_record() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Numbers.m, b: Numbers.n, neq: { a: Numbers.m, b: Numbers.n } <> { a: Numbers.n, b: Numbers.n } }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,gte
    1994-12-20T00:39:57.000000000,0,16072519723445549088,true
    1995-10-20T00:40:57.000000000,0,18113259709342437355,false
    1996-08-20T00:41:57.000000000,0,18113259709342437355,
    1997-12-12T00:42:57.000000000,0,18113259709342437355,
    1998-12-13T00:43:57.000000000,0,18113259709342437355,true
    2004-12-06T00:44:57.000000000,0,18113259709342437355,false
    "###);
}

#[tokio::test]
async fn test_neq_i64_literal() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, neq: Numbers.m <> 5 }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,neq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,false
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24,true
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17,true
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12,true
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,
    "###);
}

#[tokio::test]
async fn test_neq_i64_literal_converse() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, neq: 5 != Numbers.m }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,neq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,false
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24,true
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17,true
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12,true
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,
    "###);
}

#[tokio::test]
async fn test_neq_i64_literal_null() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, neq: Numbers.m != null }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,neq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24,
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17,
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12,
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,
    "###);
}

#[tokio::test]
async fn test_neq_i64_literal_null_converse() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, neq: null != Numbers.m }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,neq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24,
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17,
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12,
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,
    "###);
}

#[tokio::test]
async fn test_neq_f64_literal() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, neq: Numbers.m <> 5.2}").run_to_csv(&f64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,neq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5.2,false
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24.3,true
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17.6,true
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12.4,true
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,
    "###);
}

#[tokio::test]
async fn test_neq_timestamp_ns_literal() {
    insta::assert_snapshot!(QueryFixture::new("
        let m_time = Times.m as timestamp_ns
        let literal = \"1970-01-01T00:00:00.000000008Z\"
        in { m_time, literal_time: literal as timestamp_ns, neq: m_time <> literal}").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m_time,literal_time,neq
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,1970-01-01T00:00:00.000000004,1970-01-01T00:00:00.000000008,true
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,1970-01-01T00:00:00.000000003,1970-01-01T00:00:00.000000008,true
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,,1970-01-01T00:00:00.000000008,
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,,1970-01-01T00:00:00.000000008,
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,1970-01-01T00:00:00.000000008,1970-01-01T00:00:00.000000008,false
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,1970-01-01T00:00:00.000000011,1970-01-01T00:00:00.000000008,true
    "###);
}

#[tokio::test]
async fn test_neq_boolean_literal() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, neq: Booleans.a <> true }").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,neq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,false
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,false,true
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,true,false
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,false,true
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,false,true
    1996-12-20T00:45:57.000000000,0,2867199309159137213,B,,
    "###);
}

#[tokio::test]
async fn test_neq_boolean_literal_converse() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, neq: true != Booleans.a }").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,neq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,false
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,false,true
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,true,false
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,false,true
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,false,true
    1996-12-20T00:45:57.000000000,0,2867199309159137213,B,,
    "###);
}

#[tokio::test]
async fn test_neq_boolean_literal_null() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, neq: Booleans.a <> null }").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,neq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,false,
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,true,
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,false,
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,false,
    1996-12-20T00:45:57.000000000,0,2867199309159137213,B,,
    "###);
}

#[tokio::test]
async fn test_neq_boolean_literal_null_converse() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, neq: null != Booleans.a }").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,neq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,false,
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,true,
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,false,
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,false,
    1996-12-20T00:45:57.000000000,0,2867199309159137213,B,,
    "###);
}

#[tokio::test]
async fn test_neq_string_literal() {
    insta::assert_snapshot!(QueryFixture::new("{ s: Strings.s,  neq: Strings.s <> \"hello world\" }").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,s,neq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo,true
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,World,true
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,hello world,false
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,,true
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,,true
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,goodbye,true
    "###);
}

#[tokio::test]
async fn test_neq_string_literal_converse() {
    insta::assert_snapshot!(QueryFixture::new("{ s: Strings.s,  neq: \"hello world\" != Strings.s }").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,s,neq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo,true
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,World,true
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,hello world,false
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,,true
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,,true
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,goodbye,true
    "###);
}

#[tokio::test]
async fn test_neq_string_literal_null() {
    insta::assert_snapshot!(QueryFixture::new("{ s: Strings.s,  neq: Strings.s <> null }").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,s,neq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo,
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,World,
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,hello world,
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,goodbye,
    "###);
}

#[tokio::test]
async fn test_neq_string_literal_null_converse() {
    insta::assert_snapshot!(QueryFixture::new("{ s: Strings.s,  neq: null != Strings.s }").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,s,neq
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo,
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,World,
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,hello world,
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,goodbye,
    "###);
}

#[tokio::test]
#[ignore = "https://gitlab.com/kaskada/kaskada/-/issues/127"]
async fn test_neq_record_literal() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Numbers.m, b: Numbers.n, neq: { a: Numbers.m, b: Numbers.n } <> { a: 5, b: 10 } }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,gte
    1994-12-20T00:39:57.000000000,0,16072519723445549088,true
    1995-10-20T00:40:57.000000000,0,18113259709342437355,false
    1996-08-20T00:41:57.000000000,0,18113259709342437355,
    1997-12-12T00:42:57.000000000,0,18113259709342437355,
    1998-12-13T00:43:57.000000000,0,18113259709342437355,true
    2004-12-06T00:44:57.000000000,0,18113259709342437355,false
    "###);
}
