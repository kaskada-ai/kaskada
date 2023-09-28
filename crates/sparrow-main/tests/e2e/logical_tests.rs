//! Basic e2e tests for the logical operators.

use crate::fixtures::{
    boolean_data_fixture, f64_data_fixture, i64_data_fixture, strings_data_fixture,
    timestamp_ns_data_fixture,
};
use crate::QueryFixture;

#[tokio::test]
async fn test_not_boolean() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, not_a: !Booleans.a }").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,not_a
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
async fn test_logical_or_boolean() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, b: Booleans.b, logical_or: Booleans.a or Booleans.b }").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,b,logical_or
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,true,true
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,false,false,false
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,,true,true
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,true,false,true
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,false,true,true
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,false,,
    1996-12-20T00:45:57.000000000,0,2867199309159137213,B,,,
    "###);
}

#[tokio::test]
async fn test_logical_and_boolean() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, b: Booleans.b, logical_and: Booleans.a and Booleans.b }").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,b,logical_and
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,true,true
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,false,false,false
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,,true,
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,true,false,false
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,false,true,false
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,false,,false
    1996-12-20T00:45:57.000000000,0,2867199309159137213,B,,,
    "###);
}

#[tokio::test]
async fn test_if_boolean() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, if_bool: Booleans.a | if(Booleans.a) }").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,if_bool
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,true
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,false,
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,true,true
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,false,
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,false,
    1996-12-20T00:45:57.000000000,0,2867199309159137213,B,,
    "###);
}

#[tokio::test]
async fn test_if_i64() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, if_i64: Numbers.m | if(Numbers.m == 5) }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,if_i64
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,5
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24,
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17,
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12,
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,
    "###);
}

#[tokio::test]
async fn test_if_f64() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, if_f64: Numbers.m | if(Numbers.m == 5.2) }").run_to_csv(&f64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,if_f64
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5.2,5.2
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24.3,
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17.6,
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12.4,
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,
    "###);
}

#[tokio::test]
async fn test_if_string() {
    insta::assert_snapshot!(QueryFixture::new("{ s: Strings.s, if_string: Strings.s | if(Strings.s == \"hEllo\") }").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,s,if_string
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo,hEllo
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,World,
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,hello world,
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,goodbye,
    "###);
}

#[tokio::test]
async fn test_if_timestamp_ns() {
    insta::assert_snapshot!(QueryFixture::new("{ t: Times.n, if_ts: Times.n | if(Times.key == \"B\") }").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,t,if_ts
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,2,
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,4,4
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,5,5
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,,
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,8,8
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,23,23
    "###);
}

#[tokio::test]
async fn test_if_record() {
    insta::assert_snapshot!(QueryFixture::new("{ t: Times.n, if_record: Times | if(Times.key == \"B\") | $input.n }").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,t,if_record
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,2,
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,4,4
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,5,5
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,,
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,8,8
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,23,23
    "###);
}

#[tokio::test]
async fn test_if_literal() {
    insta::assert_snapshot!(QueryFixture::new("{ if_literal: 1 | if(Times.key == \"B\") }").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,if_literal
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,1
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,1
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,1
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,1
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,1
    "###);
}

#[tokio::test]
async fn test_null_if_boolean() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, null_if_bool: Booleans.a | null_if(Booleans.a) }").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,null_if_bool
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,false,false
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,true,
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,false,false
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,false,false
    1996-12-20T00:45:57.000000000,0,2867199309159137213,B,,
    "###);
}

#[tokio::test]
async fn test_null_if_i64() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, null_if_i64: Numbers.m | null_if(Numbers.m == 5) }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,null_if_i64
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24,24
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17,17
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12,12
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,
    "###);
}

#[tokio::test]
async fn test_null_if_f64() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, null_if_f64: Numbers.m | null_if(Numbers.m == 5.2) }").run_to_csv(&f64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,null_if_f64
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5.2,
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24.3,24.3
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17.6,17.6
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12.4,12.4
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,
    "###);
}

#[tokio::test]
async fn test_null_if_string() {
    insta::assert_snapshot!(QueryFixture::new("{ s: Strings.s, null_if_string: Strings.s | null_if(Strings.s == \"hEllo\") }").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,s,null_if_string
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo,
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,World,World
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,hello world,hello world
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,,
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,goodbye,goodbye
    "###);
}

#[tokio::test]
async fn test_null_if_timestamp_ns() {
    insta::assert_snapshot!(QueryFixture::new("{ t: Times.n, null_if_ts: Times.n | null_if(Times.key == \"B\") }").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,t,null_if_ts
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,2,2
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,4,
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,5,
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,,
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,8,
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,23,
    "###);
}

#[tokio::test]
async fn test_null_if_record() {
    insta::assert_snapshot!(QueryFixture::new("{ n: Times.n, null_if_record: Times | null_if(Times.key == \"B\") | $input.n }").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,null_if_record
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,2,2
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,4,
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,5,
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,,
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,8,
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,23,
    "###);
}

#[tokio::test]
async fn test_null_if_literal() {
    insta::assert_snapshot!(QueryFixture::new("{ null_if_literal: 1 | null_if(Times.key == \"B\") }").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,null_if_literal
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,1
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,
    "###);
}

#[tokio::test]
async fn test_else_boolean() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, b: Booleans.b, a_else_b: Booleans.a | else(Booleans.b) }").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,b,a_else_b
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,true,true
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,false,false,false
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,,true,true
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,true,false,true
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,false,true,false
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,false,,false
    1996-12-20T00:45:57.000000000,0,2867199309159137213,B,,,
    "###);
}

#[tokio::test]
async fn test_else_i64() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, n: Numbers.n, m_else_n: Numbers.m | else(Numbers.n) }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,m_else_n
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,10,5
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24,3,24
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17,6,17
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,9,9
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12,,12
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,,
    "###);
}

#[tokio::test]
async fn test_else_f64() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, n: Numbers.n, m_else_n: Numbers.m | else(Numbers.n) }").run_to_csv(&f64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,m_else_n
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5.2,10.0,5.2
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24.3,3.9,24.3
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17.6,6.2,17.6
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,9.25,9.25
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12.4,,12.4
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,,
    "###);
}

#[tokio::test]
async fn test_else_string() {
    insta::assert_snapshot!(QueryFixture::new("{ s: Strings.s, t: Strings.t, s_else_t: Strings.s | else(Strings.t) }").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,s,t,s_else_t
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo,hEllo,hEllo
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,World,world,World
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,hello world,hello world,hello world
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,,greetings,
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,,salutations,
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,goodbye,,goodbye
    "###);
}

#[tokio::test]
async fn test_else_timestamp_ns() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Times.m, n: Times.n, m_else_n: Times.m | else(Times.n) }").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,m_else_n
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,4,2,4
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,3,4,3
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,,5,5
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,,,
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,8,8,8
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,11,23,11
    "###);
}

#[tokio::test]
async fn test_else_record() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Times.m, n: Times.n, times_else_times: Times | else(Times) | $input.n }").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,times_else_times
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,4,2,2
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,3,4,4
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,,5,5
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,,,
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,8,8,8
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,11,23,23
    "###);
}

#[tokio::test]
async fn test_else_literal() {
    insta::assert_snapshot!(QueryFixture::new("{ n: Times.n, n_else_literal: Times.n | else(12345) }").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,n_else_literal
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,2,2
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,4,4
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,5,5
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,,12345
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,8,8
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,23,23
    "###);
}

#[tokio::test]
async fn test_if_record_nested_is_valid_num_eq() {
    // Test for https://gitlab.com/kaskada/kaskada/-/issues/342
    insta::assert_snapshot!(QueryFixture::new("Times | extend({gr_5: $input.n > 5}) | if(Times.n > 5) | when(is_valid($input.key))").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,gr_5,time,subsort,key,n,m,other_time,fruit
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,true,1998-12-13T00:43:57.000000000,0,B,8,8,,
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,true,2004-12-06T00:44:57.000000000,0,B,23,11,1994-12-20T00:39:57.000000000,mango
    "###);
}

#[tokio::test]
async fn test_if_record_nested_is_valid_string_eq() {
    insta::assert_snapshot!(QueryFixture::new("Times | extend({eq_A: len($input.fruit) > 6}) | if (len(Times.fruit) > 6) | when(is_valid($input.key))").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,eq_A,time,subsort,key,n,m,other_time,fruit
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,true,1995-10-20T00:40:57.000000000,0,B,4,3,1994-11-20T00:39:57.000000000,watermelon
    "###);
}

#[tokio::test]
async fn test_if_null_condition_number() {
    insta::assert_snapshot!(QueryFixture::new("Times | if ($input.n > 0) | extend({ cond: Times.n > 0 })").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,cond,time,subsort,key,n,m,other_time,fruit
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,true,1994-12-20T00:39:57.000000000,0,A,2,4,2003-12-20T00:39:57.000000000,pear
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,true,1995-10-20T00:40:57.000000000,0,B,4,3,1994-11-20T00:39:57.000000000,watermelon
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,true,1996-08-20T00:41:57.000000000,0,B,5,,1998-12-20T00:39:57.000000000,mango
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,,,,,,,,
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,true,1998-12-13T00:43:57.000000000,0,B,8,8,,
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,true,2004-12-06T00:44:57.000000000,0,B,23,11,1994-12-20T00:39:57.000000000,mango
    "###);
}

#[tokio::test]
async fn test_if_null_condition_string_equality() {
    insta::assert_snapshot!(QueryFixture::new("Times | if ($input.fruit == \"mango\")").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,time,subsort,key,n,m,other_time,fruit
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,,,,,,,
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,,,,,,,
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,1996-08-20T00:41:57.000000000,0,B,5,,1998-12-20T00:39:57.000000000,mango
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,,,,,,,
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,,,,,,,
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,2004-12-06T00:44:57.000000000,0,B,23,11,1994-12-20T00:39:57.000000000,mango
    "###);
}

#[tokio::test]
async fn test_null_if_null_condition() {
    insta::assert_snapshot!(QueryFixture::new("Times | null_if ($input.n > 6)").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,time,subsort,key,n,m,other_time,fruit
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,1994-12-20T00:39:57.000000000,0,A,2,4,2003-12-20T00:39:57.000000000,pear
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,1995-10-20T00:40:57.000000000,0,B,4,3,1994-11-20T00:39:57.000000000,watermelon
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,1996-08-20T00:41:57.000000000,0,B,5,,1998-12-20T00:39:57.000000000,mango
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,,,,,,,
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,,,,,,,
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,,,,,,,
    "###);
}

#[tokio::test]
async fn test_null_if_condition_null_values() {
    // Ensure that null rows are produced
    insta::assert_snapshot!(QueryFixture::new("Times | if ($input.n < 0)").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,time,subsort,key,n,m,other_time,fruit
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,,,,,,,
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,,,,,,,
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,,,,,,,
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,,,,,,,
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,,,,,,,
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,,,,,,,
    "###);
}
