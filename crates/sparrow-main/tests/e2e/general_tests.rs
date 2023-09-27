//! Basic e2e tests for the general operators (`is_valid`, `hash`, etc).
/// Note that `preview_rows` is "at least n", not a hard limit, hence the
/// results here.
use crate::fixtures::{
    boolean_data_fixture, f64_data_fixture, i64_data_fixture, strings_data_fixture,
    timestamp_ns_data_fixture,
};
use crate::QueryFixture;

#[tokio::test]
async fn test_is_valid_i64() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, is_valid: is_valid(Numbers.m)}").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,is_valid
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,true
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24,true
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17,true
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,false
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12,true
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,false
    "###);
}

#[tokio::test]
async fn test_is_valid_f64() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, is_valid: is_valid(Numbers.m)}").run_to_csv(&f64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,is_valid
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5.2,true
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24.3,true
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17.6,true
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,false
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12.4,true
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,false
    "###);
}

#[tokio::test]
async fn test_is_valid_string() {
    insta::assert_snapshot!(QueryFixture::new("{ s: Strings.s, is_valid: is_valid(Strings.s)}").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,s,is_valid
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo,true
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,World,true
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,hello world,true
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,,true
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,,true
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,goodbye,true
    "###);
}

#[tokio::test]
async fn test_is_valid_boolean() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, is_valid: is_valid(Booleans.a)}").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,is_valid
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,true
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,false,true
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,,false
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,true,true
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,false,true
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,false,true
    1996-12-20T00:45:57.000000000,0,2867199309159137213,B,,false
    "###);
}

#[tokio::test]
async fn test_is_valid_timestamp_ns() {
    insta::assert_snapshot!(QueryFixture::new("{ n: Times.n, is_valid: is_valid(Times.n)}").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,is_valid
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,2,true
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,4,true
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,5,true
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,,false
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,8,true
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,23,true
    "###);
}

#[tokio::test]
async fn test_is_valid_record() {
    insta::assert_snapshot!(QueryFixture::new("{ is_valid: is_valid(Times)}").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,is_valid
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,true
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,true
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,true
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,true
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,true
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,true
    "###);
}

#[tokio::test]
async fn test_hash_i64() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, hash: hash(Numbers.m)}").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,hash
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,16461383214845928621
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24,11274228027825807126
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17,322098188319043992
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,0
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12,2287927947190353380
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,0
    "###);
}

#[tokio::test]
async fn test_hash_string() {
    insta::assert_snapshot!(QueryFixture::new("{ s: Strings.s, hash: hash(Strings.s)}").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,s,hash
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hEllo,7011413575603941612
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,World,13226470954278774291
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,hello world,10229417672155185436
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,,5663277146615294718
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,,5663277146615294718
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,goodbye,12405021407607093536
    "###);
}

#[tokio::test]
async fn test_hash_struct() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, hash: hash({m: Numbers.m})}").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,hash
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,328624516884178922
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24,11318067407944751383
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17,11917632967804650977
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,0
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12,10866357751204891869
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,0
    "###);
}

#[tokio::test]
async fn test_hash_boolean() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, hash: hash(Booleans.a)}").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,hash
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,18433805721903975440
    1996-12-20T00:40:57.000000000,0,2867199309159137213,B,false,11832085162654999889
    1996-12-20T00:41:57.000000000,0,2867199309159137213,B,,0
    1996-12-20T00:42:57.000000000,0,2867199309159137213,B,true,18433805721903975440
    1996-12-20T00:43:57.000000000,0,2867199309159137213,B,false,11832085162654999889
    1996-12-20T00:44:57.000000000,0,2867199309159137213,B,false,11832085162654999889
    1996-12-20T00:45:57.000000000,0,2867199309159137213,B,,0
    "###);
}

#[tokio::test]
async fn test_hash_timestamp_ns() {
    insta::assert_snapshot!(QueryFixture::new("{ n: Times.n, hash: hash(Times.n)}").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,hash
    1994-12-20T00:39:57.000000000,0,12960666915911099378,A,2,2694864431690786590
    1995-10-20T00:40:57.000000000,0,2867199309159137213,B,4,17062639839782733832
    1996-08-20T00:41:57.000000000,0,2867199309159137213,B,5,16461383214845928621
    1997-12-12T00:42:57.000000000,0,2867199309159137213,B,,0
    1998-12-13T00:43:57.000000000,0,2867199309159137213,B,8,6794973171266502674
    2004-12-06T00:44:57.000000000,0,2867199309159137213,B,23,5700754754056540783
    "###);
}

#[tokio::test]
#[ignore = "hash on records is unsupported"]
#[allow(unused_attributes)]
async fn test_hash_record() {
    insta::assert_snapshot!(QueryFixture::new("{ hash: hash(Times)}").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @"");
}

#[tokio::test]
async fn test_basic_limit_rows_to_1() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, is_valid: is_valid(Numbers.m)}").with_preview_rows(1).run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,is_valid
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,true
    "###);
}

#[tokio::test]
/// Set `preview_rows` to over the number of inputs. Verifies we don't stop
/// early
async fn test_basic_limit_rows_all() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, is_valid: is_valid(Numbers.m)}").with_preview_rows(100).run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,is_valid
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,5,true
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,24,true
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,17,true
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,,false
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12,true
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,false
    "###);
}

#[tokio::test]
async fn test_constant_evaluation_preserves_types() {
    // Regression test for ensuring simplifier does not incorrectly
    // equate null and booleans.
    insta::assert_snapshot!(QueryFixture::new("{ m1: if(false, Numbers.m), m2: Numbers.m }").with_final_results().run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m1,m2
    1996-12-20T00:40:02.000000001,18446744073709551615,2867199309159137213,B,,24
    1996-12-20T00:40:02.000000001,18446744073709551615,12960666915911099378,A,,
    "###);
}
