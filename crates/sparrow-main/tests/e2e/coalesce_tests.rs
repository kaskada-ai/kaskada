use crate::fixtures::{
    boolean_data_fixture, f64_data_fixture, i64_data_fixture, strings_data_fixture,
    timestamp_ns_data_fixture,
};
use crate::QueryFixture;

#[tokio::test]
async fn test_coalesce_two_boolean() {
    insta::assert_snapshot!(QueryFixture::new("{ a: Booleans.a, b: Booleans.b, coalesce_a_b: Booleans.a | coalesce($input, Booleans.b) }").run_to_csv(&boolean_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,b,coalesce_a_b
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,true,true,true
    1996-12-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,false,false,false
    1996-12-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,,true,true
    1996-12-20T00:42:57.000000000,9223372036854775808,11753611437813598533,B,true,false,true
    1996-12-20T00:43:57.000000000,9223372036854775808,11753611437813598533,B,false,true,false
    1996-12-20T00:44:57.000000000,9223372036854775808,11753611437813598533,B,false,,false
    1996-12-20T00:45:57.000000000,9223372036854775808,11753611437813598533,B,,,
    "###);
}

#[tokio::test]
async fn test_coalesce_zero() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ m: Numbers.m, coalesce_m: coalesce() }").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0008
        message: Invalid arguments
        formatted:
          - "error[E0008]: Invalid arguments"
          - "  --> Query:1:29"
          - "  |"
          - "1 | { m: Numbers.m, coalesce_m: coalesce() }"
          - "  |                             ^^^^^^^^ First unexpected argument"
          - "  |"
          - "  = Expected 1 but got 0"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_coalesce_one_i64() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, coalesce_m: coalesce(Numbers.m) }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,coalesce_m
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,5,5
    1996-12-20T00:39:58.000000000,9223372036854775808,11753611437813598533,B,24,24
    1996-12-20T00:39:59.000000000,9223372036854775808,3650215962958587783,A,17,17
    1996-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,,
    1996-12-20T00:40:01.000000000,9223372036854775808,3650215962958587783,A,12,12
    1996-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,,
    "###);
}

#[tokio::test]
async fn test_coalesce_one_i64_one_literal_i64() {
    insta::assert_snapshot!(QueryFixture::new("{ n: Times.n, coalesce_n_literal: coalesce(Times.n, 12345) }").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,coalesce_n_literal
    1994-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,2,2
    1995-10-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,4,4
    1996-08-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,5,5
    1997-12-12T00:42:57.000000000,9223372036854775808,11753611437813598533,B,,12345
    1998-12-13T00:43:57.000000000,9223372036854775808,11753611437813598533,B,8,8
    2004-12-06T00:44:57.000000000,9223372036854775808,11753611437813598533,B,23,23
    "###);
}

#[tokio::test]
async fn test_coalesce_one_i64_one_literal_f64() {
    insta::assert_snapshot!(QueryFixture::new("{ n: Times.n, coalesce_n_literal: coalesce(Times.n, 12345.7) }").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,coalesce_n_literal
    1994-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,2,2.0
    1995-10-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,4,4.0
    1996-08-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,5,5.0
    1997-12-12T00:42:57.000000000,9223372036854775808,11753611437813598533,B,,12345.7
    1998-12-13T00:43:57.000000000,9223372036854775808,11753611437813598533,B,8,8.0
    2004-12-06T00:44:57.000000000,9223372036854775808,11753611437813598533,B,23,23.0
    "###);
}

#[tokio::test]
async fn test_coalesce_two_i64() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, n: Numbers.n, coalesce_m_n: coalesce(Numbers.m, Numbers.n) }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,coalesce_m_n
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,5,10,5
    1996-12-20T00:39:58.000000000,9223372036854775808,11753611437813598533,B,24,3,24
    1996-12-20T00:39:59.000000000,9223372036854775808,3650215962958587783,A,17,6,17
    1996-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,,9,9
    1996-12-20T00:40:01.000000000,9223372036854775808,3650215962958587783,A,12,,12
    1996-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,,,
    "###);
}

#[tokio::test]
async fn test_coalesce_two_i64_one_literal() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, n: Numbers.n, coalesce_m_n: coalesce(Numbers.m, Numbers.n, 42) }").run_to_csv(&i64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,coalesce_m_n
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,5,10,5
    1996-12-20T00:39:58.000000000,9223372036854775808,11753611437813598533,B,24,3,24
    1996-12-20T00:39:59.000000000,9223372036854775808,3650215962958587783,A,17,6,17
    1996-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,,9,9
    1996-12-20T00:40:01.000000000,9223372036854775808,3650215962958587783,A,12,,12
    1996-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,,,42
    "###);
}

#[tokio::test]
async fn test_coalesce_two_f64() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, n: Numbers.n, coalesce_m_n: coalesce(Numbers.m, Numbers.n) }").run_to_csv(&f64_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,coalesce_m_n
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,5.2,10.0,5.2
    1996-12-20T00:39:58.000000000,9223372036854775808,11753611437813598533,B,24.3,3.9,24.3
    1996-12-20T00:39:59.000000000,9223372036854775808,3650215962958587783,A,17.6,6.2,17.6
    1996-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,,9.25,9.25
    1996-12-20T00:40:01.000000000,9223372036854775808,3650215962958587783,A,12.4,,12.4
    1996-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,,,
    "###);
}

#[tokio::test]
async fn test_coalesce_two_string() {
    insta::assert_snapshot!(QueryFixture::new("{ s: Strings.s, t: Strings.t, coalesce_s_t: coalesce(Strings.s, Strings.t) }").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,s,t,coalesce_s_t
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,hEllo,hEllo,hEllo
    1996-12-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,World,world,World
    1996-12-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,hello world,hello world,hello world
    1996-12-20T00:42:57.000000000,9223372036854775808,11753611437813598533,B,,greetings,
    1996-12-20T00:43:57.000000000,9223372036854775808,11753611437813598533,B,,salutations,
    1996-12-20T00:44:57.000000000,9223372036854775808,11753611437813598533,B,goodbye,,goodbye
    "###);
}

#[tokio::test]
async fn test_coalesce_two_timestamp_ns() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Times.m, n: Times.n, coalesce_m_n: coalesce(Times.m, Times.n) }").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,coalesce_m_n
    1994-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,4,2,4
    1995-10-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,3,4,3
    1996-08-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,,5,5
    1997-12-12T00:42:57.000000000,9223372036854775808,11753611437813598533,B,,,
    1998-12-13T00:43:57.000000000,9223372036854775808,11753611437813598533,B,8,8,8
    2004-12-06T00:44:57.000000000,9223372036854775808,11753611437813598533,B,11,23,11
    "###);
}

#[tokio::test]
async fn test_coalesce_two_record() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Times.m, n: Times.n, coalesce_times_times: coalesce(Times, Times) | $input.n }").run_to_csv(&timestamp_ns_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,coalesce_times_times
    1994-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,4,2,2
    1995-10-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,3,4,4
    1996-08-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,,5,5
    1997-12-12T00:42:57.000000000,9223372036854775808,11753611437813598533,B,,,
    1998-12-13T00:43:57.000000000,9223372036854775808,11753611437813598533,B,8,8,8
    2004-12-06T00:44:57.000000000,9223372036854775808,11753611437813598533,B,11,23,23
    "###);
}

#[tokio::test]
async fn test_coalesce_incompatible_types() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ m: Numbers.m, coalesce_m: coalesce(Numbers.m, \"hello\") }").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:29"
          - "  |"
          - "1 | { m: Numbers.m, coalesce_m: coalesce(Numbers.m, \"hello\") }"
          - "  |                             ^^^^^^^^ ---------  ------- Type: string"
          - "  |                             |        |           "
          - "  |                             |        Type: i64"
          - "  |                             Invalid types for call to 'coalesce'"
          - "  |"
          - "  = Expected 'any'"
          - ""
          - ""
    "###);
}
