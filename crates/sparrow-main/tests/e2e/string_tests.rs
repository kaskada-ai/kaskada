//! Basic e2e tests for string operators.

use crate::fixtures::strings_data_fixture;
use crate::QueryFixture;

#[tokio::test]
async fn test_len() {
    insta::assert_snapshot!(QueryFixture::new("{ len: len(Strings.s)}").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,len
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,5
    1996-12-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,5
    1996-12-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,11
    1996-12-20T00:42:57.000000000,9223372036854775808,11753611437813598533,B,0
    1996-12-20T00:43:57.000000000,9223372036854775808,11753611437813598533,B,0
    1996-12-20T00:44:57.000000000,9223372036854775808,11753611437813598533,B,7
    "###);
}

#[tokio::test]
async fn test_upper_len() {
    insta::assert_snapshot!(QueryFixture::new("{ upper: upper(Strings.s)}").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,upper
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,HELLO
    1996-12-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,WORLD
    1996-12-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,HELLO WORLD
    1996-12-20T00:42:57.000000000,9223372036854775808,11753611437813598533,B,
    1996-12-20T00:43:57.000000000,9223372036854775808,11753611437813598533,B,
    1996-12-20T00:44:57.000000000,9223372036854775808,11753611437813598533,B,GOODBYE
    "###);
}

#[tokio::test]
async fn test_lower_len() {
    insta::assert_snapshot!(QueryFixture::new("{ lower: lower(Strings.s)}").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,lower
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,hello
    1996-12-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,world
    1996-12-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,hello world
    1996-12-20T00:42:57.000000000,9223372036854775808,11753611437813598533,B,
    1996-12-20T00:43:57.000000000,9223372036854775808,11753611437813598533,B,
    1996-12-20T00:44:57.000000000,9223372036854775808,11753611437813598533,B,goodbye
    "###);
}

#[tokio::test]
async fn test_substring() {
    insta::assert_snapshot!(QueryFixture::new("{ substring_0_2: substring(Strings.s, start=0, end=2)
                , substring_1: substring(Strings.s, start=1)
                , substring_0_i: substring(Strings.s, end=Strings.n)
                , substring_i: substring(Strings.s, start=Strings.n),
                }").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,substring_0_2,substring_1,substring_0_i,substring_i
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,hE,Ello,,hEllo
    1996-12-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,Wo,orld,World,
    1996-12-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,he,ello world,hello wor,ld
    1996-12-20T00:42:57.000000000,9223372036854775808,11753611437813598533,B,,,,
    1996-12-20T00:43:57.000000000,9223372036854775808,11753611437813598533,B,,,,
    1996-12-20T00:44:57.000000000,9223372036854775808,11753611437813598533,B,go,oodbye,goodbye,goodbye
    "###);
}
