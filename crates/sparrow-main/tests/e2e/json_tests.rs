//! Basic e2e tests for json functions.

use indoc::indoc;
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::fixture::DataFixture;
use crate::QueryFixture;

/// Create a simple table with a json string column 'json'.
///
/// This csv parser escapes quotes with double quotes.
pub(crate) async fn json_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new("Json", &Uuid::new_v4(), "time", Some("subsort"), "key", ""),
            indoc! { r#"
    time,subsort,key,json
    1996-12-19T16:39:57-08:00,0,A,"{""a"": 10, ""b"": ""dog""}"
    1996-12-19T16:40:57-08:00,0,B,"{""a"": 4, ""b"": ""lizard""}"
    1996-12-19T16:41:57-08:00,0,B,"{""a"": 1, ""c"": 3.3}"
    1996-12-19T16:42:57-08:00,0,B,"{""a"": 12, ""b"": ""cat""}"
    1996-12-19T16:43:57-08:00,0,A,"{""a"": 34}"
    1996-12-19T16:44:57-08:00,0,B,"{""a"": 6, ""b"": ""dog""}"
    "#},
        )
        .await
        .unwrap()
}

/// Create a simple table with a json string column 'json'.
/// Several rows  have incorrectly-formatted json.
///
/// This csv parser escapes quotes with double quotes.
pub(crate) async fn invalid_json_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new("Json", &Uuid::new_v4(), "time", Some("subsort"), "key", ""),
            indoc! { r#"
    time,subsort,key,json
    1996-12-19T16:39:57-08:00,0,A,"{a: 10, ""b"": ""dog""}"
    1996-12-19T16:40:57-08:00,0,B,"{""a"": 4, ""b": lizard""}"
    1996-12-19T16:41:57-08:00,0,B,"{""a"": 1, ""c"": 3.3}"
    1996-12-19T16:42:57-08:00,0,B,"{""a"": 12, ""b"": ""cat""}"
    1996-12-19T16:43:57-08:00,0,A,"{""a"", 34}"
    1996-12-19T16:44:57-08:00,0,B,"{""a"": 6, ""b"": ""dog""}"
    "#},
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_json_parses_field() {
    insta::assert_snapshot!(QueryFixture::new("let json = json(Json.json) in { a_test: json.a as i64, b_test: json(Json.json).b }").run_to_csv(&json_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a_test,b_test
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,10,dog
    1996-12-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,4,lizard
    1996-12-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,1,
    1996-12-20T00:42:57.000000000,9223372036854775808,11753611437813598533,B,12,cat
    1996-12-20T00:43:57.000000000,9223372036854775808,3650215962958587783,A,34,
    1996-12-20T00:44:57.000000000,9223372036854775808,11753611437813598533,B,6,dog
    "###)
}

#[tokio::test]
async fn test_json_string_field_usable_in_string_functions() {
    insta::assert_snapshot!(QueryFixture::new("let json = json(Json.json) in { string: json.b, len: len(json.b) }").run_to_csv(&json_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,string,len
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,dog,3
    1996-12-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,lizard,6
    1996-12-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,,
    1996-12-20T00:42:57.000000000,9223372036854775808,11753611437813598533,B,cat,3
    1996-12-20T00:43:57.000000000,9223372036854775808,3650215962958587783,A,,
    1996-12-20T00:44:57.000000000,9223372036854775808,11753611437813598533,B,dog,3
    "###)
}

#[tokio::test]
async fn test_json_field_number_as_string() {
    insta::assert_snapshot!(QueryFixture::new("let json = json(Json.json) in { num_as_str: json.a as string, len: len(json.a as string) }").run_to_csv(&json_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,num_as_str,len
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,10,2
    1996-12-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,4,1
    1996-12-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,1,1
    1996-12-20T00:42:57.000000000,9223372036854775808,11753611437813598533,B,12,2
    1996-12-20T00:43:57.000000000,9223372036854775808,3650215962958587783,A,34,2
    1996-12-20T00:44:57.000000000,9223372036854775808,11753611437813598533,B,6,1
    "###)
}

#[tokio::test]
async fn test_json_field_as_number_with_addition() {
    insta::assert_snapshot!(QueryFixture::new("let json = json(Json.json) in { a: json.a, plus_one: (json.a as i64) + 1 }").run_to_csv(&json_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,plus_one
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,10,11
    1996-12-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,4,5
    1996-12-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,1,2
    1996-12-20T00:42:57.000000000,9223372036854775808,11753611437813598533,B,12,13
    1996-12-20T00:43:57.000000000,9223372036854775808,3650215962958587783,A,34,35
    1996-12-20T00:44:57.000000000,9223372036854775808,11753611437813598533,B,6,7
    "###)
}

#[tokio::test]
async fn test_incorrect_json_format_produces_null() {
    // I guess this behavior is somewhat strange, in that creating a record with all
    // nulls produces nothing, while one non-null field in a record causes us to
    // print "null" in other fields.
    insta::assert_snapshot!(QueryFixture::new("let json = json(Json.json) in { a_test: json.a as i64, b_test: json(Json.json).b }").run_to_csv(&invalid_json_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a_test,b_test
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,,
    1996-12-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,,
    1996-12-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,1,
    1996-12-20T00:42:57.000000000,9223372036854775808,11753611437813598533,B,12,cat
    1996-12-20T00:43:57.000000000,9223372036854775808,3650215962958587783,A,,
    1996-12-20T00:44:57.000000000,9223372036854775808,11753611437813598533,B,6,dog
    "###)
}

#[tokio::test]
async fn test_json_of_json_object_errors() {
    insta::assert_yaml_snapshot!(QueryFixture::new("let json = json(Json.json) in { a: json(json) }").run_to_csv(&invalid_json_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:36"
          - "  |"
          - "1 | let json = json(Json.json) in { a: json(json) }"
          - "  |                                    ^^^^ ---- Actual type: json"
          - "  |                                    |     "
          - "  |                                    Invalid types for parameter 's' in call to 'json'"
          - "  |"
          - "  --> built-in signature 'json(s: string) -> json':1:9"
          - "  |"
          - "1 | json(s: string) -> json"
          - "  |         ------ Expected type: string"
          - ""
          - ""
    "###)
}

#[tokio::test]
async fn test_nested_json_produces_error() {
    // There's a way we can probably produce a better error message,
    // but, on the other hand, it's marked as an experimental feature, plus
    // this returns an error rather than incorrect results :shrug:
    //
    // The `dfg` would need to check if it recursively encounters the pattern
    // `(field_ref (json ?value ?op) ?field ?op)`
    insta::assert_yaml_snapshot!(QueryFixture::new("{ out: json(Json.json).a.b }").run_to_csv(&json_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0001
        message: Illegal field reference
        formatted:
          - "error[E0001]: Illegal field reference"
          - "  --> Query:1:8"
          - "  |"
          - "1 | { out: json(Json.json).a.b }"
          - "  |        ^^^^^^^^^^^^^^^^^ No fields for non-record base type string"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_json_as_output_field_produces_error() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ out: json(Json.json) }").run_to_csv(&json_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:8"
          - "  |"
          - "1 | { out: json(Json.json) }"
          - "  |        ^^^^^^^^^^^^^^^ Field 'out' has invalid type json"
          - ""
          - ""
    "###);
}
