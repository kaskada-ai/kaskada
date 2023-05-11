//! Basic e2e tests for record creation and field-refs.

use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::fixture::DataFixture;
use crate::fixtures::strings_data_fixture;
use crate::QueryFixture;

#[tokio::test]
async fn test_record_creation() {
    insta::assert_snapshot!(QueryFixture::new("
        let record = { x: 5, y: \"hello\", z: Strings.s, n: Strings.n }
        let y = record.y
        let z = record.z
        in { n: record.x + record.n, y, z }").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,y,z
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,5,hello,hEllo
    1996-12-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,10,hello,World
    1996-12-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,3,hello,hello world
    1996-12-20T00:42:57.000000000,9223372036854775808,11753611437813598533,B,3,hello,
    1996-12-20T00:43:57.000000000,9223372036854775808,11753611437813598533,B,7,hello,
    1996-12-20T00:44:57.000000000,9223372036854775808,11753611437813598533,B,,hello,goodbye
    "###);
}

#[tokio::test]
async fn test_record_extension() {
    insta::assert_snapshot!(QueryFixture::new("
        let record = Strings | extend({ x: 5, y: \"hello\"})
        let y = record.y
        let z = record.s
        in { n: record.x + record.n, y, z }").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,y,z
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,5,hello,hEllo
    1996-12-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,10,hello,World
    1996-12-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,3,hello,hello world
    1996-12-20T00:42:57.000000000,9223372036854775808,11753611437813598533,B,3,hello,
    1996-12-20T00:43:57.000000000,9223372036854775808,11753611437813598533,B,7,hello,
    1996-12-20T00:44:57.000000000,9223372036854775808,11753611437813598533,B,,hello,goodbye
    "###);
}

#[tokio::test]
async fn test_record_extension_error() {
    insta::assert_yaml_snapshot!(QueryFixture::new("Strings.x | extend({y: 5})").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0001
        message: Illegal field reference
        formatted:
          - "error[E0001]: Illegal field reference"
          - "  --> Query:1:9"
          - "  |"
          - "1 | Strings.x | extend({y: 5})"
          - "  |         ^ No field named 'x'"
          - "  |"
          - "  = Nearest fields: 'n', 's', 't', 'key', 'time'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_record_extension_ordering() {
    insta::assert_snapshot!(QueryFixture::new("Strings | extend({ x: 5, y: \"hello\"})").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,x,y,time,subsort,key,s,n,t
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,5,hello,1996-12-19T16:39:57-08:00,0,A,hEllo,0,hEllo
    1996-12-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,5,hello,1996-12-19T16:40:57-08:00,0,B,World,5,world
    1996-12-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,5,hello,1996-12-19T16:41:57-08:00,0,B,hello world,-2,hello world
    1996-12-20T00:42:57.000000000,9223372036854775808,11753611437813598533,B,5,hello,1996-12-19T16:42:57-08:00,0,B,,-2,greetings
    1996-12-20T00:43:57.000000000,9223372036854775808,11753611437813598533,B,5,hello,1996-12-19T16:43:57-08:00,0,B,,2,salutations
    1996-12-20T00:44:57.000000000,9223372036854775808,11753611437813598533,B,5,hello,1996-12-19T16:44:57-08:00,0,B,goodbye,,
    "###);
}

#[tokio::test]
async fn test_record_removal() {
    insta::assert_snapshot!(QueryFixture::new("remove_fields(Strings, \"time\", \"subsort\")").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,key,s,n,t
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,A,hEllo,0,hEllo
    1996-12-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,B,World,5,world
    1996-12-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,B,hello world,-2,hello world
    1996-12-20T00:42:57.000000000,9223372036854775808,11753611437813598533,B,B,,-2,greetings
    1996-12-20T00:43:57.000000000,9223372036854775808,11753611437813598533,B,B,,2,salutations
    1996-12-20T00:44:57.000000000,9223372036854775808,11753611437813598533,B,B,goodbye,,
    "###)
}

#[tokio::test]
async fn test_record_removal_pipe() {
    insta::assert_snapshot!(QueryFixture::new("Strings | remove_fields($input, \"time\", \"subsort\")").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,key,s,n,t
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,A,hEllo,0,hEllo
    1996-12-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,B,World,5,world
    1996-12-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,B,hello world,-2,hello world
    1996-12-20T00:42:57.000000000,9223372036854775808,11753611437813598533,B,B,,-2,greetings
    1996-12-20T00:43:57.000000000,9223372036854775808,11753611437813598533,B,B,,2,salutations
    1996-12-20T00:44:57.000000000,9223372036854775808,11753611437813598533,B,B,goodbye,,
    "###)
}

#[tokio::test]
async fn test_record_select() {
    insta::assert_snapshot!(QueryFixture::new("select_fields(Strings, \"time\", \"s\")").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,time,s
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,1996-12-19T16:39:57-08:00,hEllo
    1996-12-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,1996-12-19T16:40:57-08:00,World
    1996-12-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,1996-12-19T16:41:57-08:00,hello world
    1996-12-20T00:42:57.000000000,9223372036854775808,11753611437813598533,B,1996-12-19T16:42:57-08:00,
    1996-12-20T00:43:57.000000000,9223372036854775808,11753611437813598533,B,1996-12-19T16:43:57-08:00,
    1996-12-20T00:44:57.000000000,9223372036854775808,11753611437813598533,B,1996-12-19T16:44:57-08:00,goodbye
    "###)
}

#[tokio::test]
async fn test_record_select_unused_key() {
    let data_fixture = DataFixture::new()
        .with_table_from_csv(
            TableConfig::new_with_table_source("Input", &Uuid::new_v4(), "time", None, "key", ""),
            indoc::indoc! {"
            time,key,a,b,c
            2021-01-01T00:00:00.000000000Z,A,5,1.2,true
            2021-01-02T00:00:00.000000000Z,A,6.3,0.4,false
            2021-03-01T00:00:00.000000000Z,B,,3.7,true
            2021-04-10T00:00:00.000000000Z,A,13,,true"},
        )
        .await
        .unwrap();

    insta::assert_snapshot!(QueryFixture::new("select_fields(Input, 'a', 'b')").run_to_csv(&data_fixture).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,a,b
    2021-01-01T00:00:00.000000000,1931361879321862014,3650215962958587783,A,5.0,1.2
    2021-01-02T00:00:00.000000000,1931361879321862015,3650215962958587783,A,6.3,0.4
    2021-03-01T00:00:00.000000000,1931361879321862016,11753611437813598533,B,,3.7
    2021-04-10T00:00:00.000000000,1931361879321862017,3650215962958587783,A,13.0,
    "###)
}

#[tokio::test]
async fn test_record_select_pipe() {
    insta::assert_snapshot!(QueryFixture::new("Strings | select_fields($input, \"time\", \"s\")").run_to_csv(&strings_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,time,s
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,1996-12-19T16:39:57-08:00,hEllo
    1996-12-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,1996-12-19T16:40:57-08:00,World
    1996-12-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,1996-12-19T16:41:57-08:00,hello world
    1996-12-20T00:42:57.000000000,9223372036854775808,11753611437813598533,B,1996-12-19T16:42:57-08:00,
    1996-12-20T00:43:57.000000000,9223372036854775808,11753611437813598533,B,1996-12-19T16:43:57-08:00,
    1996-12-20T00:44:57.000000000,9223372036854775808,11753611437813598533,B,1996-12-19T16:44:57-08:00,goodbye
    "###)
}

#[tokio::test]
async fn test_record_remove_non_record() {
    insta::assert_yaml_snapshot!(QueryFixture::new("remove_fields(Strings.s, \"time\", \"s\")").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:15"
          - "  |"
          - "1 | remove_fields(Strings.s, \"time\", \"s\")"
          - "  |               ^^^^^^^^^ Expected 'record' argument to 'remove_fields' to be a struct, but was string"
          - ""
          - ""
    "###)
}

#[tokio::test]
async fn test_record_remove_non_field() {
    insta::assert_yaml_snapshot!(QueryFixture::new("remove_fields(Strings, \"x\", \"s\")").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0001
        message: Illegal field reference
        formatted:
          - "error[E0001]: Illegal field reference"
          - "  --> Query:1:24"
          - "  |"
          - "1 | remove_fields(Strings, \"x\", \"s\")"
          - "  |                        ^^^ No field named 'x'"
          - "  |"
          - "  = Nearest fields: 'n', 's', 't', 'key', 'time'"
          - ""
          - ""
    "###)
}

#[tokio::test]
async fn test_record_remove_error_base() {
    insta::assert_yaml_snapshot!(QueryFixture::new("remove_fields(Strings.x, \"time\", \"s\")").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0001
        message: Illegal field reference
        formatted:
          - "error[E0001]: Illegal field reference"
          - "  --> Query:1:23"
          - "  |"
          - "1 | remove_fields(Strings.x, \"time\", \"s\")"
          - "  |                       ^ No field named 'x'"
          - "  |"
          - "  = Nearest fields: 'n', 's', 't', 'key', 'time'"
          - ""
          - ""
    "###)
}

#[tokio::test]
async fn test_record_remove_non_string_field() {
    insta::assert_yaml_snapshot!(QueryFixture::new("remove_fields(Strings, 54, \"s\")").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:24"
          - "  |"
          - "1 | remove_fields(Strings, 54, \"s\")"
          - "  |                        ^^ Argument 'field' to 'remove_fields' must be string, but was i64"
          - ""
          - ""
    "###)
}

#[tokio::test]
async fn test_remove_select_error_field() {
    insta::assert_yaml_snapshot!(QueryFixture::new("remove_fields(Strings, $input, \"s\")").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 2 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0006
        message: Unbound reference
        formatted:
          - "error[E0006]: Unbound reference"
          - "  --> Query:1:24"
          - "  |"
          - "1 | remove_fields(Strings, $input, \"s\")"
          - "  |                        ^^^^^^ No reference named '$input'"
          - "  |"
          - "  = Nearest matches: 'Strings'"
          - ""
          - ""
      - severity: error
        code: E0014
        message: Invalid non-constant argument
        formatted:
          - "error[E0014]: Invalid non-constant argument"
          - "  --> Query:1:24"
          - "  |"
          - "1 | remove_fields(Strings, $input, \"s\")"
          - "  |                        ^^^^^^ Argument 'field' to 'remove_fields' must be constant non-null string, but was not constant"
          - ""
          - ""
    "###)
}

#[tokio::test]
async fn test_record_remove_non_const_field() {
    insta::assert_yaml_snapshot!(QueryFixture::new("remove_fields(Strings, Strings.s, \"s\")").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0014
        message: Invalid non-constant argument
        formatted:
          - "error[E0014]: Invalid non-constant argument"
          - "  --> Query:1:24"
          - "  |"
          - "1 | remove_fields(Strings, Strings.s, \"s\")"
          - "  |                        ^^^^^^^^^ Argument 'field' to 'remove_fields' must be constant non-null string, but was not constant"
          - ""
          - ""
    "###)
}

#[tokio::test]
async fn test_record_select_non_record() {
    insta::assert_yaml_snapshot!(QueryFixture::new("select_fields(Strings.s, \"time\", \"s\")").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:15"
          - "  |"
          - "1 | select_fields(Strings.s, \"time\", \"s\")"
          - "  |               ^^^^^^^^^ Expected 'record' argument to 'select_fields' to be a struct, but was string"
          - ""
          - ""
    "###)
}

#[tokio::test]
async fn test_record_select_non_field() {
    insta::assert_yaml_snapshot!(QueryFixture::new("select_fields(Strings, \"x\", \"s\")").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0001
        message: Illegal field reference
        formatted:
          - "error[E0001]: Illegal field reference"
          - "  --> Query:1:24"
          - "  |"
          - "1 | select_fields(Strings, \"x\", \"s\")"
          - "  |                        ^^^ No field named 'x'"
          - "  |"
          - "  = Nearest fields: 'n', 's', 't', 'key', 'time'"
          - ""
          - ""
    "###)
}

#[tokio::test]
async fn test_record_select_error_base() {
    insta::assert_yaml_snapshot!(QueryFixture::new("select_fields(Strings.x, \"time\", \"s\")").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0001
        message: Illegal field reference
        formatted:
          - "error[E0001]: Illegal field reference"
          - "  --> Query:1:23"
          - "  |"
          - "1 | select_fields(Strings.x, \"time\", \"s\")"
          - "  |                       ^ No field named 'x'"
          - "  |"
          - "  = Nearest fields: 'n', 's', 't', 'key', 'time'"
          - ""
          - ""
    "###)
}

#[tokio::test]
async fn test_record_select_non_string_field() {
    insta::assert_yaml_snapshot!(QueryFixture::new("select_fields(Strings, 54, \"s\")").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:24"
          - "  |"
          - "1 | select_fields(Strings, 54, \"s\")"
          - "  |                        ^^ Argument 'field' to 'select_fields' must be string, but was i64"
          - ""
          - ""
    "###)
}

#[tokio::test]
async fn test_record_select_error_field() {
    insta::assert_yaml_snapshot!(QueryFixture::new("select_fields(Strings, $input, \"s\")").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 2 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0006
        message: Unbound reference
        formatted:
          - "error[E0006]: Unbound reference"
          - "  --> Query:1:24"
          - "  |"
          - "1 | select_fields(Strings, $input, \"s\")"
          - "  |                        ^^^^^^ No reference named '$input'"
          - "  |"
          - "  = Nearest matches: 'Strings'"
          - ""
          - ""
      - severity: error
        code: E0014
        message: Invalid non-constant argument
        formatted:
          - "error[E0014]: Invalid non-constant argument"
          - "  --> Query:1:24"
          - "  |"
          - "1 | select_fields(Strings, $input, \"s\")"
          - "  |                        ^^^^^^ Argument 'field' to 'select_fields' must be constant non-null string, but was not constant"
          - ""
          - ""
    "###)
}

#[tokio::test]
async fn test_record_select_non_const_field() {
    insta::assert_yaml_snapshot!(QueryFixture::new("select_fields(Strings, Strings.s, \"s\")").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0014
        message: Invalid non-constant argument
        formatted:
          - "error[E0014]: Invalid non-constant argument"
          - "  --> Query:1:24"
          - "  |"
          - "1 | select_fields(Strings, Strings.s, \"s\")"
          - "  |                        ^^^^^^^^^ Argument 'field' to 'select_fields' must be constant non-null string, but was not constant"
          - ""
          - ""
    "###)
}

#[tokio::test]
async fn test_empty_record() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ }").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0008
        message: Invalid arguments
        formatted:
          - "error[E0008]: Invalid arguments"
          - "  --> Query:1:1"
          - "  |"
          - "1 | { }"
          - "  | ^^^ Record must be non-empty"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_empty_record_in_extend() {
    insta::assert_yaml_snapshot!(QueryFixture::new("Strings | extend({})").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0008
        message: Invalid arguments
        formatted:
          - "error[E0008]: Invalid arguments"
          - "  --> Query:1:18"
          - "  |"
          - "1 | Strings | extend({})"
          - "  |                  ^^ Record must be non-empty"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_empty_record_in_remove() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ a: Strings.s } | remove_fields($input, 'a')").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0008
        message: Invalid arguments
        formatted:
          - "error[E0008]: Invalid arguments"
          - "  --> Query:1:20"
          - "  |"
          - "1 | { a: Strings.s } | remove_fields($input, 'a')"
          - "  |                    ^^^^^^^^^^^^^ Removed all fields from input record."
          - ""
          - ""
    "###);
}
