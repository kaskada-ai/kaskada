//! e2e tests for basic errors.
//!
//! More specific error messages related to specific operations should
//! go in more specific locations, as appropriate.

use arrow::datatypes::{DataType, Field, Schema};
use sparrow_api::kaskada::v1alpha::{TableConfig, TableMetadata};
use uuid::Uuid;

use crate::fixtures::{
    collection_data_fixture, f64_data_fixture, i64_data_fixture, strings_data_fixture,
};
use crate::{DataFixture, QueryFixture};

#[tokio::test]
async fn test_undefined_column() {
    // The cast is here to ensure that we don't report an additional
    // error when casting from error to i64.

    insta::assert_yaml_snapshot!(QueryFixture::new("{ undefined: Numbers.undefined as i64}").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0001
        message: Illegal field reference
        formatted:
          - "error[E0001]: Illegal field reference"
          - "  --> Query:1:22"
          - "  |"
          - "1 | { undefined: Numbers.undefined as i64}"
          - "  |                      ^^^^^^^^^ No field named 'undefined'"
          - "  |"
          - "  = Nearest fields: 'time', 'key', 'n', 'm', 'subsort'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_undefined_column_last() {
    // Regression test for the code path that handles undefined fields
    // causing the diagnostic to be swallowed.
    insta::assert_yaml_snapshot!(QueryFixture::new("{ undefined: Numbers.undefined} | last()").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0001
        message: Illegal field reference
        formatted:
          - "error[E0001]: Illegal field reference"
          - "  --> Query:1:22"
          - "  |"
          - "1 | { undefined: Numbers.undefined} | last()"
          - "  |                      ^^^^^^^^^ No field named 'undefined'"
          - "  |"
          - "  = Nearest fields: 'time', 'key', 'n', 'm', 'subsort'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_undefined_column_field_ref() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ undefined: Numbers.undefined.foo }").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0001
        message: Illegal field reference
        formatted:
          - "error[E0001]: Illegal field reference"
          - "  --> Query:1:22"
          - "  |"
          - "1 | { undefined: Numbers.undefined.foo }"
          - "  |                      ^^^^^^^^^ No field named 'undefined'"
          - "  |"
          - "  = Nearest fields: 'time', 'key', 'n', 'm', 'subsort'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_undefined_column_field_addition() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ undefined: Numbers.undefined + 5 }").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0001
        message: Illegal field reference
        formatted:
          - "error[E0001]: Illegal field reference"
          - "  --> Query:1:22"
          - "  |"
          - "1 | { undefined: Numbers.undefined + 5 }"
          - "  |                      ^^^^^^^^^ No field named 'undefined'"
          - "  |"
          - "  = Nearest fields: 'time', 'key', 'n', 'm', 'subsort'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_table_no_source_files() {
    // A file-backed table with no paths should still be checked
    // against the schema.
    let schema = Schema::new(vec![
        Field::new("time", DataType::Utf8, false),
        Field::new("subsort", DataType::Int64, false),
        Field::new("group", DataType::Int64, false),
    ]);
    let data_fixture = DataFixture::new().with_table_metadata(
        TableConfig::new_with_table_source(
            "Sent",
            &Uuid::new_v4(),
            "time",
            Some("subsort"),
            "group",
            "",
        ),
        TableMetadata {
            schema: Some((&schema).try_into().unwrap()),
            file_count: 0,
        },
    );
    let error = QueryFixture::new("{ x: Sent.x }")
        .run_to_csv(&data_fixture)
        .await
        .unwrap_err();

    insta::assert_yaml_snapshot!(error, @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0001
        message: Illegal field reference
        formatted:
          - "error[E0001]: Illegal field reference"
          - "  --> Query:1:11"
          - "  |"
          - "1 | { x: Sent.x }"
          - "  |           ^ No field named 'x'"
          - "  |"
          - "  = Nearest fields: 'time', 'group', 'subsort'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_invalid_key_columns() {
    let schema = Schema::new(vec![
        Field::new("time", DataType::Utf8, false),
        Field::new("subsort", DataType::Int64, false),
        Field::new("key", DataType::Int64, false),
    ]);
    let data_fixture = DataFixture::new().with_table_metadata(
        TableConfig::new_with_table_source(
            "Miscapitalized",
            &Uuid::new_v4(),
            "Time",
            Some("Subsort"),
            "Key",
            "",
        ),
        TableMetadata {
            schema: Some((&schema).try_into().unwrap()),
            file_count: 0,
        },
    );
    let error = QueryFixture::new("{ shouldnt_matter }")
        .run_to_csv(&data_fixture)
        .await
        .unwrap_err();

    insta::assert_yaml_snapshot!(error, @r###"
    ---
    code: Internal error
    message: compilation error
    "###);
}

#[tokio::test]
async fn test_illegal_cast_to_generic() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ m: Numbers.m as number }").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0002
        message: Illegal cast
        formatted:
          - "error[E0002]: Illegal cast"
          - "  --> Query:1:19"
          - "  |"
          - "1 | { m: Numbers.m as number }"
          - "  |                   ^^^^^^ Unable to cast to type 'number'"
          - "  |"
          - "  = From type i64"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_illegal_cast() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ n: Numbers.key as duration_ns }").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0002
        message: Illegal cast
        formatted:
          - "error[E0002]: Illegal cast"
          - "  --> Query:1:21"
          - "  |"
          - "1 | { n: Numbers.key as duration_ns }"
          - "  |                     ^^^^^^^^^^^ Unable to cast to type 'duration_ns'"
          - "  |"
          - "  = From type string"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_unrecognized_function() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ n: cel(Numbers.n) }").run_to_csv(&f64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0007
        message: Undefined function
        formatted:
          - "error[E0007]: Undefined function"
          - "  --> Query:1:6"
          - "  |"
          - "1 | { n: cel(Numbers.n) }"
          - "  |      ^^^ No function named 'cel'"
          - "  |"
          - "  = Nearest matches: 'ceil', 'eq', 'len', 'mul', 'neg'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_duplicate_fields() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ n: Numbers.n, n: Numbers.n, m: Numbers.m, m: Numbers.m }").run_to_csv(&f64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 2 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0009
        message: Duplicate field names in record expression
        formatted:
          - "error[E0009]: Duplicate field names in record expression"
          - "  --> Query:1:31"
          - "  |"
          - "1 | { n: Numbers.n, n: Numbers.n, m: Numbers.m, m: Numbers.m }"
          - "  |                               ^             -"
          - "  |                               |              "
          - "  |                               Field 'm' defined multiple times"
          - ""
          - ""
      - severity: error
        code: E0009
        message: Duplicate field names in record expression
        formatted:
          - "error[E0009]: Duplicate field names in record expression"
          - "  --> Query:1:3"
          - "  |"
          - "1 | { n: Numbers.n, n: Numbers.n, m: Numbers.m, m: Numbers.m }"
          - "  |   ^             -"
          - "  |   |              "
          - "  |   Field 'n' defined multiple times"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_non_record_base_to_extension() {
    // Note - this test case is both a non-record *base* and a non-record extension.
    // Ideally, we'd report *both*, but currently we report the non-record extension
    // first.
    insta::assert_yaml_snapshot!(QueryFixture::new("Numbers.n | extend(Numbers.m)").run_to_csv(&f64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:20"
          - "  |"
          - 1 | Numbers.n | extend(Numbers.m)
          - "  |                    ^^^^^^^^^ extension argument to extend must be record, but was f64"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_non_record_extension() {
    insta::assert_yaml_snapshot!(QueryFixture::new("Numbers | extend(Numbers.m)").run_to_csv(&f64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:18"
          - "  |"
          - 1 | Numbers | extend(Numbers.m)
          - "  |                  ^^^^^^^^^ extension argument to extend must be record, but was f64"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_concrete_type_error() {
    // A type error resulting from a concrete type (string) not matching the
    // concrete type of a function (exp) which expects `f64`. This doesn't use
    // the path of instantiating generics, so we test it separately.
    insta::assert_yaml_snapshot!(QueryFixture::new("{ m: exp(Strings.s) }").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:6"
          - "  |"
          - "1 | { m: exp(Strings.s) }"
          - "  |      ^^^ --------- Actual type: string"
          - "  |      |    "
          - "  |      Invalid types for parameter 'power' in call to 'exp'"
          - "  |"
          - "  --> built-in signature 'exp(power: f64) -> f64':1:12"
          - "  |"
          - "1 | exp(power: f64) -> f64"
          - "  |            --- Expected type: f64"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_incompatible_actual_types_error() {
    // Test what happens when the actual types for a generic aren't compatible.
    // (No least-upper-bound).
    insta::assert_yaml_snapshot!(QueryFixture::new("{ m: Strings.s + Strings.n }").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:16"
          - "  |"
          - "1 | { m: Strings.s + Strings.n }"
          - "  |      --------- ^ --------- Type: i64"
          - "  |      |         |  "
          - "  |      |         Invalid types for call to 'add'"
          - "  |      Type: string"
          - "  |"
          - "  = Expected 'number'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_incompatible_lub_error() {
    // Test what happens when the solved type for a generic isn't compatible
    // with the constraint.
    insta::assert_yaml_snapshot!(QueryFixture::new("{ s2: Strings.s + Strings.s }").run_to_csv(&strings_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:17"
          - "  |"
          - "1 | { s2: Strings.s + Strings.s }"
          - "  |       --------- ^ Invalid types for call to 'add'"
          - "  |       |          "
          - "  |       Type: string"
          - "  |"
          - "  = Expected 'number'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_parse_error() {
    insta::assert_yaml_snapshot!(QueryFixture::new("let { n: Numbers.n}").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 2 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0011
        message: Invalid syntax
        formatted:
          - "error[E0011]: Invalid syntax"
          - "  --> Query:1:5"
          - "  |"
          - "1 | let { n: Numbers.n}"
          - "  |     ^ Invalid token '{'"
          - "  |"
          - "  = Expected ident"
          - ""
          - ""
      - severity: error
        code: E0011
        message: Invalid syntax
        formatted:
          - "error[E0011]: Invalid syntax"
          - "  --> Query:1:19"
          - "  |"
          - "1 | let { n: Numbers.n}"
          - "  |                   ^ Invalid token '}'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_parse_error_missing_parentheses() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ n: Numbers.n").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0011
        message: Invalid syntax
        formatted:
          - "error[E0011]: Invalid syntax"
          - "  --> Query:1:15"
          - "  |"
          - "1 | { n: Numbers.n"
          - "  |               ^ Unexpected EOF"
          - "  |"
          - "  = Expected \")\",\",\",\"]\",\"in\",\"let\",\"}\""
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_parse_error_unrecognized() {
    insta::assert_yaml_snapshot!(QueryFixture::new("limit x = 5 in { n: Numbers.n}").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 2 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0011
        message: Invalid syntax
        formatted:
          - "error[E0011]: Invalid syntax"
          - "  --> Query:1:7"
          - "  |"
          - "1 | limit x = 5 in { n: Numbers.n}"
          - "  |       ^ Invalid token 'x'"
          - "  |"
          - "  = Expected \"!=\", \"(\", \")\", \"*\", \"+\", \",\", \"-\", \".\", \"/\", \":\", \"<\", \"<=\", \"<>\", \"=\", \"=\", \"==\", \">\", \">=\", \"[\", \"]\", \"and\", \"as\", \"in\", \"let\", \"or\", \"|\", \"}\""
          - ""
          - ""
      - severity: error
        code: E0011
        message: Invalid syntax
        formatted:
          - "error[E0011]: Invalid syntax"
          - "  --> Query:1:30"
          - "  |"
          - "1 | limit x = 5 in { n: Numbers.n}"
          - "  |                              ^ Invalid token '}'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_invalid_let_binding_unrecognized() {
    insta::assert_yaml_snapshot!(QueryFixture::new("let $ = 5 in { n: Numbers.n + 1 } ").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0011
        message: Invalid syntax
        formatted:
          - "error[E0011]: Invalid syntax"
          - "  --> Query:1:5"
          - "  |"
          - "1 | let $ = 5 in { n: Numbers.n + 1 } "
          - "  |     ^ Invalid token '$'"
          - "  |"
          - "  = Expected ident"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_invalid_let_binding_not_ident() {
    insta::assert_yaml_snapshot!(QueryFixture::new("let + = 5 in { n: Numbers.n + 1 } ").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 3 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0011
        message: Invalid syntax
        formatted:
          - "error[E0011]: Invalid syntax"
          - "  --> Query:1:5"
          - "  |"
          - "1 | let + = 5 in { n: Numbers.n + 1 } "
          - "  |     ^ Invalid token '+'"
          - "  |"
          - "  = Expected ident"
          - ""
          - ""
      - severity: error
        code: E0011
        message: Invalid syntax
        formatted:
          - "error[E0011]: Invalid syntax"
          - "  --> Query:1:7"
          - "  |"
          - "1 | let + = 5 in { n: Numbers.n + 1 } "
          - "  |       ^ Invalid token '='"
          - "  |"
          - "  = Expected \"!\", \"$input\", \"(\", \"-\", \"{\", ident, literal"
          - ""
          - ""
      - severity: error
        code: E0011
        message: Invalid syntax
        formatted:
          - "error[E0011]: Invalid syntax"
          - "  --> Query:1:33"
          - "  |"
          - "1 | let + = 5 in { n: Numbers.n + 1 } "
          - "  |                                 ^ Invalid token '}'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_invalid_expr() {
    insta::assert_yaml_snapshot!(QueryFixture::new("let x = 5 in { n: Numbers.n + $ } ").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0011
        message: Invalid syntax
        formatted:
          - "error[E0011]: Invalid syntax"
          - "  --> Query:1:31"
          - "  |"
          - "1 | let x = 5 in { n: Numbers.n + $ } "
          - "  |                               ^ Invalid token '$'"
          - "  |"
          - "  = Expected \"!\", \"$input\", \"(\", \"-\", \"{\", ident, literal"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_invalid_type_ident() {
    // This test what happens when the parser action fails.
    // In this case, `as ben` attempts to parse `ben` as a FenlType, which it isn't.
    insta::assert_yaml_snapshot!(QueryFixture::new("{ n: Numbers.n as ben } ").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0002
        message: Illegal cast
        formatted:
          - "error[E0002]: Illegal cast"
          - "  --> Query:1:19"
          - "  |"
          - "1 | { n: Numbers.n as ben } "
          - "  |                   ^^^ Unable to cast to type 'ben'"
          - "  |"
          - "  = From type i64"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_invalid_type() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ n: Numbers.n as + } ").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 2 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0011
        message: Invalid syntax
        formatted:
          - "error[E0011]: Invalid syntax"
          - "  --> Query:1:19"
          - "  |"
          - "1 | { n: Numbers.n as + } "
          - "  |                   ^ Invalid token '+'"
          - "  |"
          - "  = Expected ident"
          - ""
          - ""
      - severity: error
        code: E0011
        message: Invalid syntax
        formatted:
          - "error[E0011]: Invalid syntax"
          - "  --> Query:1:21"
          - "  |"
          - "1 | { n: Numbers.n as + } "
          - "  |                     ^ Invalid token '}'"
          - "  |"
          - "  = Expected \"!\", \"$input\", \"(\", \"-\", \"{\", ident, literal"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_invalid_arguments_missing_named_value() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ n: ceil(x:) } ").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0011
        message: Invalid syntax
        formatted:
          - "error[E0011]: Invalid syntax"
          - "  --> Query:1:12"
          - "  |"
          - "1 | { n: ceil(x:) } "
          - "  |            ^ Invalid token ':'"
          - "  |"
          - "  = Expected \"!=\", \"(\", \")\", \"*\", \"+\", \",\", \"-\", \".\", \"/\", \"<\", \"<=\", \"<>\", \"=\", \"=\", \"==\", \">\", \">=\", \"[\", \"and\", \"as\", \"or\", \"|\""
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_invalid_arguments_unexpected_operator() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ n: ceil(+ Numbers.n) } ").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0011
        message: Invalid syntax
        formatted:
          - "error[E0011]: Invalid syntax"
          - "  --> Query:1:11"
          - "  |"
          - "1 | { n: ceil(+ Numbers.n) } "
          - "  |           ^ Invalid token '+'"
          - "  |"
          - "  = Expected \"!\", \"$input\", \"(\", \")\", \",\", \"-\", \"let\", \"{\", ident, literal"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_invalid_named_arguments_duplicates() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ n: ceil(x = Numbers.n, x = 5) } ").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0008
        message: Invalid arguments
        formatted:
          - "error[E0008]: Invalid arguments"
          - "  --> Query:1:6"
          - "  |"
          - "1 | { n: ceil(x = Numbers.n, x = 5) } "
          - "  |      ^^^^"
          - "  |"
          - "  = Nearest matches: n"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_invalid_named_before_positional() {
    // 1. This is only *currently* an error. We may consider requiring *all* named
    // come first, etc.
    //
    // 2. This is currently (awkwardly) produced by the parser,
    // since it is part of creating the Arguments.
    //
    // Ideally, the parser could produce an Arguments that got validated later.
    insta::assert_yaml_snapshot!(QueryFixture::new("{ n: ceil(x = Numbers.n, 5) } ").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0008
        message: Invalid arguments
        formatted:
          - "error[E0008]: Invalid arguments"
          - "  --> Query:1:26"
          - "  |"
          - "1 | { n: ceil(x = Numbers.n, 5) } "
          - "  |           -              ^ positional appears after keyword"
          - "  |           |               "
          - "  |           first keyword was here"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_invalid_output_type() {
    insta::assert_yaml_snapshot!(QueryFixture::new("Numbers.time").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0013
        message: Invalid output type
        formatted:
          - "error[E0013]: Invalid output type"
          - " = Output type must be a record, but was timestamp_s"
          - ""
          - ""
    "###);
}

#[tokio::test]
#[ignore = "Aggregating a constant panics"]
async fn test_sum_of_constant() {
    insta::assert_yaml_snapshot!(QueryFixture::new("sum(0)").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    "###);
}

#[tokio::test]
async fn test_window_as_query() {
    insta::assert_yaml_snapshot!(QueryFixture::new("since(Numbers.m > 10)").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0013
        message: Invalid output type
        formatted:
          - "error[E0013]: Invalid output type"
          - " = Output type must be a record, but was window"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_windows_as_field() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ x: since(Numbers.m > 10) }").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:6"
          - "  |"
          - "1 | { x: since(Numbers.m > 10) }"
          - "  |      ^^^^^^^^^^^^^^^^^^^^^ Field 'x' has invalid type window"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_non_const_lag() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ invalid_lag: lag(Numbers.n, Numbers.n) }").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0014
        message: Invalid non-constant argument
        formatted:
          - "error[E0014]: Invalid non-constant argument"
          - "  --> Query:1:20"
          - "  |"
          - "1 | { invalid_lag: lag(Numbers.n, Numbers.n) }"
          - "  |                    ^^^^^^^^^ Argument 'n' to 'lag' must be constant, but was not"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_unsupported_output_type_duration_basic() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ duration: seconds_between(Numbers.time, Numbers.time), other: Numbers.n }").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0013
        message: Invalid output type
        formatted:
          - "error[E0013]: Invalid output type"
          - "  --> Query:1:13"
          - "  |"
          - "1 | { duration: seconds_between(Numbers.time, Numbers.time), other: Numbers.n }"
          - "  |             ^^^^^^^^^^^^^^^ Output field 'duration' has unsupported output type 'Duration(Second)'. Try adding 'as i64'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_unsupported_output_type_interval_basic() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ interval: months_between(Numbers.time, Numbers.time), other: Numbers.time }").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0013
        message: Invalid output type
        formatted:
          - "error[E0013]: Invalid output type"
          - "  --> Query:1:13"
          - "  |"
          - "1 | { interval: months_between(Numbers.time, Numbers.time), other: Numbers.time }"
          - "  |             ^^^^^^^^^^^^^^ Output field 'interval' has unsupported output type 'Interval(YearMonth)'. Try adding 'as i64'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_unsupported_output_type_with_last() {
    // TODO: Add "Type Explanation" through nodes.
    //
    // Current: No range of bytes
    // Expected: Underline offending expr that produced invalid type.
    insta::assert_yaml_snapshot!(QueryFixture::new("{ interval: months_between(Numbers.time, Numbers.time), other: Numbers.time } | last()").run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0013
        message: Invalid output type
        formatted:
          - "error[E0013]: Invalid output type"
          - " = Output field 'interval' has unsupported output type 'Interval(YearMonth)'. Try adding 'as i64'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_unsupported_output_type_with_formula() {
    // TODO: Add "Type Explanation" through nodes.
    //
    // Current: Underlines the expr that produced the invalid type (this is good).
    // Expected: Possibly also underlines where the type is being used in the output
    // struct.
    insta::assert_yaml_snapshot!(QueryFixture::new("{ interval: mon_bet, other: Numbers.time }")
        .with_formula("mon_bet", "months_between(Numbers.time, Numbers.time)")
        .run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0013
        message: Invalid output type
        formatted:
          - "error[E0013]: Invalid output type"
          - "  --> 'test case':1:1"
          - "  |"
          - "1 | months_between(Numbers.time, Numbers.time)"
          - "  | ^^^^^^^^^^^^^^ Output field 'interval' has unsupported output type 'Interval(YearMonth)'. Try adding 'as i64'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_unsupported_output_type_with_nested_struct() {
    // TODO: Add "Type Explanation" through nodes.
    //
    // Current: Underlines entire struct
    // Expected: Underline just `months_between(...)`, since that's the expr that
    // produced the type.
    insta::assert_yaml_snapshot!(QueryFixture::new("{ interval: mon_bet, other: Numbers.time }")
        .with_formula("mon_bet", "{ interval_nested: months_between(Numbers.time, Numbers.time) }.interval_nested")
        .run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0013
        message: Invalid output type
        formatted:
          - "error[E0013]: Invalid output type"
          - "  --> 'test case':1:1"
          - "  |"
          - "1 | { interval_nested: months_between(Numbers.time, Numbers.time) }.interval_nested"
          - "  | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Output field 'interval' has unsupported output type 'Interval(YearMonth)'. Try adding 'as i64'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_unsupported_output_type_with_formula_as_record_output() {
    insta::assert_yaml_snapshot!(QueryFixture::new("output")
        .with_formula("output", "{ interval: months_between(Numbers.time, Numbers.time) }")
        .run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0013
        message: Invalid output type
        formatted:
          - "error[E0013]: Invalid output type"
          - "  --> 'test case':1:13"
          - "  |"
          - "1 | { interval: months_between(Numbers.time, Numbers.time) }"
          - "  |             ^^^^^^^^^^^^^^ Output field 'interval' has unsupported output type 'Interval(YearMonth)'. Try adding 'as i64'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_unsupported_output_type_with_nested_formulas() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ output: a }")
        .with_formula("a", "seconds_between(b, b)")
        .with_formula("b", "time_of(c)")
        .with_formula("c", "sum(Numbers.n)")
        .run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0013
        message: Invalid output type
        formatted:
          - "error[E0013]: Invalid output type"
          - "  --> 'test case':1:1"
          - "  |"
          - "1 | seconds_between(b, b)"
          - "  | ^^^^^^^^^^^^^^^ Output field 'output' has unsupported output type 'Duration(Second)'. Try adding 'as i64'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_unsupported_output_type_with_multiple_fields() {
    // TODO: Add "Type Explanation" through nodes.
    insta::assert_yaml_snapshot!(QueryFixture::new("{ a, b }")
        .with_formula("a", "months_between(Numbers.time, Numbers.time)")
        .with_formula("b", "seconds_between(Numbers.time, Numbers.time)")
        .run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 2 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0013
        message: Invalid output type
        formatted:
          - "error[E0013]: Invalid output type"
          - "  --> 'test case':1:1"
          - "  |"
          - "1 | months_between(Numbers.time, Numbers.time)"
          - "  | ^^^^^^^^^^^^^^ Output field 'a' has unsupported output type 'Interval(YearMonth)'. Try adding 'as i64'"
          - ""
          - ""
      - severity: error
        code: E0013
        message: Invalid output type
        formatted:
          - "error[E0013]: Invalid output type"
          - "  --> 'test case':1:1"
          - "  |"
          - "1 | seconds_between(Numbers.time, Numbers.time)"
          - "  | ^^^^^^^^^^^^^^^ Output field 'b' has unsupported output type 'Duration(Second)'. Try adding 'as i64'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_unsupported_output_type_with_nested_formulas_2() {
    // TODO: Add "Type Explanation" through nodes.
    //
    // Current: Doesn't propagate the type information further than a single layer.
    // Expected: Underline `seconds_between(...)`
    insta::assert_yaml_snapshot!(QueryFixture::new("{ output: a }")
        .with_formula("a", "last(b)")
        .with_formula("b", "seconds_between(Numbers.time, Numbers.time)")
        .run_to_csv(&i64_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0013
        message: Invalid output type
        formatted:
          - "error[E0013]: Invalid output type"
          - "  --> 'test case':1:1"
          - "  |"
          - 1 | last(b)
          - "  | ^^^^ Output field 'output' has unsupported output type 'Duration(Second)'. Try adding 'as i64'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_swapped_args_for_get_map() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ f1: get(Input.e0, \"f1\") }")
        .run_to_csv(&collection_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:7"
          - "  |"
          - "1 | { f1: get(Input.e0, \"f1\") }"
          - "  |       ^^^           ---- Actual type: string"
          - "  |       |              "
          - "  |       Invalid types for parameter 'map' in call to 'get'"
          - "  |"
          - "  --> built-in signature 'get<K: key, V: any>(key: K, map: map<K, V>) -> V':1:34"
          - "  |"
          - "1 | get<K: key, V: any>(key: K, map: map<K, V>) -> V"
          - "  |                                  --------- Expected type: map<K, V>"
          - ""
          - ""
    "###);
}
