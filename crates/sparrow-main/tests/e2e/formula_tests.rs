use crate::fixtures::i64_data_fixture;
use crate::QueryFixture;

#[tokio::test]
async fn test_formulas_out_of_order() {
    insta::assert_snapshot!(QueryFixture::new("{ m: Numbers.m, n: Numbers.n, add: n_plus_m }")
        .with_formula("n_plus_m", "numbers_m + Numbers.n")
        .with_formula("numbers_m", "Numbers.m").run_to_csv(&i64_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,add
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,5,10,15
    1996-12-20T00:39:58.000000000,9223372036854775808,11753611437813598533,B,24,3,27
    1996-12-20T00:39:59.000000000,9223372036854775808,3650215962958587783,A,17,6,23
    1996-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,,9,
    1996-12-20T00:40:01.000000000,9223372036854775808,3650215962958587783,A,12,,
    1996-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,,,
    "###);
}

#[tokio::test]
async fn test_formulas_cyclic_dependency() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ sum: n + m }")
        .with_formula("n", "m + 1")
        .with_formula("m", "n + 1").run_to_csv(&i64_data_fixture()).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0012
        message: Circular dependency
        formatted:
          - "error[E0012]: Circular dependency"
          - "  --> 'test case':1:1"
          - "  |"
          - 1 | m + 1
          - "  | ^ Formula 'm' referenced here in 'n'"
          - "  |"
          - "  --> 'test case':1:1"
          - "  |"
          - 1 | n + 1
          - "  | ^ Formula 'n' referenced here in 'm'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_invalid_formula_not_rereported() {
    // This test verifies that if an formula's expression is invalid (`$$`)
    // it doesn't cause an "unbound reference" error elsewhere.
    insta::assert_yaml_snapshot!(QueryFixture::new("{ n: Invalid + 5 } ")
      .with_formula("Invalid", "Numbers.n + $$").run_to_csv(&i64_data_fixture()).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0011
        message: Invalid syntax
        formatted:
          - "error[E0011]: Invalid syntax"
          - "  --> 'test case':1:13"
          - "  |"
          - 1 | Numbers.n + $$
          - "  |             ^ Invalid token '$'"
          - "  |"
          - "  = Expected \"!\", \"$input\", \"(\", \"-\", \"{\", ident, literal"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_invalid_formula_still_reports_query_error() {
    // This verifies that if a formula is invalid, we still report
    // problems in the query.
    insta::assert_yaml_snapshot!(QueryFixture::new("{ n: Invalid + $$ } ")
      .with_formula("Invalid", "Numbers.n + $$").run_to_csv(&i64_data_fixture()).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0011
        message: Invalid syntax
        formatted:
          - "error[E0011]: Invalid syntax"
          - "  --> Query:1:16"
          - "  |"
          - "1 | { n: Invalid + $$ } "
          - "  |                ^ Invalid token '$'"
          - "  |"
          - "  = Expected \"!\", \"$input\", \"(\", \"-\", \"{\", ident, literal"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_unused_formula_does_not_report_query_error() {
    // This verifies that if an invalid formula is not used, we
    // still report problems in the query.
    insta::assert_snapshot!(QueryFixture::new("{ n: Numbers.n } ")
      .with_formula("Invalid", "Numbers.n + $$").run_to_csv(&i64_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,10
    1996-12-20T00:39:58.000000000,9223372036854775808,11753611437813598533,B,3
    1996-12-20T00:39:59.000000000,9223372036854775808,3650215962958587783,A,6
    1996-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,9
    1996-12-20T00:40:01.000000000,9223372036854775808,3650215962958587783,A,
    1996-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,
    "###);
}
