//! Basic e2e tests for lookups.

use indoc::indoc;
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::{DataFixture, QueryFixture};

/// Fixture for testing lookups on the account entity.
///
/// Includes two tables `Sent` and `Received` both grouped by account ID.
/// Also includes a `CodeName` table used for lookups on a different type.
async fn lookup_account_data_fixture() -> DataFixture {
    let transactions = indoc! {"
        from,to,time,subsort,amount,description,order_time,code
        0,2,1996-12-19T16:39:57-08:00,0,50,food,2005-12-19T16:39:57-08:00,5
        0,0,1997-12-19T16:39:57-08:00,1,11,gas,2001-12-19T16:39:57-08:00,6
        2,0,1997-12-19T16:39:58-08:00,2,25,food,2001-12-19T16:39:57-08:00,5
        0,1,1998-12-19T16:39:57-08:00,3,25,gas,2003-12-19T16:39:57-08:00,6
        0,1,1999-12-19T16:39:58-08:00,4,12,MOVIe,2004-12-1,7
        0,1,1999-12-19T16:39:58-08:00,5,,null_amount,2005-12-1,
    "};

    let code_names = indoc! {"
        code,time,subsort,name
        5,1996-12-18T16:39:57-08:00,0,FiveA
        6,1997-12-18T16:39:57-08:00,0,Six
        5,1997-12-19T16:39:58-08:00,0,FiveB
        7,2000-12-19T16:39:57-08:00,0,Seven
    "};

    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new_with_table_source(
                "Sent",
                &Uuid::new_v4(),
                "time",
                Some("subsort"),
                "from",
                "account",
            ),
            transactions,
        )
        .await
        .unwrap()
        .with_table_from_csv(
            TableConfig::new_with_table_source(
                "Received",
                &Uuid::new_v4(),
                "time",
                Some("subsort"),
                "to",
                "account",
            ),
            transactions,
        )
        .await
        .unwrap()
        .with_table_from_csv(
            TableConfig::new_with_table_source(
                "CodeName",
                &Uuid::new_v4(),
                "time",
                Some("subsort"),
                "code",
                "code",
            ),
            code_names,
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_lookup_self_i64() {
    insta::assert_snapshot!(QueryFixture::new("let sum_sent = sum(Sent.amount)
                let last_sender = last(Received.from)
                let last_sender_sum_sent = lookup(last(Received.from), sum_sent)
                in { last_sender, last_sender_sum_sent }").run_to_csv(&lookup_account_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,last_sender,last_sender_sum_sent
    1996-12-20T00:39:57.000000000,9223372036854775808,2694864431690786590,2,0,50
    1997-12-20T00:39:57.000000000,9223372036854775809,11832085162654999889,0,0,61
    1997-12-20T00:39:58.000000000,9223372036854775810,11832085162654999889,0,2,25
    1998-12-20T00:39:57.000000000,9223372036854775811,18433805721903975440,1,0,86
    1999-12-20T00:39:58.000000000,9223372036854775812,18433805721903975440,1,0,98
    1999-12-20T00:39:58.000000000,9223372036854775813,18433805721903975440,1,0,98
    "###);
}

#[tokio::test]
async fn test_lookup_self_i64_with_merge_interpolation() {
    insta::assert_snapshot!(QueryFixture::new("let sum_sent = sum(Sent.amount)
                let last_sender = last(Received.from)
                let last_sender_sum_sent = lookup(last(Received.from), sum_sent)
                in { sum_sent, last_sender, last_sender_sum_sent }").run_to_csv(&lookup_account_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,sum_sent,last_sender,last_sender_sum_sent
    1996-12-20T00:39:57.000000000,9223372036854775808,2694864431690786590,2,,0,50
    1996-12-20T00:39:57.000000000,9223372036854775808,11832085162654999889,0,50,,
    1997-12-20T00:39:57.000000000,9223372036854775809,11832085162654999889,0,61,0,61
    1997-12-20T00:39:58.000000000,9223372036854775810,2694864431690786590,2,25,0,50
    1997-12-20T00:39:58.000000000,9223372036854775810,11832085162654999889,0,61,2,25
    1998-12-20T00:39:57.000000000,9223372036854775811,11832085162654999889,0,86,2,25
    1998-12-20T00:39:57.000000000,9223372036854775811,18433805721903975440,1,,0,86
    1999-12-20T00:39:58.000000000,9223372036854775812,11832085162654999889,0,98,2,25
    1999-12-20T00:39:58.000000000,9223372036854775812,18433805721903975440,1,,0,98
    1999-12-20T00:39:58.000000000,9223372036854775813,11832085162654999889,0,98,2,25
    1999-12-20T00:39:58.000000000,9223372036854775813,18433805721903975440,1,,0,98
    "###);
}

#[tokio::test]
async fn test_lookup_self_string() {
    insta::assert_snapshot!(QueryFixture::new("let last_sender = last(Received.from)
                let last_sender_description = lookup(last_sender, last(Sent.description))
                in { description: Received.description, last_sender, last_sender_description }").run_to_csv(&lookup_account_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,description,last_sender,last_sender_description
    1996-12-20T00:39:57.000000000,9223372036854775808,2694864431690786590,2,food,0,food
    1997-12-20T00:39:57.000000000,9223372036854775809,11832085162654999889,0,gas,0,gas
    1997-12-20T00:39:58.000000000,9223372036854775810,11832085162654999889,0,food,2,food
    1998-12-20T00:39:57.000000000,9223372036854775811,18433805721903975440,1,gas,0,gas
    1999-12-20T00:39:58.000000000,9223372036854775812,18433805721903975440,1,MOVIe,0,MOVIe
    1999-12-20T00:39:58.000000000,9223372036854775813,18433805721903975440,1,null_amount,0,null_amount
    "###);
}

#[tokio::test]
async fn test_lookup_self_record() {
    insta::assert_snapshot!(QueryFixture::new("let last_sender = last(Received.from)
                let last_sender_sent = lookup(last(Received.to), Sent.description)
                in Sent | extend({ received_description: Received.description, last_sender, last_sender_sent })").run_to_csv(&lookup_account_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,received_description,last_sender,last_sender_sent,from,to,time,subsort,amount,description,order_time,code
    1996-12-20T00:39:57.000000000,9223372036854775808,2694864431690786590,2,food,0,,,,,,,,,
    1996-12-20T00:39:57.000000000,9223372036854775808,11832085162654999889,0,,,,0,2,1996-12-20T00:39:57.000000000,0,50,food,2005-12-19T16:39:57-08:00,5
    1997-12-20T00:39:57.000000000,9223372036854775809,11832085162654999889,0,gas,0,gas,0,0,1997-12-20T00:39:57.000000000,1,11,gas,2001-12-19T16:39:57-08:00,6
    1997-12-20T00:39:58.000000000,9223372036854775810,2694864431690786590,2,,0,,2,0,1997-12-20T00:39:58.000000000,2,25,food,2001-12-19T16:39:57-08:00,5
    1997-12-20T00:39:58.000000000,9223372036854775810,11832085162654999889,0,food,2,,,,,,,,,
    1998-12-20T00:39:57.000000000,9223372036854775811,11832085162654999889,0,,2,,0,1,1998-12-20T00:39:57.000000000,3,25,gas,2003-12-19T16:39:57-08:00,6
    1998-12-20T00:39:57.000000000,9223372036854775811,18433805721903975440,1,gas,0,,,,,,,,,
    1999-12-20T00:39:58.000000000,9223372036854775812,11832085162654999889,0,,2,,0,1,1999-12-20T00:39:58.000000000,4,12,MOVIe,2004-12-1,7
    1999-12-20T00:39:58.000000000,9223372036854775812,18433805721903975440,1,MOVIe,0,,,,,,,,,
    1999-12-20T00:39:58.000000000,9223372036854775813,11832085162654999889,0,,2,,0,1,1999-12-20T00:39:58.000000000,5,,null_amount,2005-12-1,
    1999-12-20T00:39:58.000000000,9223372036854775813,18433805721903975440,1,null_amount,0,,,,,,,,,
    "###);
}

#[tokio::test]
async fn test_lookup_code_name() {
    insta::assert_snapshot!(QueryFixture::new("{ code: Sent.code, code_name: lookup(Sent.code, CodeName.name | last()) }").run_to_csv(&lookup_account_data_fixture().await).await
        .unwrap(), @r###"
    _time,_subsort,_key_hash,_key,code,code_name
    1996-12-20T00:39:57.000000000,9223372036854775808,11832085162654999889,0,5,FiveA
    1997-12-20T00:39:57.000000000,9223372036854775809,11832085162654999889,0,6,Six
    1997-12-20T00:39:58.000000000,9223372036854775810,2694864431690786590,2,5,FiveB
    1998-12-20T00:39:57.000000000,9223372036854775811,11832085162654999889,0,6,Six
    1999-12-20T00:39:58.000000000,9223372036854775812,11832085162654999889,0,7,
    1999-12-20T00:39:58.000000000,9223372036854775813,11832085162654999889,0,,
    "###);
}

#[tokio::test]
async fn test_lookup_code_name_wacky_unused() {
    // Ensures simplification does not associate distinct expressions together.
    insta::assert_snapshot!(QueryFixture::new("let foo = Sent.code | if(false) in
                    { code: Sent.code, code_name: lookup(Sent.code, CodeName.name | last()) }").run_to_csv(&lookup_account_data_fixture().await).await
        .unwrap(), @r###"
    _time,_subsort,_key_hash,_key,code,code_name
    1996-12-20T00:39:57.000000000,9223372036854775808,11832085162654999889,0,5,FiveA
    1997-12-20T00:39:57.000000000,9223372036854775809,11832085162654999889,0,6,Six
    1997-12-20T00:39:58.000000000,9223372036854775810,2694864431690786590,2,5,FiveB
    1998-12-20T00:39:57.000000000,9223372036854775811,11832085162654999889,0,6,Six
    1999-12-20T00:39:58.000000000,9223372036854775812,11832085162654999889,0,7,
    1999-12-20T00:39:58.000000000,9223372036854775813,11832085162654999889,0,,
    "###);
}

#[tokio::test]
async fn test_lookup_invalid_key_type() {
    insta::assert_yaml_snapshot!(QueryFixture::new("let foo = Sent.code | if(false) in
                    { code: Sent.code, code_name: lookup(Sent.description, CodeName.name | last()) }").run_to_csv(&lookup_account_data_fixture().await).await
        .unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: warning
        code: W2001
        message: Unused binding
        formatted:
          - "warning[W2001]: Unused binding"
          - "  --> Query:1:5"
          - "  |"
          - 1 | let foo = Sent.code | if(false) in
          - "  |     ^^^ Unused binding 'foo'"
          - ""
          - ""
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:2:58"
          - "  |"
          - "2 |                     { code: Sent.code, code_name: lookup(Sent.description, CodeName.name | last()) }"
          - "  |                                                          ^^^^^^^^^^^^^^^^  ---------------------- Grouping 'code' expects key type i64"
          - "  |                                                          |                  "
          - "  |                                                          Actual key type string"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_lookup_invalid_key_expression() {
    insta::assert_yaml_snapshot!(QueryFixture::new("let foo = Sent.code | if(false) in
                    { code: Sent.code, code_name: lookup(Sent.desciption, CodeName.name | last()) }").run_to_csv(&lookup_account_data_fixture().await).await
        .unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: warning
        code: W2001
        message: Unused binding
        formatted:
          - "warning[W2001]: Unused binding"
          - "  --> Query:1:5"
          - "  |"
          - 1 | let foo = Sent.code | if(false) in
          - "  |     ^^^ Unused binding 'foo'"
          - ""
          - ""
      - severity: error
        code: E0001
        message: Illegal field reference
        formatted:
          - "error[E0001]: Illegal field reference"
          - "  --> Query:2:63"
          - "  |"
          - "2 |                     { code: Sent.code, code_name: lookup(Sent.desciption, CodeName.name | last()) }"
          - "  |                                                               ^^^^^^^^^^ No field named 'desciption'"
          - "  |"
          - "  = Nearest fields: 'description', 'order_time', 'time', 'to', 'amount'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_lookup_invalid_key_expression_window() {
    insta::assert_yaml_snapshot!(QueryFixture::new("let foo = Sent.code | if(false) in
                    { code: Sent.code, code_name: lookup(since(is_valid(Sent.description)), CodeName.name | last()) }").run_to_csv(&lookup_account_data_fixture().await).await
        .unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: warning
        code: W2001
        message: Unused binding
        formatted:
          - "warning[W2001]: Unused binding"
          - "  --> Query:1:5"
          - "  |"
          - 1 | let foo = Sent.code | if(false) in
          - "  |     ^^^ Unused binding 'foo'"
          - ""
          - ""
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:2:51"
          - "  |"
          - "2 |                     { code: Sent.code, code_name: lookup(since(is_valid(Sent.description)), CodeName.name | last()) }"
          - "  |                                                   ^^^^^^ --------------------------------- Type: window"
          - "  |                                                   |       "
          - "  |                                                   Invalid types for call to 'lookup'"
          - "  |"
          - "  = Expected 'key'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_lookup_invalid_value_expression() {
    insta::assert_yaml_snapshot!(QueryFixture::new("let foo = Sent.code | if(false) in
                    { code: Sent.code, code_name: lookup(Sent.description, CodeNme.name | last()) }").run_to_csv(&lookup_account_data_fixture().await).await
        .unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: warning
        code: W2001
        message: Unused binding
        formatted:
          - "warning[W2001]: Unused binding"
          - "  --> Query:1:5"
          - "  |"
          - 1 | let foo = Sent.code | if(false) in
          - "  |     ^^^ Unused binding 'foo'"
          - ""
          - ""
      - severity: error
        code: E0006
        message: Unbound reference
        formatted:
          - "error[E0006]: Unbound reference"
          - "  --> Query:2:76"
          - "  |"
          - "2 |                     { code: Sent.code, code_name: lookup(Sent.description, CodeNme.name | last()) }"
          - "  |                                                                            ^^^^^^^ No reference named 'CodeNme'"
          - "  |"
          - "  = Nearest matches: 'CodeName', 'Received', 'Sent', 'foo'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_lookup_with_key() {
    insta::assert_snapshot!(QueryFixture::new("{ code_name: lookup(lookup_key, lookup_value) }")
    .with_formula("lookup_key", "Sent.code | last()")
    .with_formula("lookup_value", "Sent | with_key(Sent.code, grouping=\"Code\") | when($input.description == \"food\") | count(window=since(daily()))")
    .run_to_csv(&lookup_account_data_fixture().await).await
        .unwrap(), @r###"
    _time,_subsort,_key_hash,_key,code_name
    1996-12-20T00:39:57.000000000,9223372036854775808,11832085162654999889,0,1
    1997-12-20T00:39:57.000000000,9223372036854775809,11832085162654999889,0,
    1997-12-20T00:39:58.000000000,9223372036854775810,2694864431690786590,2,1
    1998-12-20T00:39:57.000000000,9223372036854775811,11832085162654999889,0,
    1999-12-20T00:39:58.000000000,9223372036854775812,11832085162654999889,0,
    1999-12-20T00:39:58.000000000,9223372036854775813,11832085162654999889,0,
    "###);
}

#[tokio::test]
async fn test_lookup_invalid_value_grouping_post_recovery() {
    // Verifies that an unrecognized name does not produce an internal error.
    insta::assert_yaml_snapshot!(QueryFixture::new("let foo = Sent.code | if(false) in
                    { code: Sent.code, code_name: lookup(Sent.description, CodeNme.name | count()) }").run_to_csv(&lookup_account_data_fixture().await).await
        .unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: warning
        code: W2001
        message: Unused binding
        formatted:
          - "warning[W2001]: Unused binding"
          - "  --> Query:1:5"
          - "  |"
          - 1 | let foo = Sent.code | if(false) in
          - "  |     ^^^ Unused binding 'foo'"
          - ""
          - ""
      - severity: error
        code: E0006
        message: Unbound reference
        formatted:
          - "error[E0006]: Unbound reference"
          - "  --> Query:2:76"
          - "  |"
          - "2 |                     { code: Sent.code, code_name: lookup(Sent.description, CodeNme.name | count()) }"
          - "  |                                                                            ^^^^^^^ No reference named 'CodeNme'"
          - "  |"
          - "  = Nearest matches: 'CodeName', 'Received', 'Sent', 'foo'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_lookup_invalid_constant_value() {
    insta::assert_yaml_snapshot!(QueryFixture::new("let foo = Sent.code | if(false) in
                    { code: Sent.code, code_name: lookup(Sent.description, 50) }").run_to_csv(&lookup_account_data_fixture().await).await
        .unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: warning
        code: W2001
        message: Unused binding
        formatted:
          - "warning[W2001]: Unused binding"
          - "  --> Query:1:5"
          - "  |"
          - 1 | let foo = Sent.code | if(false) in
          - "  |     ^^^ Unused binding 'foo'"
          - ""
          - ""
      - severity: error
        code: E0008
        message: Invalid arguments
        formatted:
          - "error[E0008]: Invalid arguments"
          - "  --> Query:2:76"
          - "  |"
          - "2 |                     { code: Sent.code, code_name: lookup(Sent.description, 50) }"
          - "  |                                                                            ^^ Invalid un-grouped foreign value for lookup."
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_lookup_invalid_constant_key() {
    insta::assert_yaml_snapshot!(QueryFixture::new("let foo = Sent.code | if(false) in
                    { code: Sent.code, code_name: lookup(50, CodeName.name | last()) }").run_to_csv(&lookup_account_data_fixture().await).await
        .unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: warning
        code: W2001
        message: Unused binding
        formatted:
          - "warning[W2001]: Unused binding"
          - "  --> Query:1:5"
          - "  |"
          - 1 | let foo = Sent.code | if(false) in
          - "  |     ^^^ Unused binding 'foo'"
          - ""
          - ""
      - severity: error
        code: E0008
        message: Invalid arguments
        formatted:
          - "error[E0008]: Invalid arguments"
          - "  --> Query:2:58"
          - "  |"
          - "2 |                     { code: Sent.code, code_name: lookup(50, CodeName.name | last()) }"
          - "  |                                                          ^^ Invalid un-grouped foreign key for lookup."
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_lookup_only_includes_primary_entites() {
    // Verifies that self-lookups do not incorrectly expand the entity set.
    // Specifically, the query should only include entities (and times)
    // that appear in `Received`, since `Sent` is only used within the value.
    // 0, 1, 2 receive transfers, but only 0 and 2 send transfers. Thus
    // there should only be 2 entities in the rows.
    insta::assert_snapshot!(QueryFixture::new("{ description: lookup(last(Sent.to), Received.description) }").run_to_csv(&lookup_account_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,description
    1996-12-20T00:39:57.000000000,9223372036854775808,11832085162654999889,0,food
    1997-12-20T00:39:57.000000000,9223372036854775809,11832085162654999889,0,gas
    1997-12-20T00:39:58.000000000,9223372036854775810,2694864431690786590,2,food
    1998-12-20T00:39:57.000000000,9223372036854775811,11832085162654999889,0,gas
    1999-12-20T00:39:58.000000000,9223372036854775812,11832085162654999889,0,MOVIe
    1999-12-20T00:39:58.000000000,9223372036854775813,11832085162654999889,0,null_amount
    "###);
}

#[tokio::test]
async fn test_lookup_only_includes_primary_entites_final_results() {
    insta::assert_snapshot!(QueryFixture::new("{ description: lookup(last(Sent.to), Received.description) }").with_final_results().run_to_csv(&lookup_account_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,description
    1999-12-20T00:39:58.000000001,18446744073709551615,2694864431690786590,2,food
    1999-12-20T00:39:58.000000001,18446744073709551615,11832085162654999889,0,null_amount
    "###);
}
