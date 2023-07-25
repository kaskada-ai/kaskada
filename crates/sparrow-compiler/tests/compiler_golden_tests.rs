#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

use arrow::datatypes::DataType;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::compile_request::ExpressionKind;
use sparrow_api::kaskada::v1alpha::schema::Field;
use sparrow_api::kaskada::v1alpha::Schema;
use sparrow_api::kaskada::v1alpha::{slice_request, SliceRequest};
use sparrow_api::kaskada::v1alpha::{
    CompileRequest, CompileResponse, ComputePlan, ComputeTable, FeatureSet, Formula,
    PerEntityBehavior, TableConfig, TableMetadata,
};
use sparrow_compiler::InternalCompileOptions;
use uuid::Uuid;

#[derive(Debug)]
struct TestScript {
    tables: Vec<TestTable>,
    feature_set: FeatureSet,
}

impl TestScript {
    async fn compile(
        self,
        slice_request: Option<SliceRequest>,
    ) -> error_stack::Result<CompileResponse, sparrow_compiler::Error> {
        let tables = self
            .tables
            .into_iter()
            .map(|table| ComputeTable {
                config: Some(TableConfig::new_with_table_source(
                    &table.name,
                    &table.id,
                    "",
                    Some(""),
                    &table.group_key,
                    &table.grouping,
                )),
                metadata: Some(TableMetadata {
                    schema: Some(table.proto_schema()),
                    file_count: 13,
                }),
                file_sets: vec![],
            })
            .collect();

        sparrow_compiler::compile_proto(
            CompileRequest {
                tables,
                feature_set: Some(self.feature_set),
                slice_request,
                expression_kind: ExpressionKind::Complete as i32,
                experimental: false,
                per_entity_behavior: PerEntityBehavior::All as i32,
            },
            InternalCompileOptions::default(),
        )
        .await
    }
}

#[derive(Debug)]
struct TestTable {
    name: String,
    id: Uuid,
    group_key: String,
    grouping: String,
    fields: Vec<TestField>,
}

impl TestTable {
    fn proto_schema(&self) -> Schema {
        let fields = self
            .fields
            .iter()
            .map(|field| Field {
                name: field.name.clone(),
                data_type: Some((&field.data_type).try_into().unwrap()),
                nullable: false,
            })
            .collect();
        Schema { fields }
    }
}

#[derive(Debug)]
struct TestField {
    name: String,
    data_type: DataType,
}

impl TestField {
    fn new(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.to_owned(),
            data_type,
        }
    }
}

async fn compile(test_script: TestScript, slice_request: Option<SliceRequest>) -> ComputePlan {
    let result = test_script.compile(slice_request).await.unwrap();
    if let Some(plan) = result.plan {
        plan
    } else {
        panic!(
            "Compilation failed with diagnostics:\n{}",
            result
                .fenl_diagnostics
                .unwrap_or_default()
                .fenl_diagnostics
                .into_iter()
                .map(|elt| elt.formatted)
                .format("\n")
        )
    }
}

async fn compile_err(test_script: TestScript) -> String {
    let result = test_script.compile(None).await.unwrap();
    if let Some(plan) = result.plan {
        panic!("Expected no plan with compile errors, but was {plan:?}")
    } else {
        result.fenl_diagnostics.unwrap_or_default().to_string()
    }
}

/// Compile and return the warnings, if any.
async fn compile_warnings(test_script: TestScript) -> String {
    let result = test_script.compile(None).await.unwrap();
    let diagnostics = result.fenl_diagnostics.unwrap_or_default().to_string();

    assert!(
        result.plan.is_some(),
        "Expected plan with only compile warnings:\n\n{diagnostics}",
    );

    diagnostics
}

fn formula(name: &str, formula: &str) -> Formula {
    Formula {
        name: name.to_owned(),
        formula: formula.to_owned(),
        source_location: name.to_owned(),
    }
}

// We can't create a constant for this since it creates `String`
// which isn't (yet) possible.
fn account_sent_table() -> TestTable {
    TestTable {
        name: "Sent".to_owned(),
        id: Uuid::parse_str("111DA01F9ABD4d9d80C702AF85C822A8").unwrap(),
        group_key: "sender".to_owned(),
        grouping: "account".to_owned(),
        fields: vec![
            TestField::new("sender", DataType::UInt64),
            TestField::new("amount", DataType::Float64),
            TestField::new("receiver", DataType::UInt64),
            TestField::new("store", DataType::UInt64),
        ],
    }
}

fn primitive_table() -> TestTable {
    TestTable {
        name: "Primitive".to_owned(),
        id: Uuid::parse_str("222DA01F9ABD4d9d80C702AF85C822A8").unwrap(),
        group_key: "key".to_owned(),
        grouping: "".to_owned(),
        fields: vec![
            TestField::new("key", DataType::UInt64),
            TestField::new("f64", DataType::Float64),
            TestField::new("i64", DataType::Int64),
        ],
    }
}

fn account_received_table() -> TestTable {
    TestTable {
        name: "Received".to_owned(),
        id: Uuid::parse_str("333DA01F9ABD4d9d80C702AF85C822A8").unwrap(),
        group_key: "receiver".to_owned(),
        grouping: "account".to_owned(),
        fields: vec![
            TestField::new("amount", DataType::Float64),
            TestField::new("receiver", DataType::UInt64),
        ],
    }
}

fn store_received_table() -> TestTable {
    TestTable {
        name: "StoreReceived".to_owned(),
        id: Uuid::parse_str("444DA01F9ABD4d9d80C702AF85C822A8").unwrap(),
        group_key: "store_id".to_owned(),
        grouping: "store".to_owned(),
        fields: vec![
            TestField::new("amount", DataType::Float64),
            TestField::new("store_id", DataType::UInt64),
            TestField::new("sender_id", DataType::UInt64),
        ],
    }
}

#[tokio::test]
async fn test_basic_sum() {
    insta::assert_yaml_snapshot!(
        compile(
            TestScript {
                tables: vec![account_sent_table()],
                feature_set: FeatureSet {
                    formulas: vec![
                        formula("f1", "sum(Sent.amount)"),
                        formula("f2", "Sent.amount | sum($input)"),
                    ],
                    query: "{f1, f2}".to_owned(),
                },
            },
            None
        )
        .await
    );
}

#[tokio::test]
async fn test_projection_pushdown() {
    insta::assert_yaml_snapshot!(
        compile(
            TestScript {
                tables: vec![account_sent_table()],
                feature_set: FeatureSet {
                    formulas: vec![],
                    query: "{ amount: Sent | first() | $input.amount  }".to_owned(),
                },
            },
            None
        )
        .await
    );
}

#[tokio::test]
async fn test_unexported() {
    let compute_plan = compile(
        TestScript {
            tables: vec![account_sent_table()],
            feature_set: FeatureSet {
                formulas: vec![formula("f1", "sum(Sent.amount)"), formula("f2", "f1 + 1")],
                query: "{f2}".to_owned(),
            },
        },
        None,
    )
    .await;

    insta::assert_yaml_snapshot!(compute_plan);
}

#[tokio::test]
async fn test_basic_sum_when_event() {
    let compute_plan = compile(
        TestScript {
            tables: vec![account_sent_table()],
            feature_set: FeatureSet {
                formulas: vec![formula("sum_amount", "sum(Sent.amount)")],
                query: "{ sum_amount } | when(Sent.amount > 10)".to_owned(),
            },
        },
        None,
    )
    .await;

    insta::assert_yaml_snapshot!(compute_plan);
}
#[tokio::test]
async fn test_count_since_window() {
    let compute_plan = compile(
        TestScript {
            tables: vec![account_sent_table()],
            feature_set: FeatureSet {
                formulas: vec![formula(
                    "count_amount",
                    "count(Sent.amount, window=since(Sent.amount > 10))",
                )],
                query: "{ count_amount }".to_owned(),
            },
        },
        None,
    )
    .await;

    insta::assert_yaml_snapshot!(compute_plan);
}

#[tokio::test]
async fn test_basic_sum_when_aggregate() {
    let compute_plan = compile(
        TestScript {
            tables: vec![account_sent_table()],
            feature_set: FeatureSet {
                formulas: vec![formula("sum_amount", "sum(Sent.amount)")],
                query: "{ sum_amount } | when($input.sum_amount > 10)".to_owned(),
            },
        },
        None,
    )
    .await;

    insta::assert_yaml_snapshot!(compute_plan);
}

#[tokio::test]
async fn test_lookup_same_grouping() {
    let compute_plan = compile(
        TestScript {
            tables: vec![account_sent_table(), account_received_table()],
            feature_set: FeatureSet {
                formulas: vec![
                    formula("sum_amount", "sum(Sent.amount)"),
                    formula(
                        "sum_received",
                        "lookup(last(Sent.receiver), sum(Received.amount))",
                    ),
                ],
                query: "{ sum_amount, sum_received }".to_owned(),
            },
        },
        None,
    )
    .await;

    insta::assert_yaml_snapshot!(compute_plan);
}

#[tokio::test]
async fn test_with_key() {
    let compute_plan = compile(
        TestScript {
            tables: vec![account_sent_table(), store_received_table()],
            feature_set: FeatureSet {
                formulas: vec![formula(
                    "StoreReceivedBySender",
                    "StoreReceived | with_key($input.sender_id, grouping = \"account\")",
                )],
                query: "{ sum_sent: Sent.amount | sum(), sum_store_received: \
                        StoreReceivedBySender.amount | sum() }"
                    .to_owned(),
            },
        },
        None,
    )
    .await;

    insta::assert_yaml_snapshot!(compute_plan);
}

#[tokio::test]
async fn test_with_key_default_grouping() {
    let compute_plan = compile(
        TestScript {
            tables: vec![account_sent_table(), store_received_table()],
            feature_set: FeatureSet {
                formulas: vec![formula(
                    "StoreReceivedBySender",
                    "StoreReceived | with_key($input.sender_id)",
                )],
                query: "{ sum_store_received: StoreReceivedBySender.amount | sum() }".to_owned(),
            },
        },
        None,
    )
    .await;

    insta::assert_yaml_snapshot!(compute_plan);
}

#[tokio::test]
async fn test_lookup_same_grouping_with_slicing() {
    let compute_plan = compile(
        TestScript {
            tables: vec![account_sent_table(), account_received_table()],
            feature_set: FeatureSet {
                formulas: vec![
                    formula("sum_amount", "sum(Sent.amount)"),
                    formula(
                        "sum_received",
                        "lookup(last(Sent.receiver), sum(Received.amount))",
                    ),
                ],
                query: "{ sum_amount, sum_received }".to_owned(),
            },
        },
        Some(SliceRequest {
            slice: Some(slice_request::Slice::Percent(slice_request::PercentSlice {
                percent: 0.5,
            })),
        }),
    )
    .await;

    insta::assert_yaml_snapshot!(compute_plan);
}

#[tokio::test]
async fn test_lookup_diff_grouping() {
    let compute_plan = compile(
        TestScript {
            tables: vec![account_sent_table(), store_received_table()],
            feature_set: FeatureSet {
                formulas: vec![
                    formula("sum_amount", "sum(Sent.amount)"),
                    formula(
                        "sum_store_received",
                        "lookup(last(Sent.store), sum(StoreReceived.amount))",
                    ),
                ],
                query: "{ sum_amount, sum_store_received }".to_owned(),
            },
        },
        None,
    )
    .await;

    insta::assert_yaml_snapshot!(compute_plan);
}

#[tokio::test]
async fn test_cast_in_record() {
    let compute_plan = compile(
        TestScript {
            tables: vec![primitive_table()],
            feature_set: FeatureSet {
                formulas: Vec::new(),
                query: "{ x: Primitive.i64 } | else ({ x: Primitive.f64 })".to_owned(),
            },
        },
        None,
    )
    .await;

    insta::assert_yaml_snapshot!(compute_plan);
}

#[tokio::test]
async fn test_null_value() {
    let compute_plan = compile(
        TestScript {
            tables: vec![account_sent_table()],
            feature_set: FeatureSet {
                formulas: Vec::new(),
                query: "{ x: null | else(Sent.amount) }".to_owned(),
            },
        },
        None,
    )
    .await;

    insta::assert_yaml_snapshot!(compute_plan);
}

#[tokio::test]
async fn test_null_record_field() {
    let compute_plan = compile(
        TestScript {
            tables: vec![account_sent_table()],
            feature_set: FeatureSet {
                formulas: Vec::new(),
                query: "null | else({ a: Sent.amount })".to_owned(),
            },
        },
        None,
    )
    .await;

    insta::assert_yaml_snapshot!(compute_plan);
}

#[tokio::test]
async fn test_null_record() {
    let compute_plan = compile(
        TestScript {
            tables: vec![account_sent_table()],
            feature_set: FeatureSet {
                formulas: Vec::new(),
                query: "{a: Sent.amount, b: null } | else({ a: Sent.amount, b: 7 })".to_owned(),
            },
        },
        None,
    )
    .await;

    insta::assert_yaml_snapshot!(compute_plan);
}

#[tokio::test]
async fn test_valid_time_domains() {
    let compute_plan = compile(
        TestScript {
            tables: vec![account_sent_table(), store_received_table()],
            feature_set: FeatureSet {
                formulas: vec![formula(
                    "shift",
                    "Sent.amount | shift_until(Sent.amount > 0) | shift_until(Sent.amount < 24)",
                )],
                query: "{ a: shift + shift }".to_owned(),
            },
        },
        None,
    )
    .await;

    insta::assert_yaml_snapshot!(compute_plan);
}

#[tokio::test]
async fn test_since_daily() {
    let compute_plan = compile(
        TestScript {
            tables: vec![account_sent_table(), store_received_table()],
            feature_set: FeatureSet {
                formulas: vec![formula(
                    "daily",
                    "count(Sent.amount, window=since(daily()))",
                )],
                query: "{ a: daily }".to_owned(),
            },
        },
        None,
    )
    .await;

    insta::assert_yaml_snapshot!(compute_plan);
}

#[tokio::test]
async fn test_since_daily_multiple_passes() {
    let compute_plan = compile(
        TestScript {
            tables: vec![account_sent_table(), store_received_table()],
            feature_set: FeatureSet {
                formulas: vec![formula(
                    "count_since",
                    "count(Sent.amount, window=since(daily()))",
                )],
                query: "{ a: count_since | shift_until(Sent.amount > 10) | sum($input, \
                        window=since(daily())) }"
                    .to_owned(),
            },
        },
        None,
    )
    .await;

    insta::assert_yaml_snapshot!(compute_plan);
}

#[tokio::test]
async fn test_tab_characters_are_allowed() {
    let compute_plan = compile(
        TestScript {
            tables: vec![account_sent_table(), store_received_table()],
            feature_set: FeatureSet {
                formulas: vec![],
                query: "	{ a: 	sum(Sent.amount) }".to_owned(),
            },
        },
        None,
    )
    .await;

    insta::assert_yaml_snapshot!(compute_plan);
}

#[tokio::test]
async fn test_extend_infers_implicit_input() {
    // Tests that we don't get any diagnostic warnings when implicitly
    // inferring input using an extend.
    //
    // We don't need to assert the plan yaml, since `compile` implicitly
    // asserts that no diagnostic warnings were written.
    let query = "Sent | extend({ sum_extend: sum(Sent.amount) })";
    compile(
        TestScript {
            tables: vec![account_sent_table(), store_received_table()],
            feature_set: FeatureSet {
                formulas: vec![],
                query: query.to_owned(),
            },
        },
        None,
    )
    .await;
}

#[tokio::test]
async fn test_incompatible_err() {
    let error = compile_err(TestScript {
        tables: vec![account_sent_table(), store_received_table()],
        feature_set: FeatureSet {
            formulas: vec![],
            query: "Sent.amount == \"hello\"".to_owned(),
        },
    })
    .await;

    insta::assert_snapshot!(error, @r###"
    error[E0015]: Incompatible argument types
      --> Query:1:13
      |
    1 | Sent.amount == "hello"
      | ----------- ^^ ------- Type: string
      | |           |
      | |           Incompatible types for call to 'eq'
      | Type: f64
    "###);
}

#[tokio::test]
async fn test_incompatible_shift_functions() {
    let warnings = compile_warnings(TestScript {
        tables: vec![account_sent_table(), store_received_table()],
        feature_set: FeatureSet {
            formulas: vec![],
            query: "{x: (Sent.amount | shift_until($input > 0)) + (Sent.amount | \
                    shift_until($input > 1)) }"
                .to_owned(),
        },
    })
    .await;

    insta::assert_snapshot!(warnings, @r###"
    warning[W2000]: Incompatible time domains
      --> Query:1:45
      |
    1 | {x: (Sent.amount | shift_until($input > 0)) + (Sent.amount | shift_until($input > 1)) }
      |     --------------------------------------- ^ --------------------------------------- Time Domain: ShiftUntil { predicate: 28 }
      |     |                                       |
      |     |                                       Incompatible time domains for operation
      |     Time Domain: ShiftUntil { predicate: 16 }
    "###);
}

#[tokio::test]
async fn test_unused_lhs_pipe() {
    let warnings = compile_warnings(TestScript {
        tables: vec![account_sent_table(), store_received_table()],
        feature_set: FeatureSet {
            formulas: vec![],
            query: "{ x: Sent.amount | sum(Sent.amount) }".to_owned(),
        },
    })
    .await;

    insta::assert_snapshot!(warnings, @r###"
    warning[W2001]: Unused binding
      --> Query:1:18
      |
    1 | { x: Sent.amount | sum(Sent.amount) }
      |                  ^ Left-hand side of pipe not used
    "###);
}

#[tokio::test]
async fn test_unused_let_binding() {
    let warnings = compile_warnings(TestScript {
        tables: vec![account_sent_table(), store_received_table()],
        feature_set: FeatureSet {
            formulas: vec![],
            query: "let unused = sum(Sent.amount) in { x: Sent.amount }".to_owned(),
        },
    })
    .await;

    insta::assert_snapshot!(warnings, @r###"
    warning[W2001]: Unused binding
      --> Query:1:5
      |
    1 | let unused = sum(Sent.amount) in { x: Sent.amount }
      |     ^^^^^^ Unused binding 'unused'
    "###);
}

#[tokio::test]
async fn test_incompatible_time_domain_tables_add() {
    let warnings = compile_warnings(TestScript {
        tables: vec![account_sent_table(), account_received_table()],
        feature_set: FeatureSet {
            formulas: vec![],
            query: "{x: Sent.amount + Received.amount }".to_owned(),
        },
    })
    .await;

    insta::assert_snapshot!(warnings, @r###"
    warning[W2000]: Incompatible time domains
      --> Query:1:17
      |
    1 | {x: Sent.amount + Received.amount }
      |     ----------- ^ --------------- Time Domain: Table 'Received'
      |     |           |
      |     |           Incompatible time domains for operation
      |     Time Domain: Table 'Sent'
    "###);
}

#[tokio::test]
async fn test_undefined_field_err() {
    let error = compile_err(TestScript {
        tables: vec![account_sent_table(), store_received_table()],
        feature_set: FeatureSet {
            formulas: vec![],
            query: "Sent.amt == 5".to_owned(),
        },
    })
    .await;

    insta::assert_snapshot!(error, @r###"
    error[E0001]: Illegal field reference
      --> Query:1:6
      |
    1 | Sent.amt == 5
      |      ^^^ No field named 'amt'
      |
      = Nearest fields: 'amount', 'store', 'sender', 'receiver'
    "###);
}

#[tokio::test]
async fn test_invalid_field_ref_base_err() {
    let error = compile_err(TestScript {
        tables: vec![account_sent_table(), store_received_table()],
        feature_set: FeatureSet {
            formulas: vec![],
            query: "Sent.amount.foo".to_owned(),
        },
    })
    .await;

    insta::assert_snapshot!(error, @r###"
    error[E0001]: Illegal field reference
      --> Query:1:1
      |
    1 | Sent.amount.foo
      | ^^^^^^^^^^^ No fields for non-record base type f64
    "###);
}

#[tokio::test]
async fn test_incompatible_grouping_add() {
    let error = compile_err(TestScript {
        tables: vec![account_received_table(), store_received_table()],
        feature_set: FeatureSet {
            formulas: vec![],
            query: "{ amount: sum(Received.amount) + sum(StoreReceived.amount) }".to_owned(),
        },
    })
    .await;

    insta::assert_snapshot!(error, @r###"
    error[W2001]: Incompatible grouping
      --> Query:1:32
      |
    1 | { amount: sum(Received.amount) + sum(StoreReceived.amount) }
      |           -------------------- ^ ------------------------- Grouping: 'store'
      |           |                    |
      |           |                    Incompatible grouping for operation 'add'
      |           Grouping: 'account'
    "###);
}

#[tokio::test]
async fn test_incompatible_grouping_record() {
    let error = compile_err(TestScript {
        tables: vec![account_received_table(), store_received_table()],
        feature_set: FeatureSet {
            formulas: vec![],
            query: "{ sum_account: sum(Received.amount), sum_store: sum(StoreReceived.amount) }"
                .to_owned(),
        },
    })
    .await;

    insta::assert_snapshot!(error, @r###"
    error[W2001]: Incompatible grouping
      --> Query:1:1
      |
    1 | { sum_account: sum(Received.amount), sum_store: sum(StoreReceived.amount) }
      | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      | |              |                                |
      | |              |                                Grouping: 'store'
      | |              Grouping: 'account'
      | Incompatible grouping for operation 'record creation'
    "###);
}

#[tokio::test]
async fn test_duplicate_formula() {
    let error = compile_err(TestScript {
        tables: vec![account_received_table(), store_received_table()],
        feature_set: FeatureSet {
            formulas: vec![
                formula("a", "Received.amount"),
                formula("a", "Received.amount"),
            ],
            query: "{ a }".to_owned(),
        },
    })
    .await;

    insta::assert_snapshot!(error, @r###"
    error[E0004]: Formula already defined
     = Formula name 'a' is defined at index 0 and again at 1.
    "###);
}

#[tokio::test]
async fn test_undefined_formula() {
    let error = compile_err(TestScript {
        tables: vec![account_received_table(), store_received_table()],
        feature_set: FeatureSet {
            formulas: vec![],
            query: "{ a }".to_owned(),
        },
    })
    .await;

    insta::assert_snapshot!(error, @r###"
    error[E0006]: Unbound reference
      --> Query:1:3
      |
    1 | { a }
      |   ^ No reference named 'a'
      |
      = Nearest matches: 'Received', 'StoreReceived'
    "###);
}

// Tests that a function that is rewritten into another fenl function
// correctly displays the error on the original signature.
#[tokio::test]
async fn test_invalid_rewritten_function() {
    let error = compile_err(TestScript {
        tables: vec![account_received_table(), store_received_table()],
        feature_set: FeatureSet {
            formulas: vec![],
            query: "{ a: sqrt(Sent.amount, Sent.amount) }".to_owned(),
        },
    })
    .await;

    insta::assert_snapshot!(error, @r###"
    error[E0008]: Invalid arguments
      --> Query:1:24
      |
    1 | { a: sqrt(Sent.amount, Sent.amount) }
      |                        ^^^^^^^^^^^ First unexpected argument
      |
      = Expected 1 but got 2
    "###);
}

#[tokio::test]
async fn test_output_type_is_struct_diagnostic() {
    let error = compile_err(TestScript {
        tables: vec![account_received_table(), store_received_table()],
        feature_set: FeatureSet {
            formulas: vec![],
            query: "Received.amount".to_owned(),
        },
    })
    .await;

    insta::assert_snapshot!(error, @r###"
    error[E0013]: Invalid output type
     = Output type must be a record, but was f64
    "###);
}
