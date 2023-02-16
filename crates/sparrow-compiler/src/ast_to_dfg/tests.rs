use sparrow_api::kaskada::v1alpha::FeatureSet;
use sparrow_syntax::{Expr, FeatureSetPart};

use super::*;
use crate::resolve_arguments::resolve_recursive;

/// If the expected is an `Err`, it expects the given string to appear in
/// the error.
fn assert_type(expr_str: &'static str, expected: FenlType) {
    let mut data_context = DataContext::for_test();
    let mut dfg = data_context.create_dfg().unwrap();

    let feature_set = FeatureSet::default();
    let mut diagnostics = DiagnosticCollector::new(&feature_set);

    let expr = Expr::try_from_str(FeatureSetPart::Internal(expr_str), expr_str).unwrap();
    let mut diagnostic_builder = Vec::new();
    let expr = resolve_recursive(&expr, &mut diagnostic_builder).unwrap();
    let actual = ast_to_dfg(&mut data_context, &mut dfg, &mut diagnostics, &expr)
        .map(|i| i.value_type().clone());

    match actual {
        Ok(actual) => {
            assert_eq!(
                actual, expected,
                "Expected the type of '{expr_str}' to be {expected:?} but was {actual:?}"
            )
        }
        Err(e) => {
            panic!("Expected the type of '{expr_str}' to be {expected:?} but got error: {e:?}")
        }
    }
}

#[test]
fn test_check_unary() {
    assert_type("!true", FenlType::Concrete(DataType::Boolean));
    assert_type("!5", FenlType::Error);
    assert_type("exp(Table1.f_f64)", FenlType::Concrete(DataType::Float64));
}

#[test]
fn test_check_binary_numeric() {
    assert_type("Table1.x_i64 + 1", FenlType::Concrete(DataType::Int64));
    assert_type("Table1.y_i64 + 1", FenlType::Concrete(DataType::Int64));

    // The BigDecimal library reports `1.0` is an integer because it has zero
    // fractional part.
    // assert_type(&mut env, "x + 1.0", Ok(DataType::Float64.into()));
    assert_type("Table1.x_i64 + 1.1", FenlType::Concrete(DataType::Float64));

    assert_type("5 + Table1.x_i64", FenlType::Concrete(DataType::Int64));
}

#[test]
fn test_typecheck_pipe_when() {
    assert_type(
        "let x_plus_1 = Table1.x_i64 + 1.1 in x_plus_1 | when(Table1.x_i64 > 5)",
        FenlType::Concrete(DataType::Float64),
    );
}
