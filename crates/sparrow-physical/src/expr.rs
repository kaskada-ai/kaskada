use std::borrow::Cow;

use arrow_schema::DataType;
use index_vec::IndexVec;
use sparrow_arrow::scalar_value::ScalarValue;

index_vec::define_index_type! {
    /// The identifier (index) of an expression.
    pub struct ExprId = u32;

    DISPLAY_FORMAT = "{}";
}

/// Represents 1 or more values computed by expressions.
///
/// Expressions are evaluated by producing a sequence of columns
/// and then selecting specific columns from those computed.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Exprs {
    /// The expressions computing the intermediate values.
    pub exprs: IndexVec<ExprId, Expr>,
    /// The indices of columns to output.
    pub outputs: Vec<ExprId>,
}

impl Exprs {
    pub fn empty() -> Self {
        Self {
            exprs: IndexVec::default(),
            outputs: vec![],
        }
    }

    /// Create expressions computing the value of the last expression.
    pub fn singleton(exprs: Vec<Expr>) -> Self {
        let output = exprs.len() - 1;
        Self {
            exprs: exprs.into(),
            outputs: vec![output.into()],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.outputs.is_empty()
    }

    pub fn is_singleton(&self) -> bool {
        self.outputs.len() == 1
    }

    /// Return the number of outputs produced by these expressions.
    pub fn output_len(&self) -> usize {
        self.outputs.len()
    }
}

/// A physical expression which describes how a value should be computed.
///
/// Generally, each expression computes a column of values from zero or more
/// input columns. Expressions appear in a variety of places within the steps
/// that make up a physical plan.
#[derive(Debug, serde::Serialize, serde::Deserialize)]

pub struct Expr {
    /// The instruction being applied by this expression.
    ///
    /// Similar to an opcode or function.
    ///
    /// Generally, interning owned strings to the specific owned static strings is preferred.
    pub name: Cow<'static, str>,
    /// Zero or more literal-valued arguments.
    pub literal_args: Vec<ScalarValue>,
    /// Arguments to the expression.
    ///
    /// These are indices referencing earlier expressions.
    pub args: Vec<ExprId>,
    /// The type produced by the expression.
    pub result_type: DataType,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_physical_exprs_yaml() {
        let exprs = vec![
            Expr {
                name: "column".into(),
                literal_args: vec![ScalarValue::Utf8(Some("foo".to_owned()))],
                args: vec![],
                result_type: DataType::Int32,
            },
            Expr {
                name: "column".into(),
                literal_args: vec![ScalarValue::Utf8(Some("bar".to_owned()))],
                args: vec![],
                result_type: DataType::Int32,
            },
            Expr {
                name: "add".into(),
                literal_args: vec![],
                args: vec![0.into(), 1.into()],
                result_type: DataType::Int32,
            },
        ];

        // Use serde_yaml directly, since insta uses a non-standard Yaml formatter.
        let yaml = serde_yaml::to_string(&exprs).unwrap();
        insta::assert_snapshot!(yaml)
    }
}
