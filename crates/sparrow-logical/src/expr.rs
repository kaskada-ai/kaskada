use crate::{Error, Grouping};
use arrow_schema::{DataType, TimeUnit};
use sparrow_types::Types;
use std::sync::Arc;
use uuid::Uuid;

/// Represents an operation applied to 0 or more arguments.
#[derive(Debug)]
pub struct Expr {
    /// The instruction being applied by this expression.
    pub name: &'static str,
    /// Zero or more literal-valued arguments.
    ///
    /// In logical plans, very few things should use literal_args. Currently, these are:
    ///
    /// 1. The expression corresponding to a literal has name `literal`, `literal_args`
    ///    corresponding to the value, and empty `args`.
    /// 2. Sources have name `read`, `literal_args` containing the UUID of the source
    ///    being read, and empty `args`.
    ///
    /// Other things that use `literal_args` (such as field refs) in the physical
    /// plan are handled by the conversion of logical plans to physical plans.
    pub literal_args: Vec<Literal>,
    /// Arguments to the expression.
    pub args: Vec<ExprRef>,
    /// The type produced by the expression.
    pub result_type: DataType,
    /// The grouping associated with the expression.
    pub grouping: Grouping,
}

#[derive(Debug, Hash, PartialEq, Eq, Ord, PartialOrd, Clone, enum_as_inner::EnumAsInner)]
pub enum Literal {
    Null,
    Bool(bool),
    String(String),
    Int64(i64),
    UInt64(u64),
    // Decorum is needed to provide a total ordering on `f64` so we can
    // derive `Ord` and `PartialOrd`.
    Float64(decorum::Total<f64>),
    Timedelta { seconds: i64, nanos: i64 },
    Uuid(Uuid),
}

impl Literal {
    pub fn new_str(str: impl Into<String>) -> Self {
        Self::String(str.into())
    }
}

impl Expr {
    pub fn try_new(name: &'static str, args: Vec<ExprRef>) -> error_stack::Result<Self, Error> {
        let Types {
            arguments: arg_types,
            result: result_type,
        } = crate::typecheck::typecheck(name, &args)?;

        // If any of the types are different, we'll need to create new arguments.
        let args = args
            .into_iter()
            .zip(arg_types)
            .map(|(arg, arg_type)| arg.cast(arg_type))
            .collect::<Result<Vec<_>, _>>()?;

        let grouping = Grouping::from_args(&args)?;

        Ok(Self {
            name,
            literal_args: vec![],
            args,
            result_type,
            grouping,
        })
    }

    /// Create a new literal node referencing a UUID.
    ///
    /// This can be used for sources, UDFs, etc.
    ///
    /// Generally, the `name` should identify the kind of thing being referenced (source, UDF, etc.)
    /// and the `uuid` should identify the specific thing being referenced.
    pub fn new_uuid(
        name: &'static str,
        uuid: Uuid,
        result_type: DataType,
        grouping: Grouping,
    ) -> Self {
        Self {
            name,
            literal_args: vec![Literal::Uuid(uuid)],
            args: vec![],
            result_type,
            grouping,
        }
    }

    pub fn new_literal(literal: Literal) -> Self {
        let result_type = match literal {
            Literal::Null => DataType::Null,
            Literal::Bool(_) => DataType::Boolean,
            Literal::String(_) => DataType::Utf8,
            Literal::Int64(_) => DataType::Int64,
            Literal::UInt64(_) => DataType::UInt64,
            Literal::Float64(_) => DataType::Float64,
            Literal::Timedelta { .. } => DataType::Duration(TimeUnit::Nanosecond),
            Literal::Uuid(_) => DataType::FixedSizeBinary(BYTES_IN_UUID),
        };
        Self {
            name: "literal",
            literal_args: vec![literal],
            args: vec![],
            result_type,
            grouping: Grouping::Literal,
        }
    }

    pub fn new_literal_str(str: impl Into<String>) -> Self {
        Self::new_literal(Literal::String(str.into()))
    }

    /// Create a new cast expression to the given type.
    pub fn cast(self: Arc<Self>, data_type: DataType) -> error_stack::Result<Arc<Self>, Error> {
        if self.result_type == data_type {
            Ok(self)
        } else {
            let grouping = self.grouping;
            Ok(Arc::new(Expr {
                name: "cast",
                literal_args: vec![],
                args: vec![self],
                result_type: data_type,
                grouping,
            }))
        }
    }

    /// If this expression is a literal, return the corresponding value.
    pub fn literal_opt(&self) -> Option<&Literal> {
        if self.name == "literal" {
            debug_assert_eq!(self.literal_args.len(), 1);
            Some(&self.literal_args[0])
        } else {
            None
        }
    }

    /// If this expression is a literal string, return it.
    ///
    /// This returns `None` if:
    ///   1. This expression is not a literal.
    ///   2. This expression is not a string literal.
    pub fn literal_str_opt(&self) -> Option<&str> {
        self.literal_opt().and_then(|scalar| match scalar {
            Literal::String(str) => Some(str.as_str()),
            _ => None,
        })
    }
}

const BYTES_IN_UUID: i32 = (std::mem::size_of::<uuid::Bytes>() / std::mem::size_of::<u8>()) as i32;

/// Reference counted expression.
pub type ExprRef = Arc<Expr>;

#[cfg(test)]
mod tests {
    use arrow_schema::Field;

    use super::*;

    #[test]
    fn test_literal() {
        let literal = Expr::new_literal(Literal::String("hello".to_owned()));
        insta::assert_debug_snapshot!(literal);
    }

    #[test]
    fn test_fieldref() {
        let uuid = Uuid::from_u64_pair(42, 84);
        let source = Arc::new(Expr::new_uuid(
            "source",
            uuid,
            DataType::Struct(
                vec![
                    Field::new("a", DataType::Int64, true),
                    Field::new("b", DataType::Float64, false),
                ]
                .into(),
            ),
            Grouping::Literal,
        ));
        let field = Expr::try_new(
            "fieldref",
            vec![
                source,
                Arc::new(Expr::new_literal(Literal::String("a".to_owned()))),
            ],
        )
        .unwrap();
        insta::assert_debug_snapshot!(field);
    }

    #[test]
    fn test_function() {
        let uuid = Uuid::from_u64_pair(42, 84);
        let source = Arc::new(Expr::new_uuid(
            "source",
            uuid,
            DataType::Struct(
                vec![
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", DataType::Float64, false),
                ]
                .into(),
            ),
            Grouping::Literal,
        ));
        let a_i32 = Arc::new(
            Expr::try_new(
                "fieldref",
                vec![
                    source,
                    Arc::new(Expr::new_literal(Literal::String("a".to_owned()))),
                ],
            )
            .unwrap(),
        );

        // i32 + f64 literal => f64
        let a_i32_plus_1 = Expr::try_new(
            "add",
            vec![
                a_i32.clone(),
                Arc::new(Expr::new_literal(Literal::Float64(1.0.into()))),
            ],
        )
        .unwrap();
        insta::assert_debug_snapshot!(a_i32_plus_1);
    }
}
