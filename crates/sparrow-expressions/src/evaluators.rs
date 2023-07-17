use std::borrow::Cow;

use arrow_array::types::ArrowPrimitiveType;
use arrow_schema::{DataType, Schema};
use hashbrown::HashMap;
use index_vec::IndexVec;
use itertools::Itertools;
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_physical::Expr;

use crate::evaluator::Evaluator;
use crate::values::{ArrayRefValue, BooleanValue, PrimitiveValue, StringValue, StructValue};
use crate::Error;

mod cast;
mod coalesce;
mod column;
mod comparison;
mod field_ref;
mod hash;
mod is_valid;
mod json_field;
mod literal;
mod logical;
mod macros;
mod math;
mod record;
mod string;
mod time;

/// Type alias for a function used to create an [Evaluator].
///
/// This type is equivalent to dynamic functions with signatures like:
///
/// ```
/// fn f<'a>(info: StaticInfo<'a>) -> error_stack::Result<Box<dyn Evaluator>, Error> + Send + Sync
/// ```
///
/// This corresponds to the functions each evaluator registers for creating
/// an evaluator from the static information (types, constant arguments, and
/// information about the arguments).
type EvaluatorFactoryFn =
    dyn for<'a> Fn(StaticInfo<'a>) -> error_stack::Result<Box<dyn Evaluator>, Error> + Send + Sync;

/// Factory for creating evaluators with a specific name.
struct EvaluatorFactory {
    name: &'static str,
    create: &'static EvaluatorFactoryFn,
}

inventory::collect!(EvaluatorFactory);

pub struct StaticInfo<'a> {
    input_schema: &'a Schema,
    name: &'a Cow<'static, str>,
    literal_args: &'a [ScalarValue],
    args: Vec<&'a StaticArg<'a>>,
    result_type: &'a DataType,
}

/// Information available when creating evaluators for a query.
pub struct StaticArg<'a> {
    /// Expression index of argument.
    index: usize,
    /// The DataType of the argument.
    data_type: &'a DataType,
}

impl<'a> StaticArg<'a> {
    pub fn primitive<T: ArrowPrimitiveType>(
        &self,
    ) -> error_stack::Result<PrimitiveValue<T>, Error> {
        PrimitiveValue::try_new(self.index, self.data_type)
    }

    pub fn boolean(&self) -> error_stack::Result<BooleanValue, Error> {
        BooleanValue::try_new(self.index, self.data_type)
    }

    pub fn string(&self) -> error_stack::Result<StringValue, Error> {
        StringValue::try_new(self.index, self.data_type)
    }

    pub fn array_ref(&self) -> ArrayRefValue {
        ArrayRefValue::new(self.index)
    }

    pub fn struct_(&self) -> error_stack::Result<StructValue, Error> {
        StructValue::try_new(self.index, self.data_type)
    }
}

impl<'a> StaticInfo<'a> {
    /// Return the scalar value corresponding to the exactly-one literal arguments.
    fn literal(&self) -> error_stack::Result<&'a ScalarValue, Error> {
        error_stack::ensure!(
            self.args.len() == 1,
            Error::InvalidLiteralCount {
                name: self.name.clone(),
                expected: 1,
                actual: self.args.len()
            }
        );
        Ok(&self.literal_args[0])
    }

    /// Return the string value corresponding to the exactly-one literal arguments.
    fn literal_string(&self) -> error_stack::Result<&'a str, Error> {
        match self.literal()? {
            ScalarValue::Utf8(Some(string)) => Ok(string),
            ScalarValue::LargeUtf8(Some(string)) => Ok(string),
            other => {
                error_stack::bail!(Error::InvalidLiteral {
                    expected: "non-null string",
                    actual: other.clone()
                })
            }
        }
    }

    fn unpack_argument(mut self) -> error_stack::Result<&'a StaticArg<'a>, Error> {
        error_stack::ensure!(
            self.args.len() == 1,
            Error::InvalidArgumentCount {
                name: self.name.clone(),
                expected: 1,
                actual: self.args.len()
            }
        );
        Ok(self.args.swap_remove(0))
    }

    fn unpack_arguments<T: itertools::traits::HomogeneousTuple<Item = &'a StaticArg<'a>>>(
        self,
    ) -> error_stack::Result<T, Error> {
        let actual = self.args.len();
        let mut args = self.args.into_iter();
        match args.next_tuple() {
            Some(t) => Ok(t),
            None => {
                error_stack::bail!(Error::InvalidArgumentCount {
                    name: self.name.clone(),
                    expected: T::num_items(),
                    actual
                });
            }
        }
    }
}

/// Create the evaluators for the given expressions.
pub(super) fn create_evaluators(
    input_schema: &Schema,
    exprs: &[Expr],
) -> error_stack::Result<Vec<Box<dyn Evaluator>>, Error> {
    // Static information (index in expressions, type, etc.) for each expression in `exprs`.
    // This is used to locate the information about arguments to the remaining expressions.
    //
    // It is only needed while instantiating the evaluators.
    let mut expressions = IndexVec::with_capacity(exprs.len());
    let mut evaluators = Vec::with_capacity(exprs.len());
    for (index, expr) in exprs.iter().enumerate() {
        let args = expr.args.iter().map(|index| &expressions[*index]).collect();
        let info = StaticInfo {
            input_schema,
            name: &expr.name,
            literal_args: &expr.literal_args,
            args,
            result_type: &expr.result_type,
        };

        evaluators.push(create_evaluator(info)?);
        expressions.push(StaticArg {
            index,
            data_type: &expr.result_type,
        });
    }
    Ok(evaluators)
}

// This needs to be marked lazy so it is run after the evaluators
// are submitted to the inventory.
#[static_init::dynamic(lazy)]
static EVALUATORS: HashMap<&'static str, &'static EvaluatorFactoryFn> = {
    let result: HashMap<_, _> = inventory::iter::<EvaluatorFactory>()
        .map(|e| (e.name, e.create))
        .collect();

    debug_assert_eq!(
        result.len(),
        inventory::iter::<EvaluatorFactory>().count(),
        "Expected every evaluator to appear in evaluator map. Duplicates: {:?}",
        inventory::iter::<EvaluatorFactory>()
            .map(|e| e.name)
            .duplicates()
            .collect::<Vec<_>>()
    );
    result
};

fn create_evaluator(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let Some(create) = EVALUATORS.get(info.name.as_ref()) else {
        error_stack::bail!(Error::NoEvaluator(info.name.clone()))
    };
    create(info)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_evaluator_registration() {
        assert!((*super::EVALUATORS).contains_key("add"))
    }
}
