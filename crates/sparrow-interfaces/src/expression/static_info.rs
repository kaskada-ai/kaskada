use std::borrow::Cow;

use arrow_array::types::ArrowPrimitiveType;
use arrow_schema::DataType;
use itertools::Itertools;
use sparrow_arrow::scalar_value::ScalarValue;

use super::error::Error;
use crate::expression::work_area::{
    ArrayRefValue, BooleanValue, PrimitiveValue, StringValue, StructValue,
};

/// Static information available when creating an evaluator.
pub struct StaticInfo<'a> {
    /// Name of the instruction to be evaluated.
    pub name: &'a Cow<'static, str>,
    /// Literal (static) arguments to *this* expression.
    pub literal_args: &'a [ScalarValue],
    /// Arguments (dynamic) to *this* expression.
    pub args: Vec<&'a StaticArg<'a>>,
    /// Result type this expression should produce.
    ///
    /// For many instructions, this should be inferred from the arguments.
    /// It is part of the plan (a) for simplicity, so a plan may be executed
    /// without performing type-checking and (b) because some instructions
    /// need to know the result-type in order to execute (eg., cast).
    pub result_type: &'a DataType,
}

/// Information available when creating evaluators for a query.
pub struct StaticArg<'a> {
    /// Expression index of argument.
    pub index: usize,
    /// The DataType of the argument.
    pub data_type: &'a DataType,
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
    pub fn literal(&self) -> error_stack::Result<&'a ScalarValue, Error> {
        error_stack::ensure!(
            self.literal_args.len() == 1,
            Error::InvalidLiteralCount {
                name: self.name.clone(),
                expected: 1,
                actual: self.literal_args.len()
            }
        );
        Ok(&self.literal_args[0])
    }

    /// Return the string value corresponding to the exactly-one literal arguments.
    pub fn literal_string(&self) -> error_stack::Result<&'a str, Error> {
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

    pub fn unpack_argument(mut self) -> error_stack::Result<&'a StaticArg<'a>, Error> {
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

    pub fn unpack_arguments<T: itertools::traits::HomogeneousTuple<Item = &'a StaticArg<'a>>>(
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
