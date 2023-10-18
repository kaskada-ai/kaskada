use std::marker::PhantomData;

use arrow_array::cast::{as_boolean_array, as_primitive_array, as_string_array, as_struct_array};
use arrow_array::types::ArrowPrimitiveType;
use arrow_array::{ArrayRef, BooleanArray, PrimitiveArray, StringArray, StructArray};
use arrow_schema::DataType;

use sparrow_interfaces::expression::Error;

pub trait WorkAreaValue: std::fmt::Debug + Clone + Copy {
    type Array<'a>;
    fn access<'a>(&self, arrays: &'a [ArrayRef]) -> Self::Array<'a>;
}

pub struct PrimitiveValue<T: ArrowPrimitiveType> {
    index: usize,
    /// Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T: ArrowPrimitiveType> PrimitiveValue<T> {
    pub fn try_new(index: usize, data_type: &DataType) -> error_stack::Result<Self, Error> {
        error_stack::ensure!(
            &T::DATA_TYPE == data_type,
            Error::InvalidArgumentType {
                expected: T::DATA_TYPE,
                actual: data_type.clone()
            }
        );

        Ok(PrimitiveValue {
            index,
            _phantom: Default::default(),
        })
    }
}

impl<T: ArrowPrimitiveType> std::fmt::Debug for PrimitiveValue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrimitiveRef")
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

impl<T: ArrowPrimitiveType> Clone for PrimitiveValue<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T: ArrowPrimitiveType> Copy for PrimitiveValue<T> {}

impl<T: ArrowPrimitiveType> WorkAreaValue for PrimitiveValue<T> {
    type Array<'a> = &'a PrimitiveArray<T>;

    fn access<'a>(&self, arrays: &'a [ArrayRef]) -> Self::Array<'a> {
        as_primitive_array(arrays[self.index].as_ref())
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct BooleanValue {
    index: usize,
}

impl BooleanValue {
    pub fn try_new(index: usize, data_type: &DataType) -> error_stack::Result<Self, Error> {
        error_stack::ensure!(
            data_type == &DataType::Boolean,
            Error::InvalidArgumentType {
                expected: DataType::Boolean,
                actual: data_type.to_owned()
            }
        );
        Ok(Self { index })
    }
}

impl WorkAreaValue for BooleanValue {
    type Array<'a> = &'a BooleanArray;

    fn access<'a>(&self, arrays: &'a [ArrayRef]) -> Self::Array<'a> {
        as_boolean_array(arrays[self.index].as_ref())
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct StringValue {
    index: usize,
}

impl StringValue {
    pub fn try_new(index: usize, data_type: &DataType) -> error_stack::Result<Self, Error> {
        error_stack::ensure!(
            data_type == &DataType::Utf8,
            Error::InvalidArgumentType {
                expected: DataType::Utf8,
                actual: data_type.to_owned()
            }
        );
        Ok(Self { index })
    }
}

impl WorkAreaValue for StringValue {
    type Array<'a> = &'a StringArray;

    fn access<'a>(&self, arrays: &'a [ArrayRef]) -> Self::Array<'a> {
        as_string_array(arrays[self.index].as_ref())
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct StructValue {
    index: usize,
}

impl StructValue {
    pub fn try_new(index: usize, data_type: &DataType) -> error_stack::Result<Self, Error> {
        error_stack::ensure!(
            matches!(data_type, DataType::Struct(_)),
            Error::InvalidNonStructArgumentType {
                actual: data_type.clone()
            }
        );
        Ok(Self { index })
    }
}

impl WorkAreaValue for StructValue {
    type Array<'a> = &'a StructArray;

    fn access<'a>(&self, arrays: &'a [ArrayRef]) -> Self::Array<'a> {
        as_struct_array(arrays[self.index].as_ref())
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct ArrayRefValue {
    index: usize,
}

impl ArrayRefValue {
    pub fn new(index: usize) -> Self {
        Self { index }
    }
}

impl WorkAreaValue for ArrayRefValue {
    type Array<'a> = &'a ArrayRef;

    fn access<'a>(&self, arrays: &'a [ArrayRef]) -> Self::Array<'a> {
        &arrays[self.index]
    }
}
