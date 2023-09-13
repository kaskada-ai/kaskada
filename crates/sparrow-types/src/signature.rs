mod parser;

use arrow_schema::DataType;
use itertools::Itertools;

use crate::type_class::TypeClass;
use crate::{Error, FenlType, TypeVariable, Types};

/// Type signature for a function or instruction.
#[derive(Debug, PartialEq, Eq)]
pub struct Signature {
    /// Type parameters to the function, if any.
    pub(super) type_parameters: Vec<TypeParameter>,
    /// Parameters to the function.
    pub parameters: Vec<Parameter>,
    pub result: FenlType,
    /// If true, the last argument may be repeated 1 or more times.
    pub(super) variadic: bool,
}

impl<'a> serde::de::Deserialize<'a> for Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        deserializer.deserialize_str(SignatureDeserializer)
    }
}

struct SignatureDeserializer;

impl<'a> serde::de::Visitor<'a> for SignatureDeserializer {
    type Value = Signature;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a function signature")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Signature::parse(v).map_err(|e| E::custom(format!("{:?}", e)))
    }
}

/// A type-parameter within a signature.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct TypeParameter {
    /// The name of the type argument.
    pub(super) name: TypeVariable,
    /// Constraints on the type argument, if any.
    pub(super) type_classes: Vec<TypeClass>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Parameter {
    pub name: String,
    pub ty: FenlType,
}

impl Signature {
    pub fn parse(input: &str) -> error_stack::Result<Self, Error> {
        let signature = parser::parse_signature(input)?;
        signature.validate();
        Ok(signature)
    }

    pub fn instantiate(&self, arguments: Vec<DataType>) -> error_stack::Result<Types, Error> {
        crate::instantiate::instantiate(self, arguments)
    }

    /// Panics if the signature is invalid.
    pub(crate) fn validate(&self) {
        // The result type must appear in the parameters in order to be instantiated.
        #[cfg(debug_assertions)]
        {
            let mut parameter_generics = hashbrown::HashSet::new();
            self.parameters.iter().for_each(|p| {
                p.ty.add_generics(&mut parameter_generics);
            });
            let result_generics = self.result.generics();
            let mut unbound: Vec<_> = result_generics.difference(&parameter_generics).collect();
            unbound.sort();
            assert!(
                unbound.is_empty(),
                "Illegal signature: unbound generics in result {unbound:?}",
            );
        }

        // The names must be unique within the signature.
        {
            let mut duplicate_names = self
                .parameters
                .iter()
                .map(|p| &p.name)
                .duplicates()
                .peekable();
            debug_assert!(
                duplicate_names.peek().is_none(),
                "Duplicate parameter names: {}",
                duplicate_names.format_with(", ", |n, f| f(&format_args!("'{n}'")))
            )
        }
    }

    /// Return an iterator over the parameters for a call with `length` arguments.
    ///
    /// This will repeat the last parameter if the there are extra arguments and
    /// the signature has varargs.
    ///
    /// This will return `None` if the number of parameters is incorrect.
    pub(crate) fn iter_parameters(
        &self,
        length: usize,
    ) -> error_stack::Result<impl Iterator<Item = &'_ Parameter> + '_, Error> {
        let repetition = if self.variadic {
            error_stack::ensure!(
                self.parameters.len() < length,
                Error::NotEnoughArguments {
                    expected: self.parameters.len(),
                    actual: length,
                }
            );
            length - self.parameters.len()
        } else {
            error_stack::ensure!(
                self.parameters.len() == length,
                Error::IncorrectArgumentCount {
                    expected: self.parameters.len(),
                    actual: length,
                }
            );
            0
        };

        let repetition = std::iter::repeat(self.parameters.last().unwrap()).take(repetition);
        Ok(self.parameters.iter().chain(repetition))
    }
}

#[cfg(test)]
mod tests {
    use crate::signature::parser;
    use crate::{Error, Signature};

    fn instantiate(signature: &Signature, args: &str) -> error_stack::Result<String, Error> {
        let args = parser::parse_types(args).unwrap();
        let args = args
            .into_iter()
            .map(|t| t.instantiate(None).unwrap())
            .collect();
        let types = signature.instantiate(args)?;
        Ok(types.to_string())
    }

    #[test]
    fn test_instantiate_add() {
        let add = Signature::parse("<N: number>(lhs: N, rhs: N) -> N").unwrap();

        // i32 should be widened to i64
        assert_eq!(instantiate(&add, "i32, i64").unwrap(), "(i64, i64) -> i64");

        // i32 should be widened to i64; u32 should be cast to i64
        assert_eq!(instantiate(&add, "i32, u32").unwrap(), "(i64, i64) -> i64");

        // both should be cast to f64
        assert_eq!(instantiate(&add, "f32, i32").unwrap(), "(f64, f64) -> f64");
    }

    #[test]
    fn test_instantiate_neg() {
        let neg = Signature::parse("<S: signed>(n: S) -> S").unwrap();

        // i32 is signed
        assert_eq!(instantiate(&neg, "i32").unwrap(), "(i32) -> i32");

        // u32 needs to be widened to i64 to represent a negative number.
        assert_eq!(instantiate(&neg, "u32").unwrap(), "(i64) -> i64");

        // u64 needs to be widened (to float)
        assert_eq!(instantiate(&neg, "u64").unwrap(), "(f64) -> f64");
    }

    #[test]
    fn test_instantiate_coalesce() {
        let coalesce = Signature::parse("<T: any>(values: T...) -> T").unwrap();

        assert_eq!(
            instantiate(&coalesce, "i32, i64").unwrap(),
            "(i64, i64) -> i64"
        );

        assert_eq!(
            instantiate(&coalesce, "i32, i64, f64").unwrap(),
            "(f64, f64, f64) -> f64"
        );
    }

    #[test]
    fn test_instantiate_get_map() {
        let get = Signature::parse("<K: key, V: any>(key: K, map: map<K, V>) -> V").unwrap();

        assert_eq!(
            instantiate(&get, "i32, map<i32, f32>").unwrap(),
            "(i32, map<i32, f32, false>) -> f32"
        );

        assert_eq!(
            instantiate(&get, "i64, map<i64, f32>").unwrap(),
            "(i64, map<i64, f32, false>) -> f32"
        );

        assert_eq!(
            instantiate(&get, "i32, map<i32, i32>").unwrap(),
            "(i32, map<i32, i32, false>) -> i32"
        );
    }

    #[test]
    fn test_instantiate_flatten() {
        let flatten = Signature::parse("<T: any>(list: list<list<T>>) -> list<T>").unwrap();

        assert_eq!(
            instantiate(&flatten, "list<list<f32>>").unwrap(),
            "(list<list<f32>>) -> list<f32>"
        );
    }
}
