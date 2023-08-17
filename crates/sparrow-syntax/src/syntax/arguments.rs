use std::borrow::Cow;
use std::iter::FromIterator;
use std::ops::Index;

use bitvec::prelude::BitVec;
use itertools::Itertools;
use smallvec::SmallVec;

use crate::{FenlType, Located, Location};

/// Represents the parameters parsed for a method signature.
///
/// Each parameter has a name, a type, and an optional default value. Parameters
/// without a default value are "required". Parameters with a default value are
/// optional.
///
/// The parameter names for a single method must be unique.
///
/// Required parameters must appear before optional parameters.
#[derive(Debug, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct Parameters<T> {
    /// The names of each parameter.
    names: ArgVec<Located<String>>,
    /// Arguments that must be constant.
    constants: BitVec,
    /// The required type of each parameter.
    types: ArgVec<Located<FenlType>>,
    /// The default value (if any) associated with each parameter.
    defaults: ArgVec<Option<Located<T>>>,
    // Whether the last argument is vararg.
    // If true, the last argument need not have a default value, since
    // it can occur one or more times.
    pub has_vararg: bool,
}

/// Checks if the elements of this iterator are partitioned according to the
/// given predicate, such that all those that return `true` precede all those
/// that return `false`.
///
/// Based on the `Iterator::is_partitioned` method which isn't yet stabilized.
fn is_partitioned<T>(
    mut iter: impl Iterator<Item = T>,
    mut predicate: impl FnMut(T) -> bool,
) -> bool {
    // Either all items test `true`, or the first clause stops at `false`
    // and we check that there are no more `true` items after that.
    iter.all(&mut predicate) || !iter.any(predicate)
}

impl<T: std::fmt::Debug> Parameters<T> {
    pub fn try_new(
        names: ArgVec<Located<String>>,
        constants: BitVec,
        types: ArgVec<Located<FenlType>>,
        defaults: ArgVec<Option<Located<T>>>,
        varargs: BitVec,
    ) -> anyhow::Result<Self> {
        {
            let mut duplicate_names = names.iter().map(Located::inner).duplicates().peekable();
            anyhow::ensure!(
                duplicate_names.peek().is_none(),
                "Illegal duplicate parameter names: {}",
                duplicate_names.format_with(", ", |n, f| f(&format_args!("'{n}'")))
            );
        }

        anyhow::ensure!(
            names.len() == types.len(),
            "Names and types must have same length, but got {:?} and {:?}",
            names,
            types,
        );

        anyhow::ensure!(
            names.len() == constants.len(),
            "Names and constant bits must have same length, but was {:?} and {:?}",
            names,
            constants
        );

        anyhow::ensure!(
            names.len() == defaults.len(),
            "Names and defaults must have same length, but got {:?} and {:?}",
            names,
            types,
        );

        anyhow::ensure!(
            names.len() == varargs.len(),
            "Names and varargs must have same length, but got {:?} and {:?}",
            names,
            varargs
        );

        anyhow::ensure!(
            varargs.count_ones() <= 1,
            "Should be at most one vararg arguments, but was {}",
            varargs.count_ones()
        );

        let vararg = if let Some(has_vararg) = varargs.first_one() {
            anyhow::ensure!(
                has_vararg == names.len() - 1,
                "Vararg parameter must be the last parameter, if present"
            );

            // We might be able to handle this by just treating `0` occurrences as a
            // single occurrence of the default, but that potentially has implictions
            // that we haven't thought through. Revisit if it comes up.
            anyhow::ensure!(
                defaults[has_vararg].is_none(),
                "Vararg parameter cannot have default value"
            );

            true
        } else {
            false
        };

        anyhow::ensure!(
            is_partitioned(defaults.iter(), Option::is_none),
            "Illegal required parameter after optional parameters: {:?}",
            defaults
        );

        Ok(Self {
            names,
            constants,
            types,
            defaults,
            has_vararg: vararg,
        })
    }

    pub fn position(&self, name: &str) -> Option<usize> {
        self.names.iter().position(|n| n.inner() == name)
    }

    pub fn names(&self) -> &[Located<String>] {
        &self.names
    }

    pub fn defaults(&self) -> &[Option<Located<T>>] {
        &self.defaults
    }

    pub fn types(&self) -> &[Located<FenlType>] {
        &self.types
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    pub fn constant_indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.constants.iter_ones()
    }
}

/// An argument vector uses a `SmallVec` that stores up to 2 elements on the
/// stack.
///
/// 2 is chosen because most functions are unary or binary operations, so
/// they'll fit into the stack slots.
pub type ArgVec<T> = SmallVec<[T; 2]>;

/// Represents a single argument, which is either positional or keyword.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(serde::Serialize))]
pub enum Argument<T> {
    /// An argument passed by position -- eg., `5` in `foo(5, x = 6)`.
    Positional(Located<T>),
    /// An argument passed by keyword -- eg., `x = 6` in `foo(5, x = 6)`.
    Keyword(Located<String>, Located<T>),
}

impl<T> Argument<T> {
    fn is_keyword(&self) -> bool {
        matches!(self, Self::Keyword(_, _))
    }

    pub fn name(&self) -> Option<Located<&String>> {
        match self {
            Argument::Positional(_) => None,
            Argument::Keyword(name, _) => Some(name.as_ref()),
        }
    }

    pub fn value(&self) -> &Located<T> {
        match self {
            Argument::Positional(value) => value,
            Argument::Keyword(_, value) => value,
        }
    }

    pub fn value_mut(&mut self) -> &mut Located<T> {
        match self {
            Argument::Positional(value) => value,
            Argument::Keyword(_, value) => value,
        }
    }
}

/// The arguments to a specific call.
///
/// Before being used the arguments must be resolved against the parameters of
/// the corresponding function.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(serde::Serialize))]
#[repr(transparent)]
pub struct Arguments<T>(ArgVec<Argument<T>>);

impl<A> FromIterator<Located<A>> for Arguments<A> {
    fn from_iter<T: IntoIterator<Item = Located<A>>>(iter: T) -> Self {
        Arguments(iter.into_iter().map(Argument::Positional).collect())
    }
}

impl<T> Arguments<T> {
    pub fn empty() -> Self {
        Self(ArgVec::new())
    }

    pub fn new(arguments: ArgVec<Argument<T>>) -> Self {
        Self(arguments)
    }

    pub fn get(&self, index: usize) -> Option<&Argument<T>> {
        self.0.get(index)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Argument<T>> {
        self.0.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Argument<T>> {
        self.0.iter_mut()
    }
}

impl<T> std::ops::Index<usize> for Arguments<T> {
    type Output = Argument<T>;

    fn index(&self, index: usize) -> &Self::Output {
        self.0.index(index)
    }
}

impl<T> std::ops::Deref for Arguments<T> {
    type Target = [Argument<T>];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Errors produced by resolving.
#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize))]
pub enum ResolveError {
    Internal(String),
    PositionalAfterKeyword {
        keyword: Location,
        positional: Location,
    },
    DuplicateKeywordAndPositional {
        keyword: Location,
        positional: Location,
    },
    PositionalArgumentForOptionalParam {
        parameter: String,
        positional: Location,
    },
    TooManyArguments {
        expected: usize,
        actual: usize,
        first_unexpected: Location,
    },
    NotEnoughArguments {
        expected: usize,
        actual: usize,
    },
    InvalidKeywordArgument {
        keyword: Located<String>,
    },
}

impl<T: Clone + std::fmt::Debug> Arguments<T> {
    /// Resolve the arguments for a call against the parameters.
    ///
    /// Returns a fully-specified set of arguments matching the
    /// order in the parameters list.
    ///
    /// The intention is to mimic the behavior of Python for
    /// [default arguments](https://docs.python.org/3/tutorial/controlflow.html#default-argument-values),
    /// but with some additional complexity due to the fact that the first
    /// omitted non-default argument may be defaulted to `$input`.
    ///
    /// To make things explicit, we require that parameters with default vaules
    /// are passed as keyword arguments. The rationale being that parameters
    /// with defaults are often used to configure the behavior. Explicitly
    /// naming these helps since they may be less often passed and serves to
    /// document the configuration being changed. Additionally, it makes such
    /// calls less sensitive to the specific ordering of defaults.
    ///
    /// 1. Parameters with default values must be passed as keyword arguments.
    /// 2. Keyword arguments must appear after positional arguments.
    /// 3. Positional arguments match parameters in order.
    /// 4. It is an error for a parameter to be bound by a positional argument
    ///    and a keyword argument.
    ///
    /// # Arguments
    /// * `names` - defined strings for arguments to an expression. Wrapped in
    ///   `Cow` to allow cheap copies for statically defined strings.
    ///
    /// # Errors
    pub fn resolve(
        &self,
        names: Cow<'static, [Located<String>]>,
        defaults: &[Option<Located<T>>],
        input: &Located<T>,
        has_vararg: bool,
    ) -> Result<Resolved<Located<T>>, ResolveError> {
        if names.len() != defaults.len() {
            return Err(ResolveError::Internal(format!(
                "Names must be same length as defaults, but was {names:?} and {defaults:?}"
            )));
        }

        // Separate the positional from keyword arguments.
        let first_keyword = self
            .0
            .iter()
            .position(Argument::is_keyword)
            .unwrap_or_else(|| self.0.len());
        let (positionals, keywords) = self.0.split_at(first_keyword);

        // Put the keyword arguments into a vector so we can pull them out
        // as they are used.
        let mut keywords: Vec<_> = keywords
            .iter()
            .map(|arg| match arg {
                Argument::Positional(_) => Err(ResolveError::PositionalAfterKeyword {
                    positional: arg.value().location().clone(),
                    keyword: keywords[0].name().unwrap().location().clone(),
                }),
                Argument::Keyword(name, value) => Ok((name, value)),
            })
            .try_collect()?;

        let mut positionals = positionals.iter().peekable();

        // This approach to resolving (iterating over the names of the parameters) was
        // originally used to make it really easy to verify that each argument in the
        // signature is resolved once. It still has some benefits, but it is
        // somewhat unwieldy to use for the new logic. It may be easier to use a
        // strategy driven by the arguments. For instance:
        //
        // 1. ensure the number of positional arguments is less than or equal to the
        //    number of required arguments,
        // 2. ensure that the keyword arguments don't include any names provided by the
        //    positional arguments
        // 3. iterate over the parameters not bound by positional arguments
        //
        // We didn't make this change yet to keep the logical changes as minimal as
        // possible. This may be done in the future if it simplifies this code
        // and we're working on it.

        // Whether we've inferred `$input` for a required parameter.
        let mut implicit_input_available = !has_vararg;
        let mut values: ArgVec<Located<T>> = names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                if let Some(kw_pos) = keywords.iter().position(|(kw, _)| (*kw) == name) {
                    // We want match positional arguments first. But we also want to
                    // report an error if that positional appears as a keyword. So we
                    // first see if we can find a keyword argument. If we do, we check
                    // to make sure that we *don't* have any more positionals left
                    // (since that would mean we have a duplicate).

                    let keyword = keywords.swap_remove(kw_pos);
                    if let Some(positional) = positionals.peek() {
                        Err(ResolveError::DuplicateKeywordAndPositional {
                            keyword: keyword.0.location().clone(),
                            positional: positional.value().location().clone(),
                        })
                    } else {
                        // If we have a matching keyword, take its value.
                        // This also removes it from the keywords so we can check that all
                        // arguments are used.
                        Ok(keyword.1.clone())
                    }
                } else if let Some(default) = defaults[index].as_ref() {
                    if let Some(positional) = positionals.peek() {
                        Err(ResolveError::PositionalArgumentForOptionalParam {
                            parameter: name.inner().clone(),
                            positional: positional.value().location().clone(),
                        })
                    } else {
                        Ok(default.clone())
                    }
                } else if let Some(positional) = positionals.next() {
                    Ok(positional.value().clone())
                } else if let Some(default) = defaults[index].as_ref() {
                    Ok(default.clone())
                } else if implicit_input_available {
                    implicit_input_available = false;
                    Ok(input.clone())
                } else {
                    Err(ResolveError::NotEnoughArguments {
                        expected: names.len(),
                        actual: self.0.len(),
                    })
                }
            })
            .try_collect()?;

        // After pairing things up, we need to make sure we used all
        // the arguments.
        if let Some((name, _)) = keywords.first() {
            // TODO: Report *all* unused keywords instead of just the first.
            Err(ResolveError::InvalidKeywordArgument {
                keyword: (*name).clone(),
            })
        } else if has_vararg {
            // If this is a vararg function, any remaining positionals can be
            // assigned to the vararg.
            values.extend(positionals.map(|arg| arg.value().clone()));

            Ok(Resolved::new(names, values, true))
        } else if let Some(first_extra) = positionals.next() {
            Err(ResolveError::TooManyArguments {
                expected: names.len(),
                actual: self.0.len(),
                first_unexpected: first_extra.value().location().clone(),
            })
        } else {
            Ok(Resolved::new(names, values, false))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize))]
/// Represents arguments that have been resolved against a given signature.
pub struct Resolved<T> {
    /// The names of the resolved parameters.
    //
    /// This is most useful for debugging and diagnostic reporting.
    /// Wrapped in `Cow` to allow cheap copies for statically defined strings.
    names: Cow<'static, [Located<String>]>,
    /// The values of each resolved parameter.
    values: ArgVec<T>,
    /// If true, the function had varargs.
    ///
    /// The last name may be used 1 or more times.
    pub has_vararg: bool,
}

impl<T> Default for Resolved<T> {
    fn default() -> Self {
        Self {
            names: Default::default(),
            values: Default::default(),
            has_vararg: Default::default(),
        }
    }
}

impl<T: std::fmt::Debug> Resolved<T> {
    pub fn empty() -> Self {
        Self {
            names: Cow::Borrowed(&[]),
            values: ArgVec::new(),
            has_vararg: false,
        }
    }

    pub fn new(
        names: Cow<'static, [Located<String>]>,
        values: ArgVec<T>,
        has_vararg: bool,
    ) -> Self {
        debug_assert!(
            (names.len() == values.len()) || (has_vararg && names.len() < values.len()),
            "Mismatched names ({names:?}) and values ({values:?}) (vararg = {has_vararg})"
        );

        Self {
            names,
            values,
            has_vararg,
        }
    }

    pub fn with_values<T2>(&self, values: ArgVec<T2>) -> Resolved<T2> {
        debug_assert_eq!(self.len(), values.len());
        Resolved {
            names: self.names.clone(),
            values,
            has_vararg: self.has_vararg,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Get the names of the parameters as a slice.
    ///
    /// Note: The length of the names may differ from the length of values
    /// when used with varargs.
    pub fn names(&self) -> &[Located<String>] {
        self.names.as_ref()
    }

    /// Get the values as a slice.
    pub fn values(&self) -> &[T] {
        &self.values
    }

    /// Get the values as a mutable slice.
    pub fn values_mut(&mut self) -> &mut [T] {
        &mut self.values
    }

    pub fn take_values(self) -> ArgVec<T> {
        self.values
    }

    /// Get the argument with the given name.
    pub fn get(&self, name: &str) -> Option<&T> {
        // TODO: Vararg?
        if let Some(index) = self.names.iter().position(|n| n.inner() == name) {
            Some(&self.values[index])
        } else {
            None
        }
    }

    /// Apply a function to transform each argument.
    pub fn transform<T2>(&self, f: impl FnMut(&T) -> T2) -> Resolved<T2> {
        let values = self.values.iter().map(f).collect();
        Resolved {
            names: self.names.clone(),
            values,
            has_vararg: self.has_vararg,
        }
    }

    /// Apply a fallible function to transform each argument.
    pub fn try_transform<E, T2>(
        &self,
        f: impl FnMut(&T) -> Result<T2, E>,
    ) -> Result<Resolved<T2>, E> {
        let Self {
            names,
            values,
            has_vararg: vararg,
        } = self;
        values.iter().map(f).try_collect().map(|values| Resolved {
            names: names.clone(),
            values,
            has_vararg: *vararg,
        })
    }

    pub fn parameter_name(&self, index: usize) -> &Located<String> {
        if self.has_vararg && index >= self.names.len() {
            &self.names[self.names.len() - 1]
        } else {
            &self.names[index]
        }
    }

    /// Return an iterator over pairs of argument name and resolved value.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.values.iter()
    }

    /// Create a vector of values matching the length of the resolved arguments.
    ///
    /// If this resolved call includes varargs, the last value will be copied
    /// as needed to match the length of the actual arguments.
    pub fn vec_for_varargs<T2: Clone>(&self, values: &[T2]) -> Vec<T2> {
        assert_eq!(
            values.len(),
            self.names.len(),
            "The length of the values should match the length of the names."
        );

        let mut values = values.to_vec();
        if self.values.len() > values.len() {
            let last_value = values[values.len() - 1].clone();
            values.resize(self.values.len(), last_value);
        }
        values
    }
}

impl<T> IntoIterator for Resolved<T> {
    type Item = T;

    #[allow(clippy::type_complexity)]
    type IntoIter = smallvec::IntoIter<[T; 2]>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl<T> Index<usize> for Resolved<T>
where
    T: std::fmt::Debug,
{
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

impl<T> Index<&str> for Resolved<T>
where
    T: std::fmt::Debug,
{
    type Output = T;

    fn index(&self, index: &str) -> &Self::Output {
        self.get(index)
            .unwrap_or_else(|| panic!("No argument named '{index}' in {self:?}"))
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use super::*;
    use crate::{Expr, FeatureSetPart, Signature};

    /// Helper to run the test and produce the result.
    ///
    /// On success, returns the expressions bound to each argument, in the order
    /// of the function parameters.
    ///
    /// Note: This removes the outermost level of information (names and
    /// argument locations) in an attempt to make the test expectations
    /// easier to read. It does not remove all locations though, since
    /// expressiion contain locations within references.
    fn test_resolve_exprs(
        signature: &'static str,
        args: &'static str,
    ) -> Result<Vec<Arc<Expr>>, ResolveError> {
        let signature =
            Signature::try_from_str(FeatureSetPart::Internal(signature), signature).unwrap();
        let params = signature.parameters;

        let args = crate::try_parse_arguments(args).unwrap();
        args.resolve(
            Cow::Owned(params.names().to_owned()),
            params.defaults(),
            &Located::internal_str("$input").with_value(Expr::implicit_input().clone()),
            params.has_vararg,
        )
        .map(|resolved| resolved.into_iter().map(|expr| expr.into_inner()).collect())
    }

    const POW_SIGNATURE: &str = "pow<N: number>(power: N, base: N) -> N";
    const CLAMP_SIGNATURE: &str = "clamp<N: number>(value: N, min: N = null, max: N = null) -> N";
    const LOG_SIGNATURE: &str = "log(power: f64, base: f64 = e) -> f64";
    const SUBSTRING_SIGNATURE: &str =
        "substring(str: string, start: i32 = null, end: i32 = null) -> string";
    const COALESCE_SIGNATURE: &str = "coalesce<T: any>(values+: T) -> T";

    #[test]
    fn resolve_pow_positional_explicit_input() {
        // pow(two, $input)
        insta::assert_ron_snapshot!(test_resolve_exprs(POW_SIGNATURE, "two, $input"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "two",
              location: Location(
                part: Internal("two, $input"),
                start: 0,
                end: 3,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Internal("two, $input"),
                start: 5,
                end: 11,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_pow_keyword_explicit_input() {
        insta::assert_ron_snapshot!(test_resolve_exprs(POW_SIGNATURE, "base = two, power = $input"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Internal("base = two, power = $input"),
                start: 20,
                end: 26,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "two",
              location: Location(
                part: Internal("base = two, power = $input"),
                start: 7,
                end: 10,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_pow_positional_implicit_input() {
        insta::assert_ron_snapshot!(test_resolve_exprs(POW_SIGNATURE, "two"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "two",
              location: Location(
                part: Internal("two"),
                start: 0,
                end: 3,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Input,
                start: 0,
                end: 6,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_pow_keyword_implicit_input() {
        insta::assert_ron_snapshot!(test_resolve_exprs(POW_SIGNATURE, "base = two"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Input,
                start: 0,
                end: 6,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "two",
              location: Location(
                part: Internal("base = two"),
                start: 7,
                end: 10,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_pow_explicit_base_and_implicit_input() {
        insta::assert_ron_snapshot!(test_resolve_exprs(POW_SIGNATURE, "base = $input"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Input,
                start: 0,
                end: 6,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Internal("base = $input"),
                start: 7,
                end: 13,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }
    #[test]
    fn resolve_pow_explicit_power_and_implicit_input() {
        insta::assert_ron_snapshot!(test_resolve_exprs(POW_SIGNATURE, "power = $input"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Internal("power = $input"),
                start: 8,
                end: 14,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Input,
                start: 0,
                end: 6,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }
    #[test]
    fn resolve_pow_positional_input_and_implicit_input() {
        insta::assert_ron_snapshot!(test_resolve_exprs(POW_SIGNATURE, "$input"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Internal("$input"),
                start: 0,
                end: 6,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Input,
                start: 0,
                end: 6,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_clamp_positional() {
        // clamp(min = zero, max = ten)
        insta::assert_ron_snapshot!(test_resolve_exprs(CLAMP_SIGNATURE, "min = zero, max = ten"),
            @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Input,
                start: 0,
                end: 6,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "zero",
              location: Location(
                part: Internal("min = zero, max = ten"),
                start: 6,
                end: 10,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "ten",
              location: Location(
                part: Internal("min = zero, max = ten"),
                start: 18,
                end: 21,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_clamp_keyword_min() {
        // clamp(min = zero)
        insta::assert_ron_snapshot!(test_resolve_exprs(CLAMP_SIGNATURE, "min = zero"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Input,
                start: 0,
                end: 6,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "zero",
              location: Location(
                part: Internal("min = zero"),
                start: 6,
                end: 10,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Literal(Located(
              value: Null,
              location: Location(
                part: Internal("clamp<N: number>(value: N, min: N = null, max: N = null) -> N"),
                start: 51,
                end: 55,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_clamp_keyword_max() {
        // clamp(max = ten)
        insta::assert_ron_snapshot!(test_resolve_exprs(CLAMP_SIGNATURE, "max = ten"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Input,
                start: 0,
                end: 6,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Literal(Located(
              value: Null,
              location: Location(
                part: Internal("clamp<N: number>(value: N, min: N = null, max: N = null) -> N"),
                start: 36,
                end: 40,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "ten",
              location: Location(
                part: Internal("max = ten"),
                start: 6,
                end: 9,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_clamp_positional_and_min() {
        // clamp(fortytwo, min = zero)
        insta::assert_ron_snapshot!(test_resolve_exprs(CLAMP_SIGNATURE, "fortytwo, min = zero"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "fortytwo",
              location: Location(
                part: Internal("fortytwo, min = zero"),
                start: 0,
                end: 8,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "zero",
              location: Location(
                part: Internal("fortytwo, min = zero"),
                start: 16,
                end: 20,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Literal(Located(
              value: Null,
              location: Location(
                part: Internal("clamp<N: number>(value: N, min: N = null, max: N = null) -> N"),
                start: 51,
                end: 55,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_clamp_positional_and_max() {
        // clamp(fortytwo, max = ten)
        insta::assert_ron_snapshot!(test_resolve_exprs(CLAMP_SIGNATURE, "fortytwo, max = ten"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "fortytwo",
              location: Location(
                part: Internal("fortytwo, max = ten"),
                start: 0,
                end: 8,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Literal(Located(
              value: Null,
              location: Location(
                part: Internal("clamp<N: number>(value: N, min: N = null, max: N = null) -> N"),
                start: 36,
                end: 40,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "ten",
              location: Location(
                part: Internal("fortytwo, max = ten"),
                start: 16,
                end: 19,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_log_keyword_base() {
        // log(base = 2.0)
        insta::assert_ron_snapshot!(test_resolve_exprs(LOG_SIGNATURE, "base = 2.0"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Input,
                start: 0,
                end: 6,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Literal(Located(
              value: Number("2.0"),
              location: Location(
                part: Internal("base = 2.0"),
                start: 7,
                end: 10,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_log_positional() {
        // log(2.0)
        insta::assert_ron_snapshot!(test_resolve_exprs(LOG_SIGNATURE, "2.0"), @r###"
        Ok([
          Expr(
            op: Literal(Located(
              value: Number("2.0"),
              location: Location(
                part: Internal("2.0"),
                start: 0,
                end: 3,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "e",
              location: Location(
                part: Internal("log(power: f64, base: f64 = e) -> f64"),
                start: 28,
                end: 29,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_log_power() {
        // log(power = $input)
        insta::assert_ron_snapshot!(test_resolve_exprs(LOG_SIGNATURE, "power = $input"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Internal("power = $input"),
                start: 8,
                end: 14,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "e",
              location: Location(
                part: Internal("log(power: f64, base: f64 = e) -> f64"),
                start: 28,
                end: 29,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_log_positional_input() {
        // log($input)
        insta::assert_ron_snapshot!(test_resolve_exprs(LOG_SIGNATURE, "power = $input"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Internal("power = $input"),
                start: 8,
                end: 14,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "e",
              location: Location(
                part: Internal("log(power: f64, base: f64 = e) -> f64"),
                start: 28,
                end: 29,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_log_no_explicit_args() {
        // log()
        insta::assert_ron_snapshot!(test_resolve_exprs(LOG_SIGNATURE, ""), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Input,
                start: 0,
                end: 6,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "e",
              location: Location(
                part: Internal("log(power: f64, base: f64 = e) -> f64"),
                start: 28,
                end: 29,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_substring_start_end() {
        // substring(start = zero, end = ten)
        insta::assert_ron_snapshot!(test_resolve_exprs(SUBSTRING_SIGNATURE, "start = zero, end = ten"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Input,
                start: 0,
                end: 6,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "zero",
              location: Location(
                part: Internal("start = zero, end = ten"),
                start: 8,
                end: 12,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "ten",
              location: Location(
                part: Internal("start = zero, end = ten"),
                start: 20,
                end: 23,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_substring_invalid_config() {
        // substring(zero, ten)
        insta::assert_ron_snapshot!(test_resolve_exprs(SUBSTRING_SIGNATURE, "zero, ten"), @r###"
        Err(PositionalArgumentForOptionalParam(
          parameter: "start",
          positional: Location(
            part: Internal("zero, ten"),
            start: 6,
            end: 9,
          ),
        ))
        "###);
    }

    #[test]
    fn resolve_substring_end_start() {
        // substring(end = ten, start = three)
        insta::assert_ron_snapshot!(
            test_resolve_exprs(SUBSTRING_SIGNATURE, "end = ten, start = three"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Input,
                start: 0,
                end: 6,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "three",
              location: Location(
                part: Internal("end = ten, start = three"),
                start: 19,
                end: 24,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "ten",
              location: Location(
                part: Internal("end = ten, start = three"),
                start: 6,
                end: 9,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_substring_start() {
        // substring(start = five)
        insta::assert_ron_snapshot!(test_resolve_exprs(SUBSTRING_SIGNATURE, "start = five"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Input,
                start: 0,
                end: 6,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "five",
              location: Location(
                part: Internal("start = five"),
                start: 8,
                end: 12,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Literal(Located(
              value: Null,
              location: Location(
                part: Internal("substring(str: string, start: i32 = null, end: i32 = null) -> string"),
                start: 53,
                end: 57,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_substring_end() {
        // substring(end = five)
        insta::assert_ron_snapshot!(test_resolve_exprs(SUBSTRING_SIGNATURE, "end = five"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "$input",
              location: Location(
                part: Input,
                start: 0,
                end: 6,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Literal(Located(
              value: Null,
              location: Location(
                part: Internal("substring(str: string, start: i32 = null, end: i32 = null) -> string"),
                start: 36,
                end: 40,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "five",
              location: Location(
                part: Internal("end = five"),
                start: 6,
                end: 10,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_coalesce_zero_args() {
        // coalesce()
        insta::assert_ron_snapshot!(test_resolve_exprs(COALESCE_SIGNATURE, ""), @r###"
        Err(NotEnoughArguments(
          expected: 1,
          actual: 0,
        ))
        "###);
    }

    #[test]
    fn resolve_coalesce_one_args() {
        // coalesce(a)
        insta::assert_ron_snapshot!(test_resolve_exprs(COALESCE_SIGNATURE, "a"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "a",
              location: Location(
                part: Internal("a"),
                start: 0,
                end: 1,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_coalesce_two_args() {
        // coalesce(a, b)
        insta::assert_ron_snapshot!(test_resolve_exprs(COALESCE_SIGNATURE, "a, b"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "a",
              location: Location(
                part: Internal("a, b"),
                start: 0,
                end: 1,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "b",
              location: Location(
                part: Internal("a, b"),
                start: 3,
                end: 4,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn resolve_coalesce_three_args() {
        // coalesce(a, b, c)
        insta::assert_ron_snapshot!(test_resolve_exprs(COALESCE_SIGNATURE, "a, b, c"), @r###"
        Ok([
          Expr(
            op: Reference(Located(
              value: "a",
              location: Location(
                part: Internal("a, b, c"),
                start: 0,
                end: 1,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "b",
              location: Location(
                part: Internal("a, b, c"),
                start: 3,
                end: 4,
              ),
            )),
            args: Arguments([]),
          ),
          Expr(
            op: Reference(Located(
              value: "c",
              location: Location(
                part: Internal("a, b, c"),
                start: 6,
                end: 7,
              ),
            )),
            args: Arguments([]),
          ),
        ])
        "###);
    }

    #[test]
    fn should_report_positional_after_keyword() {
        // fun(a = zero, 5)
        insta::assert_ron_snapshot!(
          test_resolve_exprs("fun<N: number>(a: N, b: string) -> string", "a = zero, 5"), @r###"
        Err(PositionalAfterKeyword(
          keyword: Location(
            part: Internal("a = zero, 5"),
            start: 0,
            end: 1,
          ),
          positional: Location(
            part: Internal("a = zero, 5"),
            start: 10,
            end: 11,
          ),
        ))
        "###);
    }

    #[test]
    fn should_report_invalid_named_argument() {
        // fun(c = zero, 5)
        insta::assert_ron_snapshot!(
          test_resolve_exprs("fun<N: number>(a: N, b: string) -> string", "c = zero, b = 5"), @r###"
        Err(InvalidKeywordArgument(
          keyword: Located(
            value: "c",
            location: Location(
              part: Internal("c = zero, b = 5"),
              start: 0,
              end: 1,
            ),
          ),
        ))
        "###);
    }

    #[test]
    fn should_report_too_many_arguments() {
        // fun(1, 2, 3)
        insta::assert_ron_snapshot!(
          test_resolve_exprs("fun<N: number>(a: N, b: string) -> N", "1, 2, 3"), @r###"
        Err(TooManyArguments(
          expected: 2,
          actual: 3,
          first_unexpected: Location(
            part: Internal("1, 2, 3"),
            start: 6,
            end: 7,
          ),
        ))
        "###);
    }

    #[test]
    fn should_report_too_many_missing_arguments() {
        // fun(1)
        insta::assert_ron_snapshot!(
          test_resolve_exprs("fun<N: number>(a: N, b: string, c: N) -> N", "1"), @r###"
        Err(NotEnoughArguments(
          expected: 3,
          actual: 1,
        ))
        "###);
    }

    #[test]
    fn should_report_duplicate_keyword_arguments() {
        // fun(a = 1, a = 3)
        insta::assert_ron_snapshot!(
          test_resolve_exprs("fun<N: number>(a: N, b: string) -> string", "a = 1, a = 3"), @r###"
        Err(InvalidKeywordArgument(
          keyword: Located(
            value: "a",
            location: Location(
              part: Internal("a = 1, a = 3"),
              start: 7,
              end: 8,
            ),
          ),
        ))
        "###);
    }

    #[test]
    fn should_report_duplicate_keyword_positional_arguments() {
        // fun(1, a = 2)
        insta::assert_ron_snapshot!(
          test_resolve_exprs("fun<N: number>(a: N, b: string) -> string", "1, a = 2"), @r###"
        Err(DuplicateKeywordAndPositional(
          keyword: Location(
            part: Internal("1, a = 2"),
            start: 3,
            end: 4,
          ),
          positional: Location(
            part: Internal("1, a = 2"),
            start: 0,
            end: 1,
          ),
        ))
        "###);
    }
}
