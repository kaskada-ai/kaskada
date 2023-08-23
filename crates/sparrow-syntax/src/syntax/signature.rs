use hashbrown::HashSet;
use itertools::Itertools;

use crate::parser::try_parse_signature;
use crate::{ExprRef, FeatureSetPart, FenlType, Located, Parameters, ParseErrors, TypeClass};

/// Type variable defined by a signature
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct TypeVariable(pub String);

impl From<&TypeVariable> for TypeVariable {
    fn from(value: &TypeVariable) -> Self {
        value.clone()
    }
}

impl std::fmt::Display for TypeVariable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Contains the type variable and the classes for
/// that variable.
///
/// e.g. N(type variable): Eq(type_class) + Hash(type_class)
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct TypeParameter {
    pub name: TypeVariable,
    pub type_classes: Vec<TypeClass>,
}

impl std::fmt::Display for TypeParameter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Note: this excludes the type classes
        write!(f, "{}", self.name.0)
    }
}

impl TypeParameter {
    pub fn new(name: String, type_classes: Vec<TypeClass>) -> Self {
        Self {
            name: TypeVariable(name),
            type_classes,
        }
    }
}

/// The signature of an operator or function.
#[derive(Debug, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct Signature {
    /// The name of the operator or function.
    ///
    /// For operators, this should be a searchable (unique) string describing
    /// the operator. For functions, this should be the name of the
    /// function.
    name: String,
    /// The parameters to the function.
    pub(super) parameters: Parameters<ExprRef>,
    /// The type parameters associated with this signature.
    pub type_parameters: Vec<TypeParameter>,
    /// The result of the function.
    result: FenlType,
}

fn check_signature(
    name: &str,
    parameters: &Parameters<ExprRef>,
    type_parameters: &[TypeParameter],
    result: &FenlType,
) -> anyhow::Result<()> {
    fn visit_type<'a>(type_vars: &mut HashSet<&'a TypeVariable>, ty: &'a FenlType) {
        match ty {
            FenlType::TypeRef(type_var) => {
                type_vars.insert(type_var);
            }
            FenlType::Collection(_, coll_types) => {
                for type_var in coll_types {
                    visit_type(type_vars, type_var);
                }
            }
            _ => {}
        }
    }

    // collect all the type variables and verify they are defined
    let mut type_vars = HashSet::new();
    let defined: HashSet<_> = type_parameters.iter().map(|p| &p.name).collect();

    for t in parameters.types() {
        visit_type(&mut type_vars, t.inner());
    }

    // check that all type variables defined are used in the parameters.
    // we do this before adding the result since we can't infer the type
    // for a type variable used only in the return.
    for tp in type_parameters {
        anyhow::ensure!(
            type_vars.contains(&&tp.name),
            "Type variable '{:?}' is defined in the type parameters for signature '{}',
          but is not used in the parameters",
            tp.name,
            name
        );
    }

    visit_type(&mut type_vars, result);

    // check that all referenced type variables are defined
    for type_var in type_vars {
        anyhow::ensure!(
            defined.contains(&type_var),
            "Type variable '{:?}' is not defined in the type parameters for signature: '{:?}'",
            type_var,
            name
        );
    }

    // check that no duplicates exist in the type_parameters
    let duplicates: Vec<_> = type_parameters
        .iter()
        .map(|p| &p.name.0)
        .duplicates()
        .collect();
    if !duplicates.is_empty() {
        anyhow::bail!(
            "Duplicate type parameters: {} in signature for '{name}'",
            duplicates
                .iter()
                .format_with(",", |elt, f| f(&format_args!("'{elt}'")))
        );
    };

    Ok(())
}

impl Signature {
    pub(crate) fn try_new(
        name: String,
        parameters: Parameters<ExprRef>,
        type_parameters: Vec<TypeParameter>,
        result: FenlType,
    ) -> anyhow::Result<Self> {
        check_signature(&name, &parameters, &type_parameters, &result)?;
        Ok(Self {
            name,
            parameters,
            type_parameters,
            result,
        })
    }

    pub fn try_from_str(part_id: FeatureSetPart, input: &str) -> Result<Self, ParseErrors<'_>> {
        try_parse_signature(part_id, input)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn parameters(&self) -> &Parameters<ExprRef> {
        &self.parameters
    }

    pub fn result(&self) -> &FenlType {
        &self.result
    }

    pub fn arg_names(&self) -> &[Located<String>] {
        self.parameters().names()
    }

    pub fn assert_valid_argument_count(&self, num_args: usize) {
        if self.parameters.has_vararg {
            assert!(
                num_args >= self.parameters.names().len(),
                "Expected vararg operator '{:?}' to have at least {:?} arguments, but was {:?}",
                self.name,
                self.parameters.names().len(),
                num_args,
            )
        } else {
            assert!(
                num_args == self.parameters.names().len(),
                "Expected operator '{:?}' to have exactly {:?} arguments, but was {:?} ({self:?})",
                self.name,
                self.parameters.names().len(),
                num_args,
            )
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn test_parse_signature(input: &'static str) -> Result<Signature, String> {
        Signature::try_from_str(FeatureSetPart::Internal(input), input).map_err(|errors| {
            format!(
                "{}",
                errors
                    .iter()
                    .format_with(", ", |elt, f| f(&format_args!("{elt:?}")))
            )
        })
    }

    #[test]
    fn test_type_params() {
        insta::assert_ron_snapshot!(
            test_parse_signature("test<N: number, O: ordered>(input: N, input2: O) -> O"),
            @r###"
        Ok(Signature(
          name: "test",
          parameters: Parameters(
            names: [
              Located(
                value: "input",
                location: Location(
                  part: Internal("test<N: number, O: ordered>(input: N, input2: O) -> O"),
                  start: 28,
                  end: 33,
                ),
              ),
              Located(
                value: "input2",
                location: Location(
                  part: Internal("test<N: number, O: ordered>(input: N, input2: O) -> O"),
                  start: 38,
                  end: 44,
                ),
              ),
            ],
            constants: BitSeq(
              order: "bitvec::order::Lsb0",
              head: BitIdx(
                width: 64,
                index: 0,
              ),
              bits: 2,
              data: [
                0,
              ],
            ),
            types: [
              Located(
                value: TypeRef(TypeVariable("N")),
                location: Location(
                  part: Internal("test<N: number, O: ordered>(input: N, input2: O) -> O"),
                  start: 35,
                  end: 36,
                ),
              ),
              Located(
                value: TypeRef(TypeVariable("O")),
                location: Location(
                  part: Internal("test<N: number, O: ordered>(input: N, input2: O) -> O"),
                  start: 46,
                  end: 47,
                ),
              ),
            ],
            defaults: [
              None,
              None,
            ],
            has_vararg: false,
          ),
          type_parameters: [
            TypeParameter(
              name: TypeVariable("N"),
              type_classes: [
                Number,
              ],
            ),
            TypeParameter(
              name: TypeVariable("O"),
              type_classes: [
                Ordered,
              ],
            ),
          ],
          result: TypeRef(TypeVariable("O")),
        ))
        "###);
    }

    #[test]
    fn test_parse_unary() {
        insta::assert_ron_snapshot!(
            test_parse_signature("not(input: bool) -> bool"),
            @r###"
        Ok(Signature(
          name: "not",
          parameters: Parameters(
            names: [
              Located(
                value: "input",
                location: Location(
                  part: Internal("not(input: bool) -> bool"),
                  start: 4,
                  end: 9,
                ),
              ),
            ],
            constants: BitSeq(
              order: "bitvec::order::Lsb0",
              head: BitIdx(
                width: 64,
                index: 0,
              ),
              bits: 1,
              data: [
                0,
              ],
            ),
            types: [
              Located(
                value: Concrete(Boolean),
                location: Location(
                  part: Internal("not(input: bool) -> bool"),
                  start: 11,
                  end: 15,
                ),
              ),
            ],
            defaults: [
              None,
            ],
            has_vararg: false,
          ),
          type_parameters: [],
          result: Concrete(Boolean),
        ))
        "###);
    }

    #[test]
    fn test_parse_binary() {
        insta::assert_ron_snapshot!(
            test_parse_signature(
                "add<N: number>(lhs: N, rhs: N) -> N"
            ),
            @r###"
        Ok(Signature(
          name: "add",
          parameters: Parameters(
            names: [
              Located(
                value: "lhs",
                location: Location(
                  part: Internal("add<N: number>(lhs: N, rhs: N) -> N"),
                  start: 15,
                  end: 18,
                ),
              ),
              Located(
                value: "rhs",
                location: Location(
                  part: Internal("add<N: number>(lhs: N, rhs: N) -> N"),
                  start: 23,
                  end: 26,
                ),
              ),
            ],
            constants: BitSeq(
              order: "bitvec::order::Lsb0",
              head: BitIdx(
                width: 64,
                index: 0,
              ),
              bits: 2,
              data: [
                0,
              ],
            ),
            types: [
              Located(
                value: TypeRef(TypeVariable("N")),
                location: Location(
                  part: Internal("add<N: number>(lhs: N, rhs: N) -> N"),
                  start: 20,
                  end: 21,
                ),
              ),
              Located(
                value: TypeRef(TypeVariable("N")),
                location: Location(
                  part: Internal("add<N: number>(lhs: N, rhs: N) -> N"),
                  start: 28,
                  end: 29,
                ),
              ),
            ],
            defaults: [
              None,
              None,
            ],
            has_vararg: false,
          ),
          type_parameters: [
            TypeParameter(
              name: TypeVariable("N"),
              type_classes: [
                Number,
              ],
            ),
          ],
          result: TypeRef(TypeVariable("N")),
        ))
        "###);
    }

    #[test]
    fn test_parse_signature_with_const() {
        insta::assert_ron_snapshot!(
            test_parse_signature(
                "lag<O: ordered>(e: O, const n: i64) -> O"
            ),
            @r###"
        Ok(Signature(
          name: "lag",
          parameters: Parameters(
            names: [
              Located(
                value: "e",
                location: Location(
                  part: Internal("lag<O: ordered>(e: O, const n: i64) -> O"),
                  start: 16,
                  end: 17,
                ),
              ),
              Located(
                value: "n",
                location: Location(
                  part: Internal("lag<O: ordered>(e: O, const n: i64) -> O"),
                  start: 28,
                  end: 29,
                ),
              ),
            ],
            constants: BitSeq(
              order: "bitvec::order::Lsb0",
              head: BitIdx(
                width: 64,
                index: 0,
              ),
              bits: 2,
              data: [
                2,
              ],
            ),
            types: [
              Located(
                value: TypeRef(TypeVariable("O")),
                location: Location(
                  part: Internal("lag<O: ordered>(e: O, const n: i64) -> O"),
                  start: 19,
                  end: 20,
                ),
              ),
              Located(
                value: Concrete(Int64),
                location: Location(
                  part: Internal("lag<O: ordered>(e: O, const n: i64) -> O"),
                  start: 31,
                  end: 34,
                ),
              ),
            ],
            defaults: [
              None,
              None,
            ],
            has_vararg: false,
          ),
          type_parameters: [
            TypeParameter(
              name: TypeVariable("O"),
              type_classes: [
                Ordered,
              ],
            ),
          ],
          result: TypeRef(TypeVariable("O")),
        ))
        "###);
    }

    #[test]
    fn test_parse_signature_with_vararg() {
        insta::assert_ron_snapshot!(
            test_parse_signature("coalesce<T: any>(values+: T) -> T"),
            @r###"
        Ok(Signature(
          name: "coalesce",
          parameters: Parameters(
            names: [
              Located(
                value: "values",
                location: Location(
                  part: Internal("coalesce<T: any>(values+: T) -> T"),
                  start: 17,
                  end: 23,
                ),
              ),
            ],
            constants: BitSeq(
              order: "bitvec::order::Lsb0",
              head: BitIdx(
                width: 64,
                index: 0,
              ),
              bits: 1,
              data: [
                0,
              ],
            ),
            types: [
              Located(
                value: TypeRef(TypeVariable("T")),
                location: Location(
                  part: Internal("coalesce<T: any>(values+: T) -> T"),
                  start: 26,
                  end: 27,
                ),
              ),
            ],
            defaults: [
              None,
            ],
            has_vararg: true,
          ),
          type_parameters: [
            TypeParameter(
              name: TypeVariable("T"),
              type_classes: [
                Any,
              ],
            ),
          ],
          result: TypeRef(TypeVariable("T")),
        ))
        "###);
    }

    #[test]
    fn test_reject_number_on_rhs() {
        insta::assert_ron_snapshot!(
        test_parse_signature("add(lhs: f64, rhs: f64) -> number"),
        @r###"Err("User { error: (4, \"Type variable \'TypeVariable(\\\"number\\\")\' is not defined in the type parameters for signature: \'\\\"add\\\"\'\", 22) }")"###);
    }
}
