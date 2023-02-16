use itertools::Itertools;

use crate::parser::try_parse_signature;
use crate::{ExprRef, FeatureSetPart, FenlType, Located, Parameters, ParseErrors};

/// The signature of an operator or function.
#[derive(Debug, PartialEq, Eq)]
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
    /// The result of the function.
    result: FenlType,
}

impl Signature {
    pub(crate) fn try_new(
        name: String,
        parameters: Parameters<ExprRef>,
        result: FenlType,
    ) -> anyhow::Result<Self> {
        if matches!(result, FenlType::Generic(_)) {
            anyhow::ensure!(
                parameters
                    .types()
                    .iter()
                    .map(Located::inner)
                    .contains(&result),
                "Illegal signature for '{}': {} must appear in the parameters to be used in the \
                 result",
                name,
                result
            )
        }

        Ok(Self {
            name,
            parameters,
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
                "Expected operator '{:?}' to have exactly {:?} arguments, but was {:?}",
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
          result: Concrete(Boolean),
        ))
        "###);
    }

    #[test]
    fn test_parse_binary() {
        insta::assert_ron_snapshot!(
            test_parse_signature(
                "add(lhs: number, rhs: number) -> number"
            ),
            @r###"
        Ok(Signature(
          name: "add",
          parameters: Parameters(
            names: [
              Located(
                value: "lhs",
                location: Location(
                  part: Internal("add(lhs: number, rhs: number) -> number"),
                  start: 4,
                  end: 7,
                ),
              ),
              Located(
                value: "rhs",
                location: Location(
                  part: Internal("add(lhs: number, rhs: number) -> number"),
                  start: 17,
                  end: 20,
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
                value: Generic(Number),
                location: Location(
                  part: Internal("add(lhs: number, rhs: number) -> number"),
                  start: 9,
                  end: 15,
                ),
              ),
              Located(
                value: Generic(Number),
                location: Location(
                  part: Internal("add(lhs: number, rhs: number) -> number"),
                  start: 22,
                  end: 28,
                ),
              ),
            ],
            defaults: [
              None,
              None,
            ],
            has_vararg: false,
          ),
          result: Generic(Number),
        ))
        "###);
    }

    #[test]
    fn test_parse_signature_with_const() {
        insta::assert_ron_snapshot!(
            test_parse_signature(
                "lag(e: ordered, const n: i64) -> ordered"
            ),
            @r###"
        Ok(Signature(
          name: "lag",
          parameters: Parameters(
            names: [
              Located(
                value: "e",
                location: Location(
                  part: Internal("lag(e: ordered, const n: i64) -> ordered"),
                  start: 4,
                  end: 5,
                ),
              ),
              Located(
                value: "n",
                location: Location(
                  part: Internal("lag(e: ordered, const n: i64) -> ordered"),
                  start: 22,
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
              bits: 2,
              data: [
                2,
              ],
            ),
            types: [
              Located(
                value: Generic(Ordered),
                location: Location(
                  part: Internal("lag(e: ordered, const n: i64) -> ordered"),
                  start: 7,
                  end: 14,
                ),
              ),
              Located(
                value: Concrete(Int64),
                location: Location(
                  part: Internal("lag(e: ordered, const n: i64) -> ordered"),
                  start: 25,
                  end: 28,
                ),
              ),
            ],
            defaults: [
              None,
              None,
            ],
            has_vararg: false,
          ),
          result: Generic(Ordered),
        ))
        "###);
    }

    #[test]
    fn test_parse_signature_with_vararg() {
        insta::assert_ron_snapshot!(
            test_parse_signature("coalesce(values+: any) -> any"),
            @r###"
        Ok(Signature(
          name: "coalesce",
          parameters: Parameters(
            names: [
              Located(
                value: "values",
                location: Location(
                  part: Internal("coalesce(values+: any) -> any"),
                  start: 9,
                  end: 15,
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
                value: Generic(Any),
                location: Location(
                  part: Internal("coalesce(values+: any) -> any"),
                  start: 18,
                  end: 21,
                ),
              ),
            ],
            defaults: [
              None,
            ],
            has_vararg: true,
          ),
          result: Generic(Any),
        ))
        "###);
    }

    #[test]
    fn test_reject_number_on_rhs() {
        insta::assert_ron_snapshot!(
        test_parse_signature("add(lhs: f64, rhs: f64) -> number"),
        @r###"Err("User { error: (4, \"Illegal signature for \'add\': number must appear in the parameters to be used in the result\", 22) }")"###);
    }
}
