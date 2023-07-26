//! An implementation of the [`Files`] trait backed by the [`FeatureSet`].

use codespan_reporting::files::{line_starts, Error, Files};
use sparrow_api::kaskada::v1alpha::FeatureSet;
use sparrow_syntax::FeatureSetPart;

/// Provides the positions and contents of the [FeatureSet] for diagnostics.
pub(crate) struct FeatureSetParts<'a> {
    /// The feature set the diagnostics relate to.
    feature_set: &'a FeatureSet,
    /// For each file ID in the feature set, store the line starts.
    /// This is used to efficiently determine the start of a line and the
    /// relative position in the line of absolute offsets within a given
    /// source.
    formula_line_starts: Vec<Vec<usize>>,
    query_line_starts: Vec<usize>,
}

/// An enumeration of the parts within the `FeatureSet`.
///
/// This contains a reference to the string from the `FeatureSet` and
/// allows rendering it as needed, allowing for zero-copy display.
pub enum PartName<'a> {
    /// Internal locations that don't (yet) support source.
    Internal,
    /// Inferred / implicit `$input`.
    Input,
    /// Built-in definitions
    Builtin(&'a str),
    /// User specified "source name" for this part.
    Label(&'a str),
    /// Compute the "source name" from the given formula name.
    Formula(&'a str),
    /// Return the part name for the query string.
    Query,
    Builder,
}

impl<'a> std::fmt::Display for PartName<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartName::Internal => write!(f, "internal"),
            PartName::Input => write!(f, "inferred $input"),
            PartName::Builtin(signature) => write!(f, "built-in signature '{signature}'"),
            PartName::Label(name) => write!(f, "'{name}'"),
            PartName::Formula(name) => write!(f, "'Formula: {name}'"),
            PartName::Query => write!(f, "Query"),
            PartName::Builder => write!(f, "Builder"),
        }
    }
}

/// The `starts` for a single line. Used for function signatures.
const ONE_LINE_STARTS: [usize; 1] = [0];

impl<'a> FeatureSetParts<'a> {
    /// Create a `FeatureSetParts` for the given [FeatureSet].
    pub(crate) fn new(feature_set: &'a FeatureSet) -> Self {
        let formula_line_starts = feature_set
            .formulas
            .iter()
            .map(|formula| line_starts(&formula.formula).collect())
            .collect();
        let query_line_starts = line_starts(&feature_set.query).collect();

        Self {
            feature_set,
            formula_line_starts,
            query_line_starts,
        }
    }

    fn line_range_helper(
        &self,
        id: FeatureSetPart,
        line_starts: &[usize],
        line_index: usize,
    ) -> Result<std::ops::Range<usize>, Error> {
        use std::cmp::Ordering;
        match (line_index + 1).cmp(&line_starts.len()) {
            Ordering::Less => Ok(line_starts[line_index]..line_starts[line_index + 1]),
            Ordering::Equal => Ok(line_starts[line_index]..self.source(id)?.len()),
            Ordering::Greater => Err(Error::LineTooLarge {
                given: line_index,
                max: line_starts.len() - 1,
            }),
        }
    }
}

impl<'a> Files<'a> for FeatureSetParts<'a> {
    type FileId = FeatureSetPart;

    type Name = PartName<'a>;

    type Source = &'a str;

    fn name(&'a self, id: Self::FileId) -> Result<Self::Name, Error> {
        match id {
            FeatureSetPart::Internal(_) => Ok(PartName::Internal),
            FeatureSetPart::Input => Ok(PartName::Input),
            FeatureSetPart::Function(index) => Ok(PartName::Builtin(index)),
            FeatureSetPart::Formula(index) => {
                let formula = &self
                    .feature_set
                    .formulas
                    .get(index as usize)
                    .ok_or(Error::FileMissing)?;

                if formula.source_location.is_empty() {
                    Ok(PartName::Formula(&formula.name))
                } else {
                    Ok(PartName::Label(&formula.source_location))
                }
            }
            FeatureSetPart::Query => Ok(PartName::Query),
            FeatureSetPart::Builder => Ok(PartName::Builder),
        }
    }

    fn source(&'a self, id: Self::FileId) -> Result<Self::Source, Error> {
        match id {
            FeatureSetPart::Internal(source) => Ok(source),
            FeatureSetPart::Input => Ok("$input"),
            FeatureSetPart::Function(source) => Ok(source),
            FeatureSetPart::Formula(index) => Ok(&self
                .feature_set
                .formulas
                .get(index as usize)
                .ok_or(Error::FileMissing)?
                .formula),
            FeatureSetPart::Query => Ok(&self.feature_set.query),
            FeatureSetPart::Builder => Ok("builder"),
        }
    }

    fn line_index(&'a self, id: Self::FileId, byte_index: usize) -> Result<usize, Error> {
        let line_starts: &[usize] = match id {
            FeatureSetPart::Input => return Ok(0),
            FeatureSetPart::Internal(s) => return Ok(s[0..byte_index].matches('\n').count()),
            FeatureSetPart::Function(_) => &ONE_LINE_STARTS,
            FeatureSetPart::Formula(index) => self
                .formula_line_starts
                .get(index as usize)
                .ok_or(Error::FileMissing)?,
            FeatureSetPart::Query => &self.query_line_starts,
            FeatureSetPart::Builder => return Ok(0),
        };

        Ok(line_starts
            .binary_search(&byte_index)
            .unwrap_or_else(|next_line| next_line - 1))
    }

    fn line_range(
        &'a self,
        id: Self::FileId,
        line_index: usize,
    ) -> Result<std::ops::Range<usize>, Error> {
        let line_starts: &[usize] = match id {
            FeatureSetPart::Internal(s) => {
                return self.line_range_helper(id, &line_starts(s).collect::<Vec<_>>(), line_index)
            }
            FeatureSetPart::Input => return Ok(0.."$input".len()),
            FeatureSetPart::Function(_) => &ONE_LINE_STARTS,
            FeatureSetPart::Formula(index) => self
                .formula_line_starts
                .get(index as usize)
                .ok_or(Error::FileMissing)?,
            FeatureSetPart::Query => &self.query_line_starts,
            FeatureSetPart::Builder => return Ok(0.."builder".len()),
        };

        self.line_range_helper(id, line_starts, line_index)
    }
}

#[cfg(test)]
mod tests {
    use sparrow_api::kaskada::v1alpha::Formula;

    use super::*;
    use crate::functions::get_function;

    fn feature_set_fixture() -> FeatureSet {
        FeatureSet {
            formulas: vec![
                Formula {
                    name: "a".to_owned(),
                    formula: "5 + 10".to_owned(),
                    source_location: "".to_owned(),
                },
                Formula {
                    name: "b".to_owned(),
                    formula: "6 + 10".to_owned(),
                    source_location: "".to_owned(),
                },
                Formula {
                    name: "c".to_owned(),
                    formula: "let sum = a + b\nin sum + 5".to_owned(),
                    source_location: "ViewFoo".to_owned(),
                },
            ],
            query: "let foo = a\nlet bar = b\nin { foo, bar, baz }".to_owned(),
        }
    }

    #[test]
    fn test_name() {
        let feature_set = feature_set_fixture();
        let parts = FeatureSetParts::new(&feature_set);

        assert_eq!(
            &parts
                .name(FeatureSetPart::Function("sum(a: i64, b: i64) -> i64"))
                .unwrap()
                .to_string(),
            "built-in signature 'sum(a: i64, b: i64) -> i64'"
        );
        assert_eq!(
            &parts.name(FeatureSetPart::Formula(0)).unwrap().to_string(),
            "'Formula: a'"
        );
        assert_eq!(
            &parts.name(FeatureSetPart::Formula(1)).unwrap().to_string(),
            "'Formula: b'"
        );
        assert_eq!(
            &parts.name(FeatureSetPart::Formula(2)).unwrap().to_string(),
            "'ViewFoo'"
        );
        assert_eq!(
            &parts.name(FeatureSetPart::Query).unwrap().to_string(),
            "Query"
        );
    }

    #[test]
    fn test_source() {
        let feature_set = feature_set_fixture();
        let parts = FeatureSetParts::new(&feature_set);

        let sum = get_function("sum").unwrap();
        assert_eq!(
            &parts
                .source(FeatureSetPart::Function(sum.signature_str()))
                .unwrap()
                .to_string(),
            sum.signature_str()
        );

        assert_eq!(
            &parts
                .source(FeatureSetPart::Formula(0))
                .unwrap()
                .to_string(),
            "5 + 10"
        );
        assert_eq!(
            &parts
                .source(FeatureSetPart::Formula(1))
                .unwrap()
                .to_string(),
            "6 + 10"
        );
        assert_eq!(
            &parts
                .source(FeatureSetPart::Formula(2))
                .unwrap()
                .to_string(),
            "let sum = a + b\nin sum + 5"
        );
        assert_eq!(
            &parts.source(FeatureSetPart::Query).unwrap().to_string(),
            "let foo = a\nlet bar = b\nin { foo, bar, baz }"
        );
    }

    #[test]
    fn test_line_index() {
        let feature_set = feature_set_fixture();
        let parts = FeatureSetParts::new(&feature_set);

        assert_eq!(parts.line_index(FeatureSetPart::Formula(2), 0).unwrap(), 0);
        assert_eq!(parts.line_index(FeatureSetPart::Formula(2), 1).unwrap(), 0);
        assert_eq!(parts.line_index(FeatureSetPart::Formula(2), 15).unwrap(), 0);
        assert_eq!(parts.line_index(FeatureSetPart::Formula(2), 16).unwrap(), 1);
        assert_eq!(parts.line_index(FeatureSetPart::Formula(2), 17).unwrap(), 1);

        assert_eq!(parts.line_index(FeatureSetPart::Query, 0).unwrap(), 0);
        assert_eq!(parts.line_index(FeatureSetPart::Query, 1).unwrap(), 0);
        assert_eq!(parts.line_index(FeatureSetPart::Query, 11).unwrap(), 0);
        assert_eq!(parts.line_index(FeatureSetPart::Query, 12).unwrap(), 1);
        assert_eq!(parts.line_index(FeatureSetPart::Query, 13).unwrap(), 1);
        assert_eq!(parts.line_index(FeatureSetPart::Query, 23).unwrap(), 1);
        assert_eq!(parts.line_index(FeatureSetPart::Query, 24).unwrap(), 2);
        assert_eq!(parts.line_index(FeatureSetPart::Query, 25).unwrap(), 2);
    }

    #[test]
    fn test_line_range() {
        let feature_set = feature_set_fixture();
        let parts = FeatureSetParts::new(&feature_set);

        assert_eq!(
            parts.line_range(FeatureSetPart::Formula(2), 0).unwrap(),
            0..16
        );
        assert_eq!(
            parts.line_range(FeatureSetPart::Formula(2), 1).unwrap(),
            16..26
        );

        assert_eq!(parts.line_range(FeatureSetPart::Query, 0).unwrap(), 0..12);
        assert_eq!(parts.line_range(FeatureSetPart::Query, 1).unwrap(), 12..24);
        assert_eq!(parts.line_range(FeatureSetPart::Query, 2).unwrap(), 24..44);
    }

    #[test]
    fn test_internal() {
        let feature_set = feature_set_fixture();
        let parts = FeatureSetParts::new(&feature_set);

        let location = sparrow_syntax::Located::internal_str("hello")
            .location()
            .clone();
        let id = location.part();
        assert_eq!(parts.name(id).unwrap().to_string(), "internal");
        assert_eq!(parts.source(id).unwrap(), "hello");
        assert_eq!(parts.line_index(id, location.start()).unwrap(), 0);
        assert_eq!(parts.line_index(id, location.end()).unwrap(), 0);
        assert_eq!(parts.line_range(id, 0).unwrap(), 0..5);
    }

    #[test]
    fn test_input() {
        let feature_set = feature_set_fixture();
        let parts = FeatureSetParts::new(&feature_set);

        let location = sparrow_syntax::Location::new(FeatureSetPart::Input, 0, "$input".len());
        let id = location.part();
        assert_eq!(parts.name(id).unwrap().to_string(), "inferred $input");
        assert_eq!(parts.source(id).unwrap(), "$input");
        assert_eq!(parts.line_index(id, location.start()).unwrap(), 0);
        assert_eq!(parts.line_index(id, location.end()).unwrap(), 0);
        assert_eq!(parts.line_range(id, 0).unwrap(), 0..6);
    }
}
