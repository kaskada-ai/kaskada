use std::str::FromStr;

use arrow_schema::DataType;

use crate::signature::{Parameter, Signature, TypeParameter};
use crate::type_class::TypeClass;
use crate::{Error, FenlType, TypeConstructor};

pub(super) fn parse_signature(input: &str) -> error_stack::Result<Signature, Error> {
    let mut parser = Parser::new(input);
    let signature = parser.parse_signature()?;
    parser.finish(true)?;

    signature.validate();
    Ok(signature)
}

#[cfg(test)]
pub(super) fn parse_types(input: &str) -> error_stack::Result<Vec<FenlType>, Error> {
    let mut types = Vec::new();
    let mut parser = Parser::new(input);

    while !parser.is_eof() {
        types.push(parser.parse_type()?);
        if !parser.try_consume(",") {
            break;
        }
    }
    parser.finish(true)?;
    Ok(types)
}

struct Parser<'a> {
    str: &'a str,
    location: usize,
}

#[static_init::dynamic]
static IDENTIFIER: regex::Regex = regex::Regex::new(r"^[a-zA-Z][a-zA-Z0-9_]*").unwrap();

#[static_init::dynamic]
static WHITESPACE: regex::Regex = regex::Regex::new(r"^\s+").unwrap();

impl<'a> Parser<'a> {
    fn new(s: &'a str) -> Self {
        Self {
            str: s,
            location: 0,
        }
    }

    fn is_eof(&self) -> bool {
        self.location == self.str.len()
    }

    fn finish(self, require_eof: bool) -> error_stack::Result<(), Error> {
        error_stack::ensure!(
            !require_eof || self.is_eof(),
            Error::InvalidSignature {
                signature: self.str.to_string(),
                position: self.location,
                reason: "unexpected characters at end of signature".to_string(),
            }
        );
        Ok(())
    }

    fn parse_signature(&mut self) -> error_stack::Result<Signature, Error> {
        let type_parameters = self.parse_type_parameters()?;
        let (parameters, variadic) = self.parse_parameters()?;
        self.consume("->")?;
        let result = self.parse_type()?;

        Ok(Signature {
            type_parameters,
            parameters,
            result,
            variadic,
        })
    }

    fn identifier(&mut self) -> error_stack::Result<&'a str, Error> {
        let identifier = IDENTIFIER
            .find(&self.str[self.location..])
            .map(|m| {
                self.location += m.len();
                m.as_str()
            })
            .ok_or_else(|| {
                error_stack::report!(Error::InvalidSignature {
                    signature: self.str.to_string(),
                    position: self.location,
                    reason: "missing identifier".to_string(),
                })
            })?;
        self.consume_white_space();
        Ok(identifier)
    }

    fn consume_white_space(&mut self) -> bool {
        let whitespace = WHITESPACE.find(&self.str[self.location..]);
        match whitespace {
            Some(m) => {
                self.location += m.len();
                true
            }
            None => false,
        }
    }

    fn consume(&mut self, p: &'static str) -> error_stack::Result<(), Error> {
        error_stack::ensure!(
            self.try_consume(p),
            Error::InvalidSignature {
                signature: self.str.to_string(),
                position: self.location,
                reason: format!("expected '{}'", p),
            }
        );
        Ok(())
    }

    /// Tries to consume `p` and returns true if successful.
    #[must_use]
    fn try_consume(&mut self, p: &'static str) -> bool {
        if self.peek().starts_with(p) {
            self.location += p.len();
            self.consume_white_space();
            true
        } else {
            false
        }
    }

    fn peek(&self) -> &str {
        &self.str[self.location..]
    }

    fn parse_parameters(&mut self) -> error_stack::Result<(Vec<Parameter>, bool), Error> {
        self.consume("(")?;
        let mut parameters = Vec::new();
        let mut variadic = false;
        loop {
            let name = self.identifier()?;
            self.consume(":")?;
            let ty = self.parse_type()?;
            parameters.push(Parameter {
                name: name.to_string(),
                ty,
            });

            if self.try_consume(")") {
                break;
            }
            if self.try_consume("...") {
                variadic = true;
                self.consume(")")?;
                break;
            }
            self.consume(",")?;
        }
        Ok((parameters, variadic))
    }

    fn parse_type_parameters(&mut self) -> error_stack::Result<Vec<TypeParameter>, Error> {
        if !self.try_consume("<") {
            return Ok(Vec::new());
        }

        let mut type_parameters = Vec::new();
        loop {
            let name = self.identifier()?;
            let type_classes = self.parse_type_classes()?;
            type_parameters.push(TypeParameter {
                name: name.to_string(),
                type_classes,
            });

            // Optional trailing commas.
            if self.try_consume(">") {
                break;
            }
            self.consume(",")?;
        }

        Ok(type_parameters)
    }

    fn parse_type_class(&mut self) -> error_stack::Result<TypeClass, Error> {
        let position = self.location;
        let type_class = self.identifier()?;
        let type_class = TypeClass::from_str(type_class).map_err(|_| Error::InvalidSignature {
            signature: self.str.to_owned(),
            position,
            reason: format!("invalid type class '{type_class})"),
        })?;
        Ok(type_class)
    }

    fn parse_type_classes(&mut self) -> error_stack::Result<Vec<TypeClass>, Error> {
        if !self.try_consume(":") {
            return Ok(vec![TypeClass::Any]);
        }

        let mut type_classes = Vec::new();
        type_classes.push(self.parse_type_class()?);
        while self.try_consume("+") {
            type_classes.push(self.parse_type_class()?);
        }

        Ok(type_classes)
    }

    fn parse_type(&mut self) -> error_stack::Result<FenlType, Error> {
        let ident = self.identifier()?;
        match ident {
            "bool" => Ok(FenlType::from(DataType::Boolean)),
            "i8" => Ok(FenlType::from(DataType::Int8)),
            "i16" => Ok(FenlType::from(DataType::Int16)),
            "i32" => Ok(FenlType::from(DataType::Int32)),
            "i64" => Ok(FenlType::from(DataType::Int64)),
            "u8" => Ok(FenlType::from(DataType::UInt8)),
            "u16" => Ok(FenlType::from(DataType::UInt16)),
            "u32" => Ok(FenlType::from(DataType::UInt32)),
            "u64" => Ok(FenlType::from(DataType::UInt64)),
            "f16" => Ok(FenlType::from(DataType::Float16)),
            "f32" => Ok(FenlType::from(DataType::Float32)),
            "f64" => Ok(FenlType::from(DataType::Float64)),
            "str" => Ok(FenlType::from(DataType::Utf8)),
            "map" => {
                self.consume("<")?;
                let key = self.parse_type()?;
                self.consume(",")?;
                let value = self.parse_type()?;
                self.consume(">")?;
                Ok(FenlType {
                    type_constructor: TypeConstructor::Map(false),
                    type_args: vec![key, value],
                })
            }
            "list" => {
                self.consume("<")?;
                let value = self.parse_type()?;
                self.consume(">")?;
                Ok(FenlType {
                    type_constructor: TypeConstructor::List,
                    type_args: vec![value],
                })
            }
            variable => Ok(FenlType {
                type_constructor: TypeConstructor::Generic(variable.to_string()),
                type_args: Vec::new(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::type_class::TypeClass;
    use crate::{FenlType, Parameter, Signature, TypeParameter};

    #[test]
    fn test_identifier() {
        let mut parser = super::Parser::new("foo  bar baz i8 f64");
        assert_eq!(parser.identifier().unwrap(), "foo");
        assert_eq!(parser.location, 5);

        assert_eq!(parser.identifier().unwrap(), "bar");
        assert_eq!(parser.location, 9);

        assert_eq!(parser.identifier().unwrap(), "baz");
        assert_eq!(parser.identifier().unwrap(), "i8");
        assert_eq!(parser.identifier().unwrap(), "f64");
    }

    #[test]
    fn test_parse_signature_simple() {
        assert_eq!(
            super::parse_signature("(a: i64) -> str").unwrap(),
            Signature {
                type_parameters: vec![],
                parameters: vec![Parameter {
                    name: "a".to_string(),
                    ty: FenlType::from(arrow_schema::DataType::Int64),
                }],
                result: FenlType::from(arrow_schema::DataType::Utf8),
                variadic: false,
            }
        );
    }

    #[test]
    fn test_parse_signature_constrained_generic() {
        assert_eq!(
            super::parse_signature("<T: number>(a: T, b: T) -> T").unwrap(),
            Signature {
                type_parameters: vec![TypeParameter {
                    name: "T".to_string(),
                    type_classes: vec![TypeClass::Number]
                }],
                parameters: vec![
                    Parameter {
                        name: "a".to_string(),
                        ty: FenlType::generic("T"),
                    },
                    Parameter {
                        name: "b".to_string(),
                        ty: FenlType::generic("T")
                    }
                ],
                result: FenlType::generic("T"),
                variadic: false,
            }
        );
    }

    #[test]
    fn test_parse_signature_unconstrained_generic() {
        assert_eq!(
            super::parse_signature("<T>(a: T, b: T) -> T").unwrap(),
            Signature {
                type_parameters: vec![TypeParameter {
                    name: "T".to_string(),
                    type_classes: vec![TypeClass::Any]
                }],
                parameters: vec![
                    Parameter {
                        name: "a".to_string(),
                        ty: FenlType::generic("T"),
                    },
                    Parameter {
                        name: "b".to_string(),
                        ty: FenlType::generic("T")
                    }
                ],
                result: FenlType::generic("T"),
                variadic: false,
            }
        );
    }

    #[test]
    fn test_parse_signature_generic_variadic() {
        assert_eq!(
            super::parse_signature("<T: number>(a: T, b: T...) -> T").unwrap(),
            Signature {
                type_parameters: vec![TypeParameter {
                    name: "T".to_string(),
                    type_classes: vec![TypeClass::Number]
                }],
                parameters: vec![
                    Parameter {
                        name: "a".to_string(),
                        ty: FenlType::generic("T"),
                    },
                    Parameter {
                        name: "b".to_string(),
                        ty: FenlType::generic("T"),
                    },
                ],
                result: FenlType::generic("T"),
                variadic: true,
            }
        );
    }

    #[test]
    fn test_identifier_fail() {
        let mut parser = super::Parser::new("+  foo");
        assert!(parser.identifier().is_err());
    }
}
