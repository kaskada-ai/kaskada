use std::str::FromStr;

/// Fenl uses a limited form of ad-hoc polymorphism for functions.
///
/// Specifically, function signatures may have one or more type variables
/// constrained to satiisfy zero or more of the following constraints.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, derive_more::Display, enum_map::Enum)]
pub enum TypeClass {
    /// Any type.
    #[display(fmt = "any")]
    Any,
    /// Any type that is a valid key.
    #[display(fmt = "key")]
    Key,
    /// Any numeric type.
    #[display(fmt = "number")]
    Number,
    /// Any signed numeric type.
    #[display(fmt = "signed")]
    Signed,
    /// Any floating point numeric type.
    #[display(fmt = "float")]
    Float,
    /// Any time delta.
    #[display(fmt = "timedelta")]
    TimeDelta,
    /// Any ordered type. This includes numbers and time stamps.
    #[display(fmt = "ordered")]
    Ordered,
}

impl FromStr for TypeClass {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "any" => Ok(Self::Any),
            "key" => Ok(Self::Key),
            "number" => Ok(Self::Number),
            "signed" => Ok(Self::Signed),
            "float" => Ok(Self::Float),
            "timedelta" => Ok(Self::TimeDelta),
            "ordered" => Ok(Self::Ordered),
            _ => Err(()),
        }
    }
}
