use itertools::Itertools;

use crate::{Error, ExprRef};

/// A wrapper around a u32 identifying a distinct grouping.
#[derive(Debug, Eq, PartialEq, Clone, Copy, Hash, Ord, PartialOrd)]
#[repr(transparent)]
pub struct GroupId(u32);

/// The grouping associated with an expression.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum Grouping {
    Literal,
    Group(GroupId),
}

impl Grouping {
    pub fn new(group: u32) -> Grouping {
        Grouping::Group(GroupId(group))
    }

    pub fn from_args(args: &[ExprRef]) -> error_stack::Result<Grouping, Error> {
        let groupings = args
            .iter()
            .map(|arg| &arg.grouping)
            .unique()
            .filter(|g| **g == Grouping::Literal)
            .cloned();

        match groupings.at_most_one() {
            Ok(None) => Ok(Grouping::Literal),
            Ok(Some(grouping)) => Ok(grouping),
            Err(groupings) => {
                let groupings: Vec<_> = groupings.collect();
                error_stack::bail!(Error::IncompatibleGroupings(groupings))
            }
        }
    }
}
