use arrow_schema::DataType;
use egg::Id;
use smallvec::SmallVec;
use sparrow_arrow::scalar_value::ScalarValue;

use crate::Error;

#[derive(Hash, PartialOrd, Ord, PartialEq, Eq, Clone, Debug)]
pub(crate) struct ExprLang {
    /// The name of the instruction being applied by this expression.
    ///
    /// Similar to an opcode or function.
    ///
    /// Generally, interning owned strings to the specific static strings is preferred.
    pub name: &'static str,
    /// Literal arguments to the expression.
    pub literal_args: SmallVec<[ScalarValue; 2]>,
    /// Arguments to the expression.
    pub args: SmallVec<[egg::Id; 2]>,
    // TODO: This includes the `DataType` in the enodes.
    // This is necessary for ensuring that cast instructions to different types are treated
    // as distinct, however it is potentially risky for writing simplifications, since the
    // patterns won't have specific types. We may need to make this optional, so only the
    // cast instruction has to specify it, and then rely on analysis to infer the types.
    pub result_type: DataType,
}

// It is weird that we need to implement `Display` for `ExprLang` to pretty print
// only the kind. But, this is a requirement of `egg`.
impl std::fmt::Display for ExprLang {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.name.fmt(f)
    }
}

impl egg::Language for ExprLang {
    fn children(&self) -> &[egg::Id] {
        &self.args
    }

    fn children_mut(&mut self) -> &mut [egg::Id] {
        &mut self.args
    }

    fn matches(&self, other: &Self) -> bool {
        // Note: As per
        // https://egraphs-good.github.io/egg/egg/trait.Language.html#tymethod.matches,
        // "This should only consider the operator, not the children `Id`s".
        //
        // egg itself will check whether the arguments are *equivalent*.
        //
        // Some instructions (especially `cast`) depend on the `result_type` to
        // determine the operation being performed.

        self.name == other.name
            && self.literal_args == other.literal_args
            && self.result_type == other.result_type
    }
}

impl egg::FromOp for ExprLang {
    type Error = error_stack::Report<Error>;

    fn from_op(op: &str, children: Vec<Id>) -> Result<Self, Self::Error> {
        let name = sparrow_interfaces::expression::intern_name(op)
            .ok_or_else(|| Error::NoSuchInstruction(op.to_owned()))?;

        let args = SmallVec::from_vec(children);
        Ok(Self {
            name,
            literal_args: smallvec::smallvec![],
            args,
            result_type: DataType::Null,
        })
    }
}
