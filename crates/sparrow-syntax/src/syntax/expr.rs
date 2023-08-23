use std::fmt::Debug;
use std::sync::Arc;

use codespan_reporting::diagnostic::Label;
use static_init::dynamic;

use crate::parser::try_parse_expr;
use crate::{ArgVec, Arguments, FenlType, LiteralValue, ParseErrors, Resolved};

/// Identifies a specific part of a feature set query.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(test, derive(serde::Serialize))]
pub enum FeatureSetPart {
    /// Internal definitions that don't reference contents of FeatureSet.
    ///
    /// The value is the *source* of the internal definition.
    Internal(&'static str),
    /// The definition of `$input`. Reported as "inferred $input".
    Input,
    /// A catalog entry for the Nth registered instruction.
    ///
    /// The value is the function name or signature in the catalog.
    Function(&'static str),
    /// The formula entry for the Nth formula in the feature set.
    Formula(u32),
    /// The query.
    Query,
    /// Code coming from python.
    Builder,
}

/// The location of part of an expression in the original source.
///
/// Contains the start and end position in bytes within the source.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct Location {
    /// The part of the feature set this location is in.
    part: FeatureSetPart,
    /// The start of the range of bytes containing this location.
    start: usize,
    /// The end of the range of bytes containing this location.
    end: usize,
}

impl Location {
    pub fn new(part: FeatureSetPart, start: usize, end: usize) -> Self {
        Self { part, start, end }
    }

    pub fn builder() -> Self {
        Location::new(FeatureSetPart::Builder, 0, "builder".len())
    }

    pub fn internal_str(value: &'static str) -> Self {
        Self {
            part: FeatureSetPart::Internal(value),
            start: 0,
            end: value.len(),
        }
    }

    pub fn primary_label(&self) -> Label<FeatureSetPart> {
        Label::primary(self.part, self.start..self.end)
    }

    pub fn secondary_label(&self) -> Label<FeatureSetPart> {
        Label::secondary(self.part, self.start..self.end)
    }

    pub fn part(&self) -> FeatureSetPart {
        self.part
    }

    pub fn start(&self) -> usize {
        self.start
    }

    pub fn end(&self) -> usize {
        self.end
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct Located<T> {
    value: T,
    location: Location,
}

impl<T: std::fmt::Display> std::fmt::Display for Located<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

impl Located<&'static str> {
    pub fn internal_str(value: &'static str) -> Self {
        let location = Location::new(FeatureSetPart::Internal(value), 0, value.len());
        Self::new(value, location)
    }
}

impl Located<String> {
    pub fn internal_string(value: &'static str) -> Self {
        let location = Location::new(FeatureSetPart::Internal(value), 0, value.len());
        Self::new(value.to_owned(), location)
    }
}

impl<T> Located<T> {
    pub fn new(value: T, location: Location) -> Self {
        Self { value, location }
    }

    pub fn builder(value: T) -> Self {
        Self {
            value,
            location: Location::builder(),
        }
    }

    pub fn inner(&self) -> &T {
        &self.value
    }

    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.value
    }

    pub fn into_inner(self) -> T {
        self.value
    }

    pub fn location(&self) -> &Location {
        &self.location
    }

    pub fn take_location(self) -> Location {
        self.location
    }

    pub fn transform<T2>(self, f: impl FnOnce(T) -> T2) -> Located<T2> {
        Located {
            value: f(self.value),
            location: self.location,
        }
    }

    pub fn try_map<T2, E>(self, f: impl FnOnce(T) -> Result<T2, E>) -> Result<Located<T2>, E> {
        Ok(Located {
            value: f(self.value)?,
            location: self.location,
        })
    }

    pub fn with_value<T2>(&self, value: T2) -> Located<T2> {
        Located {
            value,
            location: self.location.clone(),
        }
    }

    pub fn update_value(&mut self, value: T) {
        self.value = value;
    }

    pub fn as_ref(&self) -> Located<&T> {
        Located {
            value: &self.value,
            location: self.location.clone(),
        }
    }
}

// A `Located<T>` behaves like a `T` with additional information.
impl<T> std::ops::Deref for Located<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner()
    }
}

impl<T> AsRef<T> for Located<T> {
    fn as_ref(&self) -> &T {
        self.inner()
    }
}

impl<T: PartialEq> PartialEq for Located<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T: Eq> Eq for Located<T> {}

impl<T: PartialOrd> PartialOrd for Located<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.value.partial_cmp(&other.value)
    }
}

impl<T: Ord> Ord for Located<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value.cmp(&other.value)
    }
}

impl<T: std::hash::Hash> std::hash::Hash for Located<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

/// A reference counted expression.
///
/// This is generally used to represent the nodes in the AST so they
/// can be passed around without copying.
pub type ExprRef = Arc<Expr>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct Expr {
    op: ExprOp,
    /// The arguments to the expression.
    ///
    /// The expression needs to be boxed (or in this case reference counted) to
    /// avoid infinite recursion when put in the `SmallVec` in `Arguments`.
    args: Arguments<ExprRef>,
}

/// The result of parsing and resolving an expression.
///
/// A resolved expression has verified its signature against
/// the arguments given.
#[derive(Debug, PartialEq, Eq)]
pub struct ResolvedExpr {
    pub op: ExprOp,
    // The resolved arguments to the expression.
    pub args: Resolved<Located<Box<ResolvedExpr>>>,
}

impl ResolvedExpr {
    pub fn op(&self) -> &ExprOp {
        &self.op
    }

    pub fn args(&self) -> &Resolved<Located<Box<ResolvedExpr>>> {
        &self.args
    }

    pub fn arg(&self, index: usize) -> Option<&Located<Box<ResolvedExpr>>> {
        self.args.values().get(index)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(serde::Serialize))]
pub enum ExprOp {
    Literal(Located<LiteralValue>),
    Reference(Located<String>),
    /// A field ref expression. Specifies the field to access.
    FieldRef(Located<String>, Location),
    /// A call expression. Specifies the name of the function being called.
    Call(Located<String>),
    /// A pipe expression.
    ///
    /// While similar to a binary call, pipe binds the lhs to `$input`, so the
    /// behavior is different enough to merit a separate operator.
    Pipe(Location),
    /// A let expression. Specifies the names of the bindings.
    /// Should be one more argument than bindings, corresponding to the body.
    ///
    /// Each of the bindings is evaluated and added to the environment in order
    /// and then the value is evaluated and returned. Earlier bindings are
    /// available to the expressions of later bindings.
    Let(ArgVec<Located<String>>, Location),
    /// A record expression. Creates a record with the given field names.
    /// Each argument corresponds to a record.
    Record(ArgVec<Located<String>>, Location),
    /// Record extension.
    ExtendRecord(Location),
    /// Remove Fields.
    ///
    /// This is not currently possible as a function since the result type
    /// needs to be computed from the input struct type. May be possible
    /// if we either had an "escape hatch" for this special handling or
    /// row polymorphism.
    RemoveFields(Location),
    /// Select Fields.
    ///
    /// This is not currently possible as a function since the result type
    /// needs to be computed from the input struct type. May be possible
    /// if we either had an "escape hatch" for this special handling or
    /// row polymorphism.
    SelectFields(Location),
    /// Cast the input to the given type.
    Cast(Located<FenlType>, Location),
    /// Indicates an error parsing an expression.
    Error,
}

#[dynamic]
static IMPLICIT_INPUT: ExprRef = {
    let reference = Expr::reference(Located::new(
        "$input",
        Location::new(FeatureSetPart::Input, 0, "$input".len()),
    ));
    Arc::new(reference)
};

impl Expr {
    pub fn number(input: Located<&str>) -> Expr {
        Self::literal(input.transform(|s| LiteralValue::Number(s.to_owned())))
    }

    pub fn literal(literal: Located<LiteralValue>) -> Expr {
        Self {
            op: ExprOp::Literal(literal),
            args: Arguments::empty(),
        }
    }

    pub fn implicit_input() -> &'static ExprRef {
        &IMPLICIT_INPUT
    }

    pub fn reference(ident: Located<&str>) -> Expr {
        Self {
            op: ExprOp::Reference(ident.transform(ToOwned::to_owned)),
            args: Arguments::empty(),
        }
    }

    pub fn new(op: ExprOp, args: impl IntoIterator<Item = Located<ExprRef>>) -> Expr {
        Self {
            op,
            args: args.into_iter().collect(),
        }
    }

    pub fn error() -> Expr {
        Self {
            op: ExprOp::Error,
            args: Arguments::empty(),
        }
    }

    pub fn call(
        function_name: Located<&str>,
        args: impl IntoIterator<Item = Located<ExprRef>>,
    ) -> Expr {
        Self::call_args(function_name, args.into_iter().collect())
    }

    pub fn call_args(function_name: Located<&str>, args: Arguments<ExprRef>) -> Expr {
        let op = match *function_name.inner() {
            "extend" => ExprOp::ExtendRecord(function_name.location),
            "remove_fields" => ExprOp::RemoveFields(function_name.location),
            "select_fields" => ExprOp::SelectFields(function_name.location),
            _ => ExprOp::Call(function_name.transform(ToOwned::to_owned)),
        };
        Self { op, args }
    }

    pub fn new_field_ref(base: Located<ExprRef>, field: Located<&str>, location: Location) -> Expr {
        Self {
            op: ExprOp::FieldRef(field.transform(ToOwned::to_owned), location),
            args: vec![base].into_iter().collect(),
        }
    }

    pub fn new_let(
        bindings: ArgVec<(Located<&str>, Located<ExprRef>)>,
        body: Located<ExprRef>,
        location: Location,
    ) -> Expr {
        let names = bindings
            .iter()
            .map(|(name, _)| name.clone().transform(ToOwned::to_owned))
            .chain([Located::internal_string("let_body")])
            .collect();
        let args = bindings
            .into_iter()
            .map(|(_, value)| value)
            .chain([body])
            .collect();
        Expr {
            op: ExprOp::Let(names, location),
            args,
        }
    }

    pub fn new_record(
        fields: ArgVec<(Located<&str>, Located<ExprRef>)>,
        location: Location,
    ) -> Expr {
        let names: ArgVec<Located<String>> = fields
            .iter()
            .map(|(name, _)| name.clone().transform(ToOwned::to_owned))
            .collect();
        let args = fields.into_iter().map(|(_, value)| value).collect();

        Expr {
            op: ExprOp::Record(names, location),
            args,
        }
    }

    /// Parse the expression from a string.
    pub fn try_from_str(part_id: FeatureSetPart, input: &str) -> Result<ExprRef, ParseErrors<'_>> {
        try_parse_expr(part_id, input)
    }

    pub fn op(&self) -> &ExprOp {
        &self.op
    }

    pub fn args(&self) -> &Arguments<ExprRef> {
        &self.args
    }

    pub fn arg(&self, index: usize) -> Option<&Located<ExprRef>> {
        self.args.get(index).map(|arg| arg.value())
    }
}
