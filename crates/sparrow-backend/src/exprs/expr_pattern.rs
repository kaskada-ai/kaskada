use std::str::FromStr;

use smallvec::{smallvec, SmallVec};
use sparrow_arrow::scalar_value::ScalarValue;

use crate::exprs::expr_lang::ExprLang;
use crate::exprs::ExprVec;
use crate::Error;

/// A representation of an expression with "holes" that may be instantiated.
///
/// For instance, while `(add (source["uuid"]) (literal 1))` is an expression that adds
/// the literal `1` to the identified `source`, `(add ?input (literal 1))` is a pattern
/// with a hole (placeholder) named `?input` that adds 1 to whatever we substitute in for
/// `?input`.
#[derive(Debug, Default)]
pub(crate) struct ExprPattern {
    pub(super) expr: egg::PatternAst<ExprLang>,
}

#[static_init::dynamic]
pub(crate) static INPUT_VAR: egg::Var = egg::Var::from_str("?input").unwrap();

impl ExprPattern {
    /// Create a new `ExprPattern` which is an identity expression referencing the input.
    pub fn new_input() -> error_stack::Result<Self, Error> {
        let mut exprs = ExprPattern::default();
        exprs.add_var(*INPUT_VAR)?;
        Ok(exprs)
    }

    /// Create an expr pattern containing the given arguments.
    ///
    /// Returns the resulting pattern as well as the IDs of each of the arguments.
    pub fn new_instruction(
        name: &'static str,
        literal_args: smallvec::SmallVec<[ScalarValue; 2]>,
        args: Vec<ExprPattern>,
        data_type: arrow_schema::DataType,
    ) -> error_stack::Result<ExprPattern, Error> {
        // NOTE: This adds the pattern for each argument to an `EGraph`
        // to add an instruction. This may be overkill, but does simplify
        // (a) de-duplicating expressions that appear in multiple arguments
        // (b) managing things like "all of these arguments should have a
        // single input".
        //
        // If the use of the EGraph and extractor proves to be too expensive
        // we could do this "combine while de-duplicating" ourselves.
        let mut graph = egg::EGraph::<egg::ENodeOrVar<ExprLang>, ()>::default();
        let mut arg_ids = SmallVec::with_capacity(args.len());
        for arg in args {
            let id = graph.add_expr(&arg.expr);
            arg_ids.push(id);
        }

        // We can only extract a single expression, so we create one.
        // This is why we need to know the instruction to create, rather than
        // just returning the resulting `egg::Id` for each argument.
        let output = graph.add(egg::ENodeOrVar::ENode(ExprLang {
            name,
            literal_args,
            args: arg_ids,
            result_type: data_type,
        }));

        let cost_function = egg::AstSize;
        let extractor = egg::Extractor::new(&graph, cost_function);
        let (_best_cost, expr) = extractor.find_best(output);

        Ok(ExprPattern { expr })
    }

    /// Instantiate the pattern.
    ///
    /// Replaces `?input` with the `input` instruction.
    pub fn instantiate(
        &self,
        input_type: arrow_schema::DataType,
    ) -> error_stack::Result<ExprVec, Error> {
        // Note: Instead of instantiating the pattern ourselves (replacing `?input` with the
        // input expression) we instead make an `EGraph`, add the input expression, and then
        // instantiate the pattern into that.
        //
        // This lets us extract the *best* (shortest) expression, rather than copying all of
        // the pattern. One nice thing about this is that the `EGraph` will de-duplicate
        // equivalent operations, etc.
        let mut graph = egg::EGraph::<ExprLang, ()>::default();

        let input_id = graph.add(ExprLang {
            name: "input",
            literal_args: smallvec![],
            args: smallvec![],
            result_type: input_type,
        });
        let mut subst = egg::Subst::with_capacity(1);
        subst.insert(*INPUT_VAR, input_id);

        let result = graph.add_instantiation(&self.expr, &subst);

        let cost_function = egg::AstSize;
        let extractor = egg::Extractor::new(&graph, cost_function);
        let (_best_cost, expr) = extractor.find_best(result);

        Ok(ExprVec { expr })
    }

    pub fn len(&self) -> usize {
        self.expr.as_ref().len()
    }

    /// Return true if this pattern just returns `?input`.
    ///
    /// This is used to identify expression patterns that "just pass the value through".
    /// For instance, a projection step with the `identity` pattern is a noop and can
    /// be removed.
    pub fn is_identity(&self) -> bool {
        // TODO: We may want to make this more intelligent and detect cases where
        // the expression is *equivalent* to the identity. But for now, we think
        // we can treat that as an optimization performed by a later pass.
        let instructions = self.expr.as_ref();
        instructions.len() == 1 && instructions[0] == egg::ENodeOrVar::Var(*INPUT_VAR)
    }

    /// Return the `egg::Id` corresponding to the last expression.
    pub fn last_value(&self) -> egg::Id {
        egg::Id::from(self.expr.as_ref().len() - 1)
    }

    pub fn last(&self) -> &egg::ENodeOrVar<ExprLang> {
        self.expr.as_ref().last().expect("non empty")
    }

    /// Add a physical expression.
    ///
    /// Args:
    /// - name: The name of the operation to apply.
    /// - literal_args: Literal arguments to the physical expression.
    /// - args: The actual arguments to use.
    ///
    /// Returns the `egg::Id` referencing the expression.
    pub fn add_instruction(
        &mut self,
        name: &'static str,
        literal_args: smallvec::SmallVec<[ScalarValue; 2]>,
        args: smallvec::SmallVec<[egg::Id; 2]>,
        data_type: arrow_schema::DataType,
    ) -> error_stack::Result<egg::Id, Error> {
        let expr = self.expr.add(egg::ENodeOrVar::ENode(ExprLang {
            name,
            literal_args,
            args,
            result_type: data_type,
        }));

        Ok(expr)
    }

    /// Add a variable to the expression.
    pub fn add_var(&mut self, var: egg::Var) -> error_stack::Result<egg::Id, Error> {
        Ok(self.expr.add(egg::ENodeOrVar::Var(var)))
    }

    /// Add the given pattern to this pattern, applying the substitution.
    pub fn add_pattern(
        &mut self,
        pattern: &ExprPattern,
        subst: &egg::Subst,
    ) -> error_stack::Result<egg::Id, Error> {
        let mut new_ids = Vec::with_capacity(pattern.len());
        for expr in pattern.expr.as_ref() {
            let new_id = match expr {
                egg::ENodeOrVar::Var(var) => match subst.get(*var) {
                    Some(existing_id) => *existing_id,
                    None => self.add_var(*var)?,
                },
                egg::ENodeOrVar::ENode(node) => {
                    let mut node = node.clone();
                    node.args = node
                        .args
                        .into_iter()
                        .map(|arg| new_ids[usize::from(arg)])
                        .collect();
                    self.expr.add(egg::ENodeOrVar::ENode(node))
                }
            };
            new_ids.push(new_id);
        }
        Ok(*new_ids.last().unwrap())
    }
}
