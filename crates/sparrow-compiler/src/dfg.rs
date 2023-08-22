//! An implementation of DFG construction and optimization using E-graphs.
//!
//! The E-graph library operates by providing a [hash-consed](https://en.wikipedia.org/wiki/Hash_consing)
//! representation. This ensures that two operations applied to the same
//! arguments produces the same node. The hash-consing is performed on the
//! [`DfgLang`] based on the [`StepKind`] and the [`DfgLang::children`].
//!
//! The key feature of the E-graph library is discovering "equivalence" between
//! nodes. It does this using a set of rewrite rules that indicate certain
//! patterns in the DFG are equivalent to other patterns. After applying all of
//! the rewrites to reach "saturation" (no new nodes or equivalences are
//! discovered) it can extract the simplest expression (measured by number of
//! nodes). This makes it easy to add new arithmetic equivalences and optimize
//! based on them, as well as adjust the cost of specific operations
//! to get a "cost-aware" optimizer.
//!
//! The E-graph library also allows for analysis information to be associated
//! with each node when created. The analysis data is merged when nodes are
//! found to be equivalent. We use this for holding the type and partitioning
//! information of DFG nodes.
mod analysis;
mod const_eval;
mod dfg_to_dot;
mod expr;
mod language;
mod pattern;
pub mod simplification;
mod step_kind;
mod useless_transforms;

use std::sync::Arc;

pub(crate) use analysis::*;
use anyhow::Context;
use egg::{AstSize, Extractor, Id, Language};
use hashbrown::HashMap;
use itertools::{izip, Itertools};
pub(crate) use language::ChildrenVec;
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_instructions::{InstKind, InstOp, Udf};
use sparrow_syntax::{FenlType, Location};
pub(crate) use step_kind::*;
type DfgGraph = egg::EGraph<language::DfgLang, analysis::DfgAnalysis>;
pub(super) use expr::DfgExpr;
pub(crate) use pattern::*;
use smallvec::smallvec;
use tracing::{info, info_span};
pub use useless_transforms::*;

use crate::ast_to_dfg::AstDfg;
use crate::dfg::language::DfgLang;
use crate::env::Env;
use crate::nearest_matches::NearestMatches;
use crate::time_domain::TimeDomain;
use crate::{AstDfgRef, CompilerOptions};

#[derive(Debug)]
/// A wrapper around the DFG construction / manipulation functions.
pub struct Dfg {
    /// The DFG being built/manipulated.
    graph: DfgGraph,
    /// A mapping from identifiers to corresponding DFG nodes.
    env: Env<String, AstDfgRef>,
    /// String literal IDs (used for interning, and to avoid copying).
    string_literals: HashMap<String, Id>,
    /// Reference to a shared error node.
    ///
    /// Allows shallow cloning of the reference, rather than having to create
    /// a new error node each time.
    error_node: AstDfgRef,
    /// Id of the empty operation.
    empty_operation: Id,
}

impl Default for Dfg {
    fn default() -> Self {
        let mut graph = DfgGraph::default();
        let env = Env::default();
        let string_literals = HashMap::default();

        // Preemptively create a single error node, allowing for shallow
        // clones of the reference.
        let error_id = graph.add(DfgLang::new(StepKind::Error, smallvec![]));
        let error_node = Arc::new(AstDfg::new(
            error_id,
            error_id,
            FenlType::Error,
            None,
            TimeDomain::error(),
            Location::internal_str("error"),
            None,
        ));

        let empty_operation = graph.add(DfgLang::new(
            StepKind::Operation(Operation::Empty),
            smallvec![],
        ));

        Self {
            graph,
            env,
            string_literals,
            error_node,
            empty_operation,
        }
    }
}

impl Dfg {
    pub fn add_literal(&mut self, literal: impl Into<ScalarValue>) -> anyhow::Result<Id> {
        let literal = literal.into();
        if let ScalarValue::Utf8(Some(literal)) = literal {
            self.add_string_literal(&literal)
        } else {
            self.add_expression(Expression::Literal(literal.clone()), smallvec![])
        }
    }

    /// Add an instruction node to the DFG.
    pub(super) fn add_instruction(
        &mut self,
        instruction: InstOp,
        children: ChildrenVec,
    ) -> anyhow::Result<Id> {
        self.add_expression(Expression::Inst(InstKind::Simple(instruction)), children)
    }

    /// Add a udf node to the DFG.
    pub(super) fn add_udf(
        &mut self,
        udf: Arc<dyn Udf>,
        children: ChildrenVec,
    ) -> anyhow::Result<Id> {
        self.add_expression(Expression::Inst(InstKind::Udf(udf)), children)
    }

    /// Add an expression to the DFG.
    pub(super) fn add_expression(
        &mut self,
        expression: Expression,
        mut children: ChildrenVec,
    ) -> anyhow::Result<Id> {
        // First, determine the operation the expression should be in.
        // This is created by merging the operations from each of the arguments.
        let operation = self.infer_operation(children.iter().copied())?;

        // Second, make sure all of the arguments are in that operation.
        // This may require applying `transform`.
        for arg in children.iter_mut() {
            let arg_operation = self.operation(*arg);
            if arg_operation != operation {
                if let Some(literal) = self.literal(*arg).cloned() {
                    // TODO: Re-evaluate the various `add` methods.
                    // Should the operation be explicit? etc.
                    // Can't use `add_literal` because we *want* the literal in an operation (not
                    // `none`). Can't use `add_expression` because that infers
                    // the operation.
                    *arg = self.add_node(
                        StepKind::Expression(Expression::Literal(literal)),
                        smallvec![operation],
                    )?;
                } else {
                    *arg = self.add_node(StepKind::Transform, smallvec![*arg, operation])?;
                }
            }
            debug_assert_eq!(self.operation(*arg), operation);
        }
        children.push(operation);

        // Create the new node.
        self.add_node(StepKind::Expression(expression), children)
    }

    /// Infer the operation for the a function of the given arguments.
    ///
    /// This may require creating a new operation merging together
    /// the different operations associated with the expression.
    fn infer_operation(&mut self, args: impl IntoIterator<Item = Id>) -> anyhow::Result<Id> {
        // First, determine the operation the expression should be in.
        // This is created by merging the operations from each of the arguments.
        #[allow(clippy::needless_collect)]
        let arg_operations: Vec<_> = args
            .into_iter()
            .map(|arg| self.operation(arg))
            .filter(|arg| *arg != self.empty_operation)
            .unique()
            .collect();

        let mut arg_operations = arg_operations.into_iter();
        // If there are no arguments, this is a constant (literal, late-bound, etc.) so
        // we place it in the constant operation. It should be automatically transformed
        // as needed.
        let mut operation = arg_operations.next().unwrap_or(self.empty_operation);
        for next_operation in arg_operations {
            operation =
                self.add_operation(Operation::MergeJoin, smallvec![operation, next_operation])?;
        }

        Ok(operation)
    }

    // Add an operation to the DFG.
    pub(super) fn add_operation(
        &mut self,
        operation: Operation,
        children: ChildrenVec,
    ) -> anyhow::Result<Id> {
        self.add_node(StepKind::Operation(operation), children)
    }

    /// Add a node to the DFG and return the corresponding `Id`.
    pub(super) fn add_node(&mut self, kind: StepKind, children: ChildrenVec) -> anyhow::Result<Id> {
        if let StepKind::Operation(Operation::MergeJoin) = kind {
            // Aggregations introduce a special case where we need to remove an unnecessary
            // merge if the window argument is null.
            debug_assert!(
                children.len() == 2,
                "merge should have 2 args, saw: {}",
                children.len()
            );
            // If either child is [Operation::Empty], we can ignore the merge and
            // use the non-null operation as the domain.
            let optional_empty = children.iter().position(|c| {
                let kind = self.step_kind(*c);
                matches!(kind, StepKind::Operation(Operation::Empty))
            });
            if let Some(empty) = optional_empty {
                match empty {
                    0 => return Ok(children[1]),
                    1 => return Ok(children[0]),
                    _ => anyhow::bail!("Unexpected index in merge"),
                }
            };
        };

        #[cfg(debug_assertions)]
        self.validate_node(&kind, &children)?;

        Ok(self.graph.add(DfgLang::new(kind, children)))
    }

    #[cfg(debug_assertions)]
    fn validate_node(&self, kind: &StepKind, children: &ChildrenVec) -> anyhow::Result<()> {
        match kind {
            StepKind::Operation(_) => {}
            StepKind::Expression(expr) => {
                // 1. All arguments should be in the same operation as the result.
                let operation = *children.last().with_context(|| {
                    format!("Expressions must have at least one argument, but {kind:?} did not")
                })?;

                for (index, argument) in children.iter().enumerate() {
                    let argument_operation = self.operation(*argument);
                    anyhow::ensure!(
                        argument_operation == operation,
                        "All expression arguments to {:?} must be in the same operation as the \
                         result ({}), but argument {} was in {}",
                        kind,
                        operation,
                        index,
                        argument_operation
                    );
                }

                // 2. The number of args should be correct.
                match expr {
                    Expression::Literal(_) | Expression::LateBound(_) => {
                        anyhow::ensure!(
                            children.len() == 1,
                            "Literal or late-bound value should have one argument (operation), \
                             but was {}",
                            children.len(),
                        );
                    }
                    Expression::Inst(InstKind::Simple(op)) => op
                        .signature()
                        .assert_valid_argument_count(children.len() - 1),
                    Expression::Inst(InstKind::Udf(udf)) => udf
                        .signature()
                        .assert_valid_argument_count(children.len() - 1),
                    Expression::Inst(InstKind::FieldRef) => {
                        anyhow::ensure!(
                            children.len() == 3,
                            "FieldRef should have 3 arguments (operation, base, field name) but \
                             was {}",
                            children.len()
                        )
                    }
                    Expression::Inst(InstKind::Cast(_)) => {
                        anyhow::ensure!(
                            children.len() == 2,
                            "Cast should have 2 arguments (operation, input) but was {}",
                            children.len(),
                        )
                    }
                    Expression::Inst(InstKind::Record) => {
                        anyhow::ensure!(
                            children.len() % 2 == 1,
                            "Record should have an odd number of arguments (operation + \
                             (field_name, value)*) but was {}",
                            children.len()
                        )
                    }
                }
            }
            StepKind::Transform => {
                // 1. The second argument must be an operation.
                // 2. The domain of the first argument must be contained in the
                // second.
            }
            StepKind::Error => {}
            StepKind::Window(_) => {}
        }

        Ok(())
    }

    pub(super) fn add_string_literal(&mut self, literal: &str) -> anyhow::Result<Id> {
        if let Some(id) = self.string_literals.get(literal) {
            Ok(*id)
        } else {
            // In a perfect implementation, we wouldn't need to call `to_owned` at all --
            // we'd be able to reference the entry in the AST as long as we
            // needed. In a slightly less perfect, but still better world, we
            // have a hash map that could reference the string in the scalar
            // value, or some other magic. But... this is easy, and should still
            // be orders of magnitude fewer copies than not handling this -- two
            // copies per unique string, rather than one copy per use of any string.
            let value = ScalarValue::Utf8(Some(literal.to_owned()));
            let id = self.add_expression(Expression::Literal(value), smallvec![])?;
            self.string_literals
                .insert_unique_unchecked(literal.to_owned(), id);
            Ok(id)
        }
    }

    /// Returns an `AstDfgRef` representing an error.
    pub(super) fn error_node(&mut self) -> AstDfgRef {
        self.error_node.clone()
    }

    pub(super) fn enter_env(&mut self) {
        self.env.enter();
    }

    pub(super) fn exit_env(&mut self) {
        self.env.exit()
    }

    /// Add a binding for the given name to the environment.
    ///
    /// Returns `true` if the binding was new, and `false` if it already
    /// existed.
    pub(super) fn bind(&mut self, name: &str, value: AstDfgRef) -> bool {
        self.env.insert(name.to_owned(), value)
    }

    pub(super) fn is_bound(&self, name: &str) -> bool {
        self.env.contains(name)
    }

    /// Return the `AstDfg` node containing the DFG nodes corresponding to the
    /// value and "is_new" for the given reference.
    ///
    /// # Error
    /// Returns an error containing the (up-to-5) nearest matches.
    pub(super) fn get_binding(&self, name: &str) -> Result<AstDfgRef, NearestMatches<&'_ str>> {
        if let Some(found) = self.env.get(name) {
            Ok(found.clone())
        } else {
            Err(crate::nearest_matches::NearestMatches::new_nearest_strs(
                name,
                self.env.keys().map(|s| s.as_str()),
            ))
        }
    }

    /// Runs simplifications on the graph.
    pub fn run_simplifications(&mut self, options: &CompilerOptions) -> anyhow::Result<()> {
        let span = info_span!("Running simplifications");
        let _enter = span.enter();

        let graph = std::mem::take(&mut self.graph);
        let graph = simplification::run_simplifications(graph, options)?;
        self.graph = graph;
        Ok(())
    }

    /// Extract the simplest representation of the node `id` from the graph.
    pub fn extract_simplest(&self, id: Id) -> DfgExpr {
        let span = info_span!("Extracting simplest DFG");
        let _enter = span.enter();

        let extractor = Extractor::new(&self.graph, AstSize);
        let (best_cost, best_expr) = extractor.find_best(id);

        info!(
            "Extracted expression with cost {} and length {}",
            best_cost,
            best_expr.as_ref().len()
        );

        DfgExpr::new(best_expr)
    }

    /// Add the given pattern to the DFG.
    pub fn add_pattern(&mut self, pattern: &DfgPattern, subst: &egg::Subst) -> anyhow::Result<Id> {
        let id = self.add_instantiation(pattern.ast(), subst)?;
        Ok(self.graph.find(id))
    }

    /// Adds the instantiation of a pattern to the DFG.
    ///
    /// Patterns used with this method *should not* include the `operation`
    /// argument to expressions. It will be inferred automatically from the
    /// children (arguments) to each expression.
    ///
    /// This is based on the similar method from the egraph, but we write it
    /// ourselves so that it uses the `add_node` method and applies the
    /// necessary DFG construction checks.
    fn add_instantiation(
        &mut self,
        pat: &egg::PatternAst<DfgLang>,
        subst: &egg::Subst,
    ) -> anyhow::Result<Id> {
        // This method does *not* include the egg logic for adding explanations.
        // We could port that over, but it currently uses internal methods.
        let nodes = pat.as_ref();
        let mut new_ids = Vec::with_capacity(nodes.len());
        for node in nodes {
            match node {
                egg::ENodeOrVar::Var(var) => {
                    let id = subst[*var];
                    new_ids.push(id);
                }
                egg::ENodeOrVar::ENode(node) => {
                    let args: ChildrenVec = node
                        .children()
                        .iter()
                        .map(|id| new_ids[usize::from(*id)])
                        .collect();

                    let next_id = match node.kind() {
                        StepKind::Expression(expression) => {
                            // If the step is an expression, use `add_expression`. This
                            // infers the operation based on the arguments.
                            self.add_expression(expression.clone(), args)?
                        }
                        other => self.add_node(other.clone(), args)?,
                    };
                    new_ids.push(next_id);
                }
            }
        }

        Ok(*new_ids.last().context("expected at least one node")?)
    }

    /// Remove nodes that aren't needed for the `output` from the graph.
    ///
    /// Returns the new ID of the `output`.
    pub fn prune(&mut self, output: Id) -> anyhow::Result<Id> {
        // The implementation is somewhat painful -- we extract a `RecExpr`, and then
        // recreate the EGraph. This has the desired property -- only referenced nodes
        // are extracted. But, it may cause the IDs to change.
        //
        // We don't heavily use the environment or string literals after, but they *may*
        // be used, so we also need to rewrite those IDs.

        let extracted = self.extract_simplest(output);

        // For each node in the expression, determine the canonical IDs in the original
        // graph. We'll use this to create a mapping for converting the existing IDs.

        let old_ids = self
            .graph
            .lookup_expr_ids(extracted.expr())
            .context("expected extracted expression to exist")?;

        // Create the new graph, and determine the new IDs.
        let mut new_graph = DfgGraph::default();
        let new_output = new_graph.add_expr(extracted.expr());
        let new_ids = new_graph
            .lookup_expr_ids(extracted.expr())
            .context("expected new expression to exist")?;

        // Create the mapping from original ID to new ID.
        let mapping: HashMap<_, _> = izip!(old_ids, new_ids).collect();

        // Update the fields
        let old_graph = &self.graph;

        // Values that aren't used will not have a mapping.
        // We convert them to an error. Since they're not used
        // it shouldn't matter.
        let new_error = new_graph.add(DfgLang::new(StepKind::Error, smallvec![]));
        self.string_literals.values_mut().for_each(|field| {
            let old_id = old_graph.find(*field);
            *field = mapping.get(&old_id).copied().unwrap_or(new_error)
        });
        self.env.foreach_value(|node| {
            let old_value = old_graph.find(node.value());
            let new_value = mapping.get(&old_value).copied().unwrap_or(new_error);
            *node.value.lock().unwrap() = new_value;

            let old_is_new = old_graph.find(node.is_new());
            let new_is_new = mapping.get(&old_is_new).copied().unwrap_or(new_error);
            *node.is_new.lock().unwrap() = new_is_new;
        });
        self.graph = new_graph;
        Ok(new_output)
    }

    /// Returns `Some(literal)` if the ID is a literal in the graph.
    pub fn literal(&self, id: Id) -> Option<&ScalarValue> {
        self.graph[id].data.literal_opt()
    }

    /// Returns `Some(str)` if the ID is a string literal in the graph.
    pub fn string_literal(&self, id: Id) -> Option<&str> {
        self.literal(id).and_then(|s| match s {
            ScalarValue::Utf8(s) => s.as_ref().map(|s| s.as_str()),
            ScalarValue::LargeUtf8(s) => s.as_ref().map(|s| s.as_str()),
            _ => None,
        })
    }

    /// Returns the ID of the operation node defining the domain of `id`.
    pub fn operation(&self, id: Id) -> Id {
        self.graph[id].data.operation(id)
    }

    /// Returns the stepkind associated with this `id`.
    fn step_kind(&self, id: Id) -> StepKind {
        self.graph[id].data.kind()
    }

    /// Returns the canonical ID for the given ID.
    #[cfg(test)]
    pub fn find(&self, id: Id) -> Id {
        self.graph.find(id)
    }

    #[cfg(test)]
    pub fn dump_graph(&self) -> impl std::fmt::Debug + '_ {
        self.graph.dump()
    }

    #[cfg(test)]
    pub(crate) fn data(&self, id: Id) -> &DfgAnalysisData {
        &self.graph[id].data
    }
}
