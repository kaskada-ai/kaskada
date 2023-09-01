use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use enum_map::EnumMap;
use hashbrown::HashMap;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::expression_plan::Operator;
use sparrow_api::kaskada::v1alpha::{ExpressionPlan, LateBoundValue, OperationInputRef};
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_instructions::{
    create_evaluator, ColumnarValue, ComputeStore, Evaluator, GroupingIndices, InstKind, InstOp,
    RuntimeInfo, StaticArg, StaticInfo, StoreKey,
};
use sparrow_instructions::{Udf, ValueRef};
use uuid::Uuid;

use crate::execute::operation::InputBatch;
use crate::Batch;

/// Executes the expressions within an operation.
///
/// Each operation produces a stream of inputs (`BoxedInputBatch`).
/// Expressions create columns from the input and by evaluating instructions
/// against existing columns.
pub(super) struct ExpressionExecutor {
    operation_label: &'static str,
    input_columns: Vec<InputColumn>,
    expression_evaluators: Vec<Box<dyn Evaluator>>,
    /// Expressions which are part of the output schema.
    output_columns: Vec<ValueRef>,
    schema: SchemaRef,
}

/// Information about an input column needed by the executor.
///
/// These correspond to the inputs needed within the operation that
/// are received from other operations.
#[derive(Debug)]
pub(super) struct InputColumn {
    pub input_ref: OperationInputRef,
    pub data_type: DataType,
}

impl ExpressionExecutor {
    /// Create an `ExpressionExecutor` for the given expressions.
    pub fn try_new(
        operation_label: &'static str,
        expressions: Vec<ExpressionPlan>,
        late_bindings: &EnumMap<LateBoundValue, Option<ScalarValue>>,
        udfs: &HashMap<Uuid, Arc<dyn Udf>>,
    ) -> anyhow::Result<Self> {
        let mut input_columns = Vec::new();

        // HACK: This could likely be written more cleanly, but is currently in this
        // form because it converts the "new expression plan" to the old
        // ValueRef based pattern so that we can instantiate and execute
        // evaluators the same way. This should likely be changed once we're
        // more fully on the new operation based plans.

        // Expression implementations for the computed columns.
        // Note that each expression will produce AT MOST one entry.
        // Literals and late bound values *do not* add to expression impls.
        let mut expression_evaluators = Vec::with_capacity(expressions.len());
        let mut output_columns = Vec::new();

        // Static information about how to reference the value of this expression.
        //
        // Each expression is converted to a `StaticArg` which is used for wiring
        // up expressions that use this as an input. The `StaticArg` indicates whether
        // this is a literal or a computed column.
        let mut static_args: Vec<StaticArg> = Vec::with_capacity(expressions.len());

        let schema = output_schema(&expressions)?;

        for expression in expressions {
            let args: Vec<_> = expression
                .arguments
                .into_iter()
                // TODO: If we stick with the use of `StaticArg` for creating evaluators
                // we could avoid the clone by just passing a `&[&StaticArg]` to the
                // static info. Deferred during introduction of new plan/execution since
                // it would require changing many of the evaluators.
                .map(|index| static_args[index as usize].clone())
                .collect();

            let data_type = DataType::try_from(
                expression
                    .result_type
                    .as_ref()
                    .context("missing result type")?,
            )?;

            let value_ref = match expression.operator.context("missing expression operator")? {
                Operator::Instruction(inst) => {
                    // TODO: This could probably be cleaned up, possibly by eliminating
                    // `inst kind` entirely.
                    let inst_kind = if inst == "field_ref" {
                        InstKind::FieldRef
                    } else if inst == "record" {
                        InstKind::Record
                    } else if inst == "cast" {
                        InstKind::Cast(data_type.clone())
                    } else {
                        // This assumes we'll never have an InstOp function name that
                        // matches a uuid, which should be safe.
                        if let Ok(uuid) = Uuid::from_str(&inst) {
                            let udf = udfs.get(&uuid).ok_or(anyhow::anyhow!("expected udf"))?;
                            InstKind::Udf(udf.clone())
                        } else {
                            let inst_op = InstOp::from_str(&inst)?;
                            InstKind::Simple(inst_op)
                        }
                    };

                    let static_info = StaticInfo::new(&inst_kind, args, &data_type);
                    let evaluator = create_evaluator(static_info)?;

                    let index = expression_evaluators.len();
                    expression_evaluators.push(evaluator);
                    ValueRef::Inst(index as u32)
                }

                Operator::Input(input_ref) => {
                    let input_index = input_columns.len();
                    input_columns.push(InputColumn {
                        input_ref,
                        data_type: data_type.clone(),
                    });
                    ValueRef::Input(input_index as u32)
                }
                Operator::Literal(literal) => {
                    // This is a bit tricky. Literals (and late bound values) don't create
                    // expressions. This is beacuse each expression creates a column of N
                    // rows. We *could* check to see if any of the evaluators (or the output)
                    // need an instantiated column, and treat this literal expression
                    // differently based on that. That would let us avoid re-instantiating
                    // the same N-row literal column multiple times.

                    let scalar_value: ScalarValue = (literal).try_into_scalar_value(&data_type)?;

                    // HACK: The scalar value protobuf encoding isn't great.
                    // `ScalarValue::Int64(None)` encodes to `"null"` which parses to
                    // `ScalarValue::Null`. As such, the data type *may be incorrect*.
                    //
                    // A better strategy would be to encode a scalar value as a protobuf
                    // with all the cases spelled out, or as a string + a datatype, so that
                    // the data type can be used to interpret the string.
                    let scalar_value = if scalar_value.data_type() == data_type {
                        scalar_value
                    } else if scalar_value.data_type() == DataType::Null {
                        ScalarValue::try_new_null(&data_type)?
                    } else {
                        anyhow::bail!(
                            "Literal data type was {:?} but expected {:?}. Literal was {:?}",
                            scalar_value.data_type(),
                            data_type,
                            literal
                        )
                    };

                    ValueRef::Literal(scalar_value)
                }
                Operator::LateBound(late_bound) => {
                    let late_bound =
                        LateBoundValue::from_i32(late_bound).context("late bound value")?;
                    let literal = late_bindings[late_bound]
                        .as_ref()
                        .with_context(|| format!("missing late bound value {late_bound:?}"))?
                        .clone();

                    anyhow::ensure!(
                        literal.data_type() == data_type,
                        "Literal data type was {:?} but expected {:?}",
                        literal.data_type(),
                        data_type
                    );

                    // Treat late bindings as literals.
                    // This suggests we can actually eliminate that level of
                    // special case from ValueRef.
                    ValueRef::Literal(literal)
                }
            };

            if expression.output {
                output_columns.push(value_ref.clone());
            }

            static_args.push(StaticArg {
                value_ref,
                data_type,
            });
        }

        Ok(Self {
            operation_label,
            input_columns,
            expression_evaluators,
            output_columns,
            schema,
        })
    }

    pub fn restore(
        &mut self,
        operation_index: u8,
        compute_store: &ComputeStore,
    ) -> anyhow::Result<()> {
        for (inst_index, evaluator) in self.expression_evaluators.iter_mut().enumerate() {
            if let Some(state) = evaluator.state_token_mut() {
                let key = StoreKey::new_accumulator(operation_index, inst_index as u32);
                state.restore(&key, compute_store)?;
            }
        }
        Ok(())
    }

    pub fn store(&self, operation_index: u8, compute_store: &ComputeStore) -> anyhow::Result<()> {
        for (inst_index, evaluator) in self.expression_evaluators.iter().enumerate() {
            if let Some(state) = evaluator.state_token() {
                let key = StoreKey::new_accumulator(operation_index, inst_index as u32);
                state.store(&key, compute_store)?;
            }
        }
        Ok(())
    }

    pub fn input_columns(&self) -> &[InputColumn] {
        &self.input_columns
    }

    /// Execute the expressions on the given input batch.
    pub fn execute(&mut self, input_batch: InputBatch) -> anyhow::Result<Batch> {
        anyhow::ensure!(self.input_columns().len() == input_batch.input_columns.len());

        let mut work_area = WorkArea::new(
            input_batch.time,
            input_batch.subsort,
            input_batch.key_hash,
            input_batch.input_columns,
            self.expression_evaluators.len(),
            input_batch.grouping,
        );

        // Run the expression to compute additional columns.
        for evaluator in self.expression_evaluators.iter_mut() {
            let column = evaluator.evaluate(&work_area)?;
            work_area.push_computed_column(column);
        }

        // Create the output columns.
        let mut output_columns = Vec::with_capacity(self.schema.fields().len());
        output_columns.extend_from_slice(&[
            work_area.time.clone(),
            work_area.subsort.clone(),
            work_area.key_hash.clone(),
        ]);
        for output_column in self.output_columns.iter() {
            output_columns.push(work_area.value(output_column)?.array_ref()?);
        }

        let batch =
            RecordBatch::try_new(self.schema.clone(), output_columns).with_context(|| {
                format!("creating result record batch for {}", self.operation_label)
            })?;
        Batch::try_new_with_bounds(batch, input_batch.lower_bound, input_batch.upper_bound)
            .with_context(|| format!("creating result batch for {}", self.operation_label))
    }
}

/// Wrapper around the columns produced during execution.
struct WorkArea {
    time: ArrayRef,
    subsort: ArrayRef,
    key_hash: ArrayRef,
    /// Columns from the input to the operation.
    input_columns: Vec<ArrayRef>,
    /// Columns computed by expressions within the operation.
    computed_columns: Vec<ArrayRef>,
    /// An assignment of each key to a unique index in the range
    /// `[0...num_distinct_keys)`.
    ///
    /// OPTIMIZATION: This requires storing an array of entity indices that is
    /// used exclusively by aggregations. If this operation does not contain
    /// an aggregation, we can reduce memory pressure by making this
    /// optional.
    grouping: GroupingIndices,
}

impl WorkArea {
    fn new(
        time: ArrayRef,
        subsort: ArrayRef,
        key_hash: ArrayRef,
        input_columns: Vec<ArrayRef>,
        computed_capacity: usize,
        grouping: GroupingIndices,
    ) -> Self {
        Self {
            time,
            subsort,
            key_hash,
            input_columns,
            computed_columns: Vec::with_capacity(computed_capacity),
            grouping,
        }
    }

    fn push_computed_column(&mut self, column: ArrayRef) {
        self.computed_columns.push(column)
    }
}

// TODO: The `RuntimeInfo` was part of the "old" way of accessing things like
// grouping, time, etc. We may be able to do something simpler using the
// information from the operation input.
impl RuntimeInfo for WorkArea {
    fn value(&self, arg: &ValueRef) -> anyhow::Result<ColumnarValue> {
        match arg {
            ValueRef::Input(index) => Ok(ColumnarValue::Array(
                self.input_columns[*index as usize].clone(),
            )),
            ValueRef::Inst(index) => Ok(ColumnarValue::Array(
                self.computed_columns[*index as usize].clone(),
            )),
            ValueRef::Literal(literal) => Ok(ColumnarValue::Literal {
                rows: self.num_rows(),
                literal: literal.clone(),
            }),
            ValueRef::Tick(_) => Err(anyhow::anyhow!("Unexpected ValueRef::Tick")),
        }
    }

    fn grouping(&self) -> &GroupingIndices {
        &self.grouping
    }

    fn time_column(&self) -> ColumnarValue {
        ColumnarValue::Array(self.time.clone())
    }

    fn storage(&self) -> Option<&ComputeStore> {
        todo!()
    }

    fn num_rows(&self) -> usize {
        self.time.len()
    }
}

#[static_init::dynamic]
static KEY_FIELDS: [Field; 3] = [
    Field::new(
        "_time",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    ),
    Field::new("_subsort", DataType::UInt64, false),
    Field::new("_key_hash", DataType::UInt64, false),
];

/// Determine the output schema for based on the expressions.
fn output_schema(expressions: &[ExpressionPlan]) -> anyhow::Result<SchemaRef> {
    let exported_fields = expressions
        .iter()
        .enumerate()
        .filter(|(_, expr)| expr.output)
        .map(|(index, expr)| -> anyhow::Result<_> {
            let data_type = expr
                .result_type
                .as_ref()
                .context("missing result type")?
                .try_into()?;
            Ok(Field::new(format!("e{index}"), data_type, true))
        });

    let fields: Vec<_> = KEY_FIELDS
        .iter()
        .map(|field| Ok(field.clone()))
        .chain(exported_fields)
        .try_collect()?;
    Ok(Arc::new(Schema::new(fields)))
}
