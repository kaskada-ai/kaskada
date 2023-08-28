use std::sync::Arc;

use anyhow::Context;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use hashbrown::HashMap;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::{ComputePlan, OperationPlan};
use sparrow_compiler::DataContext;

use crate::execute::operation::{OperationContext, OperationExecutor};
use crate::key_hash_inverse::ThreadSafeKeyHashInverse;
use crate::stores::ObjectStoreRegistry;
use crate::Batch;

pub(super) async fn batches_to_csv(
    mut receiver: tokio::sync::mpsc::Receiver<Batch>,
) -> anyhow::Result<String> {
    let mut csv_string = Vec::new();
    let mut writer = arrow::csv::Writer::new(&mut csv_string);
    while let Some(batch) = receiver.recv().await {
        writer.write(&batch.data).context("write batch")?;
    }
    std::mem::drop(writer);
    String::from_utf8(csv_string).context("writing batches")
}

pub(super) async fn batches_to_json(
    mut receiver: tokio::sync::mpsc::Receiver<Batch>,
) -> anyhow::Result<String> {
    let mut json_string = Vec::new();
    let mut writer = arrow::json::LineDelimitedWriter::new(&mut json_string);
    while let Some(batch) = receiver.recv().await {
        writer.write(&batch.data).context("write batch")?;
    }
    writer.finish()?;
    String::from_utf8(json_string).context("writing batches")
}

/// Parse a `RecordBatch` from the given CSV string.
///
/// This expects the first 3 columns to be the "key columns"
/// (_time, _subsort, _key_hash). As part of that, it will attempt
/// to cast these 3 columns if needed.
///
/// The `column_types` (for the non-key-columns) may be specified.
/// If they are, then all data columns (excluding the first three)
/// must be specified. The column will be cast as needed. This is
/// useful when a test requires the input be a specific type such
/// as `u64` which the Arrow CSV parser would not produce by
/// default.
pub(super) fn batch_from_csv(
    csv: &str,
    column_types: Option<Vec<DataType>>,
) -> anyhow::Result<RecordBatch> {
    // Trim trailing/leading whitespace on each line.
    let csv = csv.lines().map(|line| line.trim()).join("\n");

    let cursor = std::io::Cursor::new(csv.as_bytes());

    let (read_schema, _) = arrow::csv::reader::Format::default()
        .with_header(true)
        .infer_schema(cursor.clone(), None)
        .context("no schema")?;

    // Convert the key columns.
    let mut fields = vec![
        Arc::new(Field::new(
            "_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )),
        Arc::new(Field::new("_subsort", DataType::UInt64, false)),
        Arc::new(Field::new("_key_hash", DataType::UInt64, false)),
    ];

    if let Some(column_types) = column_types {
        anyhow::ensure!(read_schema.fields()[3..].len() == column_types.len());
        fields.extend(read_schema.fields()[3..].iter().zip(column_types).map(
            |(field, data_type)| {
                if field.data_type() == &data_type && field.is_nullable() {
                    field.clone()
                } else {
                    Arc::new(Field::new(field.name(), data_type, true))
                }
            },
        ));
    } else {
        fields.extend_from_slice(&read_schema.fields()[3..]);
    };
    let schema = Arc::new(Schema::new(fields));

    let reader = arrow::csv::ReaderBuilder::new(schema.clone())
        .has_header(true)
        .build(cursor)?;
    let batches: Vec<_> = reader.try_collect()?;
    let batch =
        arrow::compute::concat_batches(&schema, &batches).context("concatenate read batches")?;
    Ok(batch)
}

/// Parse a `RecordBatch` from the given JSON string.
///
/// This expects the first 3 columns to be the "key columns"
/// (_time, _subsort, _key_hash). As part of that, it will attempt
/// to cast these 3 columns if needed.
///
/// The `column_types` (for the non-key-columns) may be specified.
/// If they are, then all data columns (excluding the first three)
/// must be specified. The column will be cast as needed. This is
/// useful when a test requires the input be a specific type such
/// as `u64` which the Arrow CSV parser would not produce by
/// default.
pub(super) fn batch_from_json(
    json: &str,
    column_types: Vec<DataType>,
) -> anyhow::Result<RecordBatch> {
    // Trim trailing/leading whitespace on each line.
    let json = json.lines().map(|line| line.trim()).join("\n");

    // Determine the schema.
    let schema = {
        let mut fields = vec![
            Field::new(
                "_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("_subsort", DataType::UInt64, false),
            Field::new("_key_hash", DataType::UInt64, false),
        ];

        fields.extend(
            column_types
                .into_iter()
                .enumerate()
                .map(|(index, data_type)| Field::new(format!("e{index}"), data_type, true)),
        );

        Arc::new(Schema::new(fields))
    };

    // Create the reader
    let reader = std::io::Cursor::new(json.as_bytes());
    let reader = arrow::json::ReaderBuilder::new(schema).build(reader)?;

    // Read all the batches and concatenate them.
    let batches: Vec<_> = reader.try_collect()?;
    let read_schema = batches.get(0).context("no batches read")?.schema();
    let batch = arrow::compute::concat_batches(&read_schema, &batches)
        .context("concatenate read batches")?;

    Ok(batch)
}

/// Run an operation on the given inputs (each being a CSV string)
pub(super) async fn run_operation(
    input_batches: Vec<RecordBatch>,
    plan: OperationPlan,
) -> anyhow::Result<String> {
    let mut inputs = Vec::with_capacity(input_batches.len());
    for input in input_batches {
        let input = Batch::try_new_from_batch(input)?;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        sender.send(input).await.context("populate input channel")?;
        inputs.push(receiver);
    }

    let (max_event_tx, mut max_event_rx) = tokio::sync::mpsc::unbounded_channel();
    let (sender, receiver) = tokio::sync::mpsc::channel(10);
    let mut executor = OperationExecutor::new(plan.clone());
    executor.add_consumer(sender);

    // Channel for the output stats.
    let (progress_updates_tx, _) = tokio::sync::mpsc::channel(29);

    let key_hash_inverse = Arc::new(ThreadSafeKeyHashInverse::from_data_type(&DataType::Utf8));

    let mut context = OperationContext {
        plan: ComputePlan {
            operations: vec![plan],
            ..ComputePlan::default()
        },
        object_stores: Arc::new(ObjectStoreRegistry::default()),
        data_context: DataContext::default(),
        compute_store: None,
        key_hash_inverse,
        max_event_in_snapshot: None,
        progress_updates_tx,
        output_at_time: None,
        bounded_lateness_ns: None,
        materialize: false,
        udfs: HashMap::new(),
    };
    executor
        .execute(
            0,
            &mut context,
            inputs,
            max_event_tx,
            &Default::default(),
            None,
        )
        .await
        .unwrap()
        .await
        .unwrap();

    max_event_rx.close();

    batches_to_csv(receiver).await
}

/// Run an operation on the given inputs (each being a JSON string)
pub(super) async fn run_operation_json(
    input_batches: Vec<RecordBatch>,
    plan: OperationPlan,
) -> anyhow::Result<String> {
    let mut inputs = Vec::with_capacity(input_batches.len());
    for input in input_batches {
        let input = Batch::try_new_from_batch(input)?;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        sender.send(input).await.context("populate input channel")?;
        inputs.push(receiver);
    }

    let key_hash_inverse = Arc::new(ThreadSafeKeyHashInverse::from_data_type(&DataType::Utf8));

    let (max_event_tx, mut max_event_rx) = tokio::sync::mpsc::unbounded_channel();

    let (sender, receiver) = tokio::sync::mpsc::channel(10);
    let mut executor = OperationExecutor::new(plan.clone());
    executor.add_consumer(sender);

    // Channel for the output stats.
    let (progress_updates_tx, _) = tokio::sync::mpsc::channel(29);
    let mut context = OperationContext {
        plan: ComputePlan {
            operations: vec![plan],
            ..ComputePlan::default()
        },
        object_stores: Arc::new(ObjectStoreRegistry::default()),
        data_context: DataContext::default(),
        compute_store: None,
        key_hash_inverse,
        max_event_in_snapshot: None,
        progress_updates_tx,
        output_at_time: None,
        bounded_lateness_ns: None,
        materialize: false,
        udfs: HashMap::new(),
    };
    executor
        .execute(
            0,
            &mut context,
            inputs,
            max_event_tx,
            &Default::default(),
            None,
        )
        .await
        .unwrap()
        .await
        .unwrap();

    max_event_rx.close();

    batches_to_json(receiver).await
}
