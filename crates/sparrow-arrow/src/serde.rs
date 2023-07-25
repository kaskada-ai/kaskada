//! Provides serde modules for `RecordBatch` and `ArrayRef`

use arrow::error::ArrowError;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use itertools::Itertools;
use std::io::Cursor;

/// Provides serde for `RecordBatch`.
///
/// Example:
///
/// ```rust
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct Foo {
///   #[serde(with = "sparrow_arrow::serde::record_batch")]
///   batch: RecordBatch
/// }
/// ```
pub mod record_batch {
    use arrow::record_batch::RecordBatch;
    use serde::Deserialize;

    pub fn serialize<S>(batch: &RecordBatch, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;
        let bytes = super::encode_batch(batch).map_err(Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<RecordBatch, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let batch = super::decode_batch(bytes).map_err(Error::custom)?;

        Ok(batch)
    }
}

/// Provides serde for `ArrayRef`.
///
/// Example:
///
/// ```rust
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct Foo {
///   #[serde(with = "sparrow_arrow::serde::array_ref")]
///   array: ArrayRef
/// }
/// ```
pub mod array_ref {
    use std::sync::Arc;

    use arrow::array::ArrayRef;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use serde::Deserialize;

    pub fn serialize<S>(array: &ArrayRef, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            array.data_type().clone(),
            true,
        )]));

        let record_batch =
            RecordBatch::try_new(schema, vec![array.clone()]).map_err(Error::custom)?;
        let bytes = super::encode_batch(&record_batch).map_err(Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ArrayRef, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let batch = super::decode_batch(bytes).map_err(Error::custom)?;

        let columns = batch.columns();
        assert_eq!(columns.len(), 1);
        Ok(columns[0].clone())
    }
}

fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>, ArrowError> {
    let c = Cursor::new(Vec::new());

    let mut file_writer = FileWriter::try_new(c, &batch.schema())?;
    file_writer.write(batch)?;
    file_writer.finish()?;

    let c = file_writer.into_inner()?;
    Ok(c.into_inner())
}

fn decode_batch(bytes: Vec<u8>) -> Result<RecordBatch, ArrowError> {
    let c = Cursor::new(bytes);
    let file_reader = FileReader::try_new(c, None)?;
    let schema = file_reader.schema();

    let batches: Vec<_> = file_reader.into_iter().try_collect()?;
    arrow::compute::concat_batches(&schema, &batches)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{MapBuilder, StringBuilder, UInt32Builder};
    use arrow_array::{ArrayRef, UInt32Array};

    #[derive(serde::Serialize, serde::Deserialize)]
    struct ArrayRefTest {
        #[serde(with = "crate::serde::array_ref")]
        array: ArrayRef,
    }

    #[test]
    fn test_array_ref_u32() {
        let original = ArrayRefTest {
            array: Arc::new(UInt32Array::from(vec![0, 1, 2])),
        };
        let copied = round_trip_bincode(&original);
        assert_eq!(&original.array, &copied.array);
    }

    #[test]
    fn test_array_ref_map() {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), UInt32Builder::new());
        builder.keys().append_value("hello");
        builder.values().append_value(5);
        builder.append(true).unwrap();
        let array = builder.finish();

        let original = ArrayRefTest {
            array: Arc::new(array),
        };
        let copied = round_trip_bincode(&original);
        assert_eq!(&original.array, &copied.array);
    }

    fn round_trip_bincode<T: serde::Serialize + serde::de::DeserializeOwned>(t: &T) -> T {
        let serialized = bincode::serialize(t).unwrap();
        bincode::deserialize(&serialized).unwrap()
    }
}
