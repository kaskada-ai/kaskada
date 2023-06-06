use arrow::array::ArrowPrimitiveType;
use arrow::datatypes::{Schema, SchemaRef, TimestampMicrosecondType};

use error_stack::{IntoReport, Result, ResultExt};
use std::sync::Arc;

#[derive(Debug, derive_more::Display)]
pub enum Error {
    #[allow(dead_code)]
    AvroNotEnabled,
    AvroSchemaConversion,
    SchemaRequest,
    UnsupportedSchema,
}
impl error_stack::Context for Error {}

pub fn get_output_schema(schema: SchemaRef) -> Result<SchemaRef, Error> {
    let fields = schema.fields();

    // Avro does not support certain types that we use internally for the implicit columns.
    // For the `_time` (timestamp_ns) column, we cast to timestamp_us, sacrificing nano precision.
    // (Optionally, we could provide a separate column composed of the nanos as a separate i64).
    // The `_subsort` and `_key_hash` columns are dropped.
    let time_us = Arc::new(arrow::datatypes::Field::new(
        "_time",
        TimestampMicrosecondType::DATA_TYPE,
        false,
    ));

    let mut new_fields = vec![time_us];
    fields
        .iter()
        .skip(3) // Skip the `_time`, `_subsort`, and `_key_hash` fields
        .for_each(|f| new_fields.push(f.clone()));

    Ok(Arc::new(Schema::new(new_fields)))
}

#[cfg(not(feature = "avro"))]
fn format_schema(_schema: SchemaRef) -> Result<String, Error> {
    error_stack::bail!(Error::AvroNotEnabled)
}

#[cfg(feature = "avro")]
pub fn format_schema(schema: SchemaRef) -> Result<String, Error> {
    let avro_schema = sparrow_arrow::avro::to_avro_schema(&schema)
        .change_context(Error::AvroSchemaConversion)
        .attach_printable_lazy(|| {
            format!("failed to convert arrow schema to avro schema {schema}")
        })?;
    serde_json::to_string(&avro_schema)
        .into_report()
        .change_context(Error::AvroSchemaConversion)
        .attach_printable_lazy(|| {
            format!("failed to serialize avro schema to json string: {avro_schema:?}")
        })
}

fn schema_from_formatted(formatted_schema: &str) -> Result<Schema, Error> {
    // construct an avro_schema::schema::Schema from the json formatted schema
    let avro_schema: avro_schema::schema::Schema = serde_json::from_str(formatted_schema)
        .into_report()
        .change_context(Error::AvroSchemaConversion)
        .attach_printable_lazy(|| {
            format!("failed to deserialize avro schema from json string: {formatted_schema}")
        })?;
    // convert to sparrow format
    let sparrow_schema = sparrow_arrow::avro::from_avro_schema(&avro_schema)
        .change_context(Error::AvroSchemaConversion)
        .attach_printable_lazy(|| format!("from_avro_schema({:?}) failed", &avro_schema))?;
    Ok(sparrow_schema)
}

#[derive(serde::Deserialize, Debug)]
struct SchemaResponse {
    #[allow(dead_code)]
    version: u32,
    #[serde(rename = "type")]
    schema_type: String,
    #[allow(dead_code)]
    timestamp: u64,
    data: String,
    #[allow(dead_code)]
    properties: serde_json::Value,
}

// auth_params looks like
// token:xxx
pub fn pulsar_auth_token(auth_params: &str) -> Result<&str, Error> {
    // split on the first colon
    let mut parts = auth_params.splitn(2, ':');
    // verify first part is "token"
    let auth_type = parts.next();
    let Some(auth_type) = auth_type else {
        return Err(Error::SchemaRequest.into()).attach_printable("missing auth_type");
    };
    if auth_type != "token" {
        return Err(Error::SchemaRequest.into())
            .attach_printable_lazy(|| format!("auth type {:?}", auth_type));
    }
    let auth_token = parts.next();
    let Some(auth_token) = auth_token else {
        return Err(Error::SchemaRequest.into()).attach_printable("missing auth token");
    };
    Ok(auth_token)
}

// retrieve the schema for the given topic via the admin api, with a REST call.
// we can't use the pulsar client because the schema is not exposed there in the Rust client.
pub async fn get_pulsar_schema(
    admin_service_url: &str,
    tenant: &str,
    namespace: &str,
    topic: &str,
    auth_params: &str,
) -> Result<Schema, Error> {
    let url = format!(
        "{}/admin/v2/schemas/{}/{}/{}/schema",
        admin_service_url, tenant, namespace, topic
    );
    tracing::debug!("requesting schema from {}", url);
    let client = reqwest::Client::new();
    let mut request_builder = client.get(&url);

    // Auth is generally recommended, but some local builds may be run
    // without auth for exploration/testing purposes.
    if !auth_params.is_empty() {
        request_builder = request_builder.header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", pulsar_auth_token(auth_params)?),
        )
    }

    let text = request_builder
        .send()
        .await
        .into_report()
        .change_context(Error::SchemaRequest)?
        .text()
        .await
        .into_report()
        .change_context(Error::SchemaRequest)?;

    let schema_response: SchemaResponse = serde_json::from_str(&text)
        .into_report()
        .change_context(Error::AvroSchemaConversion)
        .attach_printable_lazy(|| format!("from_str({:?}) failed", &text))?;
    if schema_response.schema_type != "AVRO" {
        return Err(Error::UnsupportedSchema.into())
            .attach_printable_lazy(|| format!("schema type {:?}", schema_response.schema_type));
    }
    let schema = schema_from_formatted(&schema_response.data)?;

    Ok(schema)
}
