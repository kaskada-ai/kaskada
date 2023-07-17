
use arrow::datatypes::Schema;
use schema_registry_converter::{async_impl::schema_registry::{get_referenced_schema, SrSettings, get_schema_by_subject}, schema_registry_common::SubjectNameStrategy};

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

pub async fn get_kafka_schema() -> Result<String, Error> {
    let sr_settings = SrSettings::new(String::from("http://localhost:8085"));
    let subject_name_strategy = SubjectNameStrategy::TopicNameStrategy(String::from("my-topic"), false);
    let schema = get_schema_by_subject(&sr_settings, &subject_name_strategy)
        .await
        .into_report()
        .change_context(Error::SchemaRequest)?;
    Ok(schema.schema)
}

#[cfg(test)]
mod tests {
    use super::get_kafka_schema;

    #[tokio::test]
    async fn test() {
        let result = get_kafka_schema().await.unwrap();
        assert_eq!("", result);
    }
}