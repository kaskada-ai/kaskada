use error_stack::ResultExt;
use sparrow_api::kaskada::v1alpha::{slice_plan, PreparedFile, TableConfig};

use crate::prepare::Error;
use crate::read::ParquetFile;
use crate::stores::{ObjectStoreRegistry, ObjectStoreUrl};

pub async fn prepare_parquet<'a>(
    object_stores: &ObjectStoreRegistry,
    source: ObjectStoreUrl,
    config: &'a TableConfig,
    slice: &'a Option<slice_plan::Slice>,
) -> error_stack::Result<Vec<PreparedFile>, Error> {
    let source_file = ParquetFile::try_new(object_stores, url, None)
        .await
        .change_context(Error::CreateReader)?;

    Ok(())
}
