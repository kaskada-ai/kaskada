//! e2e tests for the collection operators.

use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::{DataFixture, QueryFixture};

/// Fixture for testing collection operations.
async fn collection_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_files(
            TableConfig::new_with_table_source("Input", &Uuid::new_v4(), "time", None, "name", ""),
            &["parquet/data_with_map.parquet"],
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_get() {
    insta::assert_snapshot!(QueryFixture::new("{ get: Input.map | get(\"age\")} }").run_to_csv(&collection_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,m,n,eq
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,5,21,0
    1996-12-20T00:39:58.000000000,9223372036854775808,11753611437813598533,B,24,14,0
    1996-12-20T00:39:59.000000000,9223372036854775808,3650215962958587783,A,17,17,1
    1996-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,,20,
    1996-12-20T00:40:01.000000000,9223372036854775808,3650215962958587783,A,12,,
    1996-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,,,
    "###);
}
