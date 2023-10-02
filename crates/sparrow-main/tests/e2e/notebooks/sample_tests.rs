use indoc::indoc;
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::{DataFixture, QueryFixture};

const SAMPLE_EVENTS_CSV: &str = indoc! {"
    event_at,entity_id,event_name,commit_count
    2022-01-01 12:00:00+00:00,ada,wrote_code,1
    2022-01-01 13:10:00+00:00,ada,wrote_code,1
    2022-01-01 13:20:00+00:00,ada,wrote_code,1
    2022-01-01 14:00:00+00:00,ada,wrote_code,3
    2022-01-01 12:00:00+00:00,brian,data_scienced,1
    2022-01-01 13:20:00+00:00,brian,data_scienced,2
    2022-01-01 13:40:00+00:00,brian,data_scienced,1
    2022-01-01 15:00:00+00:00,brian,data_scienced,1
"};

/// Create a fixture with the sample events csv.
async fn sample_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new_with_table_source(
                "SampleEvents",
                &Uuid::new_v4(),
                "event_at",
                None,
                "entity_id",
                "",
            ),
            SAMPLE_EVENTS_CSV,
        )
        .await
        .unwrap()
}

const SAMPLE_EVENTS: &str = indoc! {"
    let event_count_total = SampleEvents | count()
    let count_hourly = SampleEvents | count(window=since(hourly()))

    let timestamp_continuous = event_count_total | time_of() | last()
    let username_continuous = SampleEvents.entity_id | last()

    in {
        timestamp_continuous,
        username_continuous,
        count_hourly,
        event_count_total,
        event_time_not_continuous: SampleEvents.event_at,
        event_username_not_continuous: SampleEvents.entity_id,
    } | when(hourly())

"};

#[tokio::test]
async fn test_sample_events_to_csv() {
    insta::assert_snapshot!(QueryFixture::new(SAMPLE_EVENTS).run_to_csv(&sample_data_fixture().await).await.unwrap(),
    @r###"
    _time,_subsort,_key_hash,_key,timestamp_continuous,username_continuous,count_hourly,event_count_total,event_time_not_continuous,event_username_not_continuous
    2022-01-01T12:00:00.000000000,18446744073709551615,763888930855861605,brian,2022-01-01T12:00:00.000000000,brian,1,1,,
    2022-01-01T12:00:00.000000000,18446744073709551615,10511513854575835016,ada,2022-01-01T12:00:00.000000000,ada,1,1,,
    2022-01-01T13:00:00.000000000,18446744073709551615,763888930855861605,brian,2022-01-01T12:00:00.000000000,brian,0,1,,
    2022-01-01T13:00:00.000000000,18446744073709551615,10511513854575835016,ada,2022-01-01T12:00:00.000000000,ada,0,1,,
    2022-01-01T14:00:00.000000000,18446744073709551615,763888930855861605,brian,2022-01-01T13:40:00.000000000,brian,2,3,,
    2022-01-01T14:00:00.000000000,18446744073709551615,10511513854575835016,ada,2022-01-01T14:00:00.000000000,ada,3,4,,
    2022-01-01T15:00:00.000000000,18446744073709551615,763888930855861605,brian,2022-01-01T15:00:00.000000000,brian,1,4,,
    2022-01-01T15:00:00.000000000,18446744073709551615,10511513854575835016,ada,2022-01-01T14:00:00.000000000,ada,0,4,,
    "###);
}

#[tokio::test]
async fn test_sample_events_to_parquet() {
    let data_fixture = sample_data_fixture().await;
    let no_simplifier = QueryFixture::new(SAMPLE_EVENTS)
        .run_to_parquet_hash(&data_fixture)
        .await
        .unwrap();
    let simplifier = QueryFixture::new(SAMPLE_EVENTS)
        .run_to_parquet_hash(&data_fixture)
        .await
        .unwrap();

    assert_eq!(no_simplifier, simplifier);
    insta::assert_snapshot!(simplifier, @"30D87B12D438E393F3F3F02F00A50849100AC5F0FA4FF31429609C9C")
}
