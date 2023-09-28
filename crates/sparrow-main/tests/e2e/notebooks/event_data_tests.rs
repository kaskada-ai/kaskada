//! Tests based on queries in the churn notebook.

use indoc::indoc;
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::{DataFixture, QueryFixture};

async fn sample_event_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_files(
            TableConfig::new_with_table_source(
                "Events",
                &Uuid::new_v4(),
                "timestamp",
                Some("subsort_id"),
                "anonymousId",
                "user",
            ),
            &["eventdata/sample_event_data.parquet"],
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_initial_query() {
    let data_fixture = sample_event_data_fixture().await;
    let no_simplifier = QueryFixture::new("Events")
        .without_simplification()
        .run_to_parquet_hash(&data_fixture)
        .await
        .unwrap();
    let simplifier = QueryFixture::new("Events")
        .run_to_parquet_hash(&data_fixture)
        .await
        .unwrap();

    // Regression test for a take on a null array
    assert_eq!(no_simplifier, simplifier);
    insta::assert_snapshot!(no_simplifier, @"9BA7713F41907C9DB4105CDF8C854D5CD9F7C4B215922640354A3A9E");
}

const EVENTS: &str = indoc! {"
let last_locale = Events.context_locale | last()

in {
    id: Events.anonymousId,

    count_today: Events
      | count(window=since(daily())),

    locale_score_sliding: Events
      | count()
      | with_key(last_locale, grouping = \"Locale\")
      | lookup(last_locale),
}
"};

const PAGE_EVENTS: &str = indoc! {"
    let EventsEvent = Events
      | when($input.type == \"page\")
      | extend({
          score: coalesce(
              if($input.event == \"Feature Run Clicked\", 2),
              if($input.event == \"Feature Selected\", 1),
              0,
          )
      })

    let last_locale = Events.context_locale | last()
    in {
        id: EventsEvent.anonymousId,

        locale_score_sliding: EventsEvent.score
          | mean(window=sliding(5, $input | is_valid()))
          | with_key(last_locale, grouping = \"Locale\")
          | lookup(last_locale),
    } | when(is_valid(EventsEvent.anonymousId))
"};

const PAGE_VIEWS: &str = indoc! {"
let Common = {
    anonymousId: Events.anonymousId,
    timestamp: Events.timestamp,
    type: Events.type,
    locale: Events.context_locale,
    projectId: Events.projectId, 
    event: Events.event,
} 

let PageEvents = {
  id: Events.anonymousId,
  type: Events.type,
} | when(Events.type == \"page\")

let is_login_event = Common.type == \"identify\" | is_valid()
let last_locale = Common.locale | last()

let page_views = PageEvents
  | count()

let page_views_this_month = PageEvents
  | count($input, window=since(monthly()))

in {is_login_event, last_locale, page_views, page_views_this_month}
"};

#[tokio::test]
async fn test_events() {
    insta::assert_snapshot!(QueryFixture::new(EVENTS).run_to_csv(&sample_event_data_fixture().await).await.unwrap(),
    @r###"
    _time,_subsort,_key_hash,_key,id,count_today,locale_score_sliding
    2020-10-27T16:03:28.331000000,0,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,1,1
    2020-10-27T17:24:17.956000000,3,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,2,2
    2020-10-27T17:24:17.967000000,5,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,3,3
    2020-10-27T17:24:17.967000000,6,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,4,4
    2020-10-27T17:25:45.242000000,21,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,5,5
    2020-10-27T17:25:45.248000000,22,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,6,6
    2020-10-27T17:25:53.717000000,31,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,7,7
    2020-10-27T17:26:25.213000000,46,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,8,8
    2020-10-27T17:26:35.816000000,47,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,9,9
    2020-10-27T17:26:49.665000000,48,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,10,10
    2020-10-27T17:29:35.525000000,49,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,11,11
    2020-10-27T17:30:21.233000000,51,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,12,12
    2020-10-27T17:32:36.646000000,52,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,13,13
    2020-10-27T17:33:55.353000000,59,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,14,14
    2020-10-27T17:34:03.546000000,60,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,15,15
    2020-10-27T17:35:39.310000000,61,1279197888909376308,02b9152e-3b25-45cc-b7bb-0d8f98bf7524,02b9152e-3b25-45cc-b7bb-0d8f98bf7524,1,1
    2020-10-27T17:35:39.311000000,62,1279197888909376308,02b9152e-3b25-45cc-b7bb-0d8f98bf7524,02b9152e-3b25-45cc-b7bb-0d8f98bf7524,2,2
    2020-10-27T17:35:47.195000000,73,1279197888909376308,02b9152e-3b25-45cc-b7bb-0d8f98bf7524,02b9152e-3b25-45cc-b7bb-0d8f98bf7524,3,3
    2020-10-27T17:35:47.201000000,74,1279197888909376308,02b9152e-3b25-45cc-b7bb-0d8f98bf7524,02b9152e-3b25-45cc-b7bb-0d8f98bf7524,4,4
    2020-10-27T17:36:30.940000000,89,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,16,16
    2020-10-27T17:36:31.894000000,90,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,1,1
    2020-10-27T17:36:31.894000000,91,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,2,2
    2020-10-27T17:36:31.895000000,92,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,3,3
    2020-10-27T17:36:35.873000000,101,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,4,4
    2020-10-27T17:36:36.031000000,110,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,5,5
    2020-10-27T17:36:37.360000000,111,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,6,6
    2020-10-27T17:36:37.453000000,112,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,7,7
    2020-10-27T17:36:38.193000000,113,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,8,8
    2020-10-27T17:36:38.259000000,114,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,9,9
    2020-10-27T17:36:38.923000000,115,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,10,10
    2020-10-27T17:36:39.012000000,116,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,11,11
    2020-10-27T17:36:41.397000000,117,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,12,12
    2020-10-27T17:36:41.916000000,118,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,13,13
    2020-10-27T17:36:41.980000000,119,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,14,14
    2020-10-27T17:36:42.939000000,120,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,15,15
    2020-10-27T17:36:43.652000000,121,1279197888909376308,02b9152e-3b25-45cc-b7bb-0d8f98bf7524,02b9152e-3b25-45cc-b7bb-0d8f98bf7524,5,5
    2020-10-27T17:36:43.862000000,122,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,16,16
    2020-10-27T17:36:43.927000000,123,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,17,17
    2020-10-27T17:36:47.068000000,126,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,18,18
    2020-10-27T17:36:48.517000000,127,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,19,19
    2020-10-27T17:36:52.086000000,130,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,20,20
    2020-10-27T17:36:52.145000000,131,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,21,21
    2020-10-27T17:36:52.548000000,132,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,22,22
    2020-10-27T17:36:52.629000000,133,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,23,23
    2020-10-27T17:36:57.093000000,134,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,17,17
    2020-10-27T17:36:57.104000000,135,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,18,18
    "###);
}

#[tokio::test]
async fn test_events_with_final_results() {
    insta::assert_snapshot!(QueryFixture::new(EVENTS).with_final_results().run_to_csv(&sample_event_data_fixture().await).await.unwrap(),
    @r###"
    _time,_subsort,_key_hash,_key,id,count_today,locale_score_sliding
    2020-10-27T17:36:57.104000001,18446744073709551615,1279197888909376308,02b9152e-3b25-45cc-b7bb-0d8f98bf7524,02b9152e-3b25-45cc-b7bb-0d8f98bf7524,5,5
    2020-10-27T17:36:57.104000001,18446744073709551615,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,18,18
    2020-10-27T17:36:57.104000001,18446744073709551615,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,23,23
    "###);
}

#[tokio::test]
async fn test_page_event() {
    insta::assert_snapshot!(QueryFixture::new(PAGE_EVENTS).run_to_csv(&sample_event_data_fixture().await).await.unwrap(),
    @r###"
    _time,_subsort,_key_hash,_key,id,locale_score_sliding
    2020-10-27T16:03:28.331000000,0,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,2.0
    2020-10-27T17:24:17.967000000,5,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,1.5
    2020-10-27T17:25:45.242000000,21,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,1.0
    2020-10-27T17:25:53.717000000,31,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,0.75
    2020-10-27T17:35:39.310000000,61,1279197888909376308,02b9152e-3b25-45cc-b7bb-0d8f98bf7524,02b9152e-3b25-45cc-b7bb-0d8f98bf7524,0.0
    2020-10-27T17:35:47.195000000,73,1279197888909376308,02b9152e-3b25-45cc-b7bb-0d8f98bf7524,02b9152e-3b25-45cc-b7bb-0d8f98bf7524,0.0
    2020-10-27T17:36:31.894000000,90,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0.0
    2020-10-27T17:36:31.894000000,91,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0.0
    2020-10-27T17:36:57.093000000,134,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,0.8
    "###);
}

#[tokio::test]
async fn test_page_event_with_final_results() {
    insta::assert_snapshot!(QueryFixture::new(PAGE_EVENTS).with_final_results().run_to_csv(&sample_event_data_fixture().await).await.unwrap(),
    @r###"
    _time,_subsort,_key_hash,_key,id,locale_score_sliding
    2020-10-27T17:36:57.104000001,18446744073709551615,1279197888909376308,02b9152e-3b25-45cc-b7bb-0d8f98bf7524,02b9152e-3b25-45cc-b7bb-0d8f98bf7524,0.0
    2020-10-27T17:36:57.104000001,18446744073709551615,17552223493047837804,8a16beda-c07a-4625-a805-2d28f5934107,8a16beda-c07a-4625-a805-2d28f5934107,0.8
    2020-10-27T17:36:57.104000001,18446744073709551615,17703029354039803950,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0b00083c-5c1e-47f5-abba-f89b12ae3cf4,0.0
    "###);
}

#[tokio::test]
async fn test_page_views() {
    // TODO: Support time zones in sparrow.
    // We can't assert on the result, since the definition of `monthly()`
    // depends on the timezone of the machine the test runs on.
    QueryFixture::new(PAGE_VIEWS)
        .run_to_parquet(&sample_event_data_fixture().await)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_multiple_distinct_partitions() {
    let data = sample_event_data_fixture()
        .await
        .with_table_from_files(
            TableConfig::new_with_table_source(
                "EventsByLocale",
                &Uuid::new_v4(),
                "timestamp",
                Some("subsort_id"),
                "context_locale",
                "",
            ),
            &["eventdata/event_data.parquet"],
        )
        .await
        .unwrap();

    let hash = QueryFixture::new(
        "EventsByLocale |  {
            time: time_of($input),
            count_yesterday: count($input) | when(daily()) | last(),
            count_session:   count($input, window = since($input.type == \"identify\")) }",
    )
    .with_formula(
        "Events",
        "{
        id: Events.anonymousId,
        projectId: Events.projectId,
        event: Events.event
      } | when(Events.type == \"page\") ",
    )
    .run_to_parquet_hash(&data)
    .await
    .unwrap();

    insta::assert_snapshot!(
        hash,
        @"4A0B30BC114271D47445F10F35A83ECFE4A9FAC0CF5370CA2423E852"
    );
}

#[tokio::test]
async fn test_consistent_result_count_with_select_fields() {
    // Regression test for select_fields affecting cardinality
    let data_fixture = event_data_fixture().await;

    let query1 = r#"
    let TrackEvent = Events
    | when(Events.type == 'track')
    in {
      count_today: TrackEvent | count(window=since(daily())),
    } | when(TrackEvent | is_valid())
    "#;

    let query2 = r#"
    let TrackEvent = Events
    | when(Events.type == 'track')
    | select_fields($input, "projectId", "event", "context_locale")
    in {
        count_today: TrackEvent | count(window=since(daily())),
    } | when(TrackEvent | is_valid())
    "#;
    assert_equal_result_counts(12705, query1, query2, &data_fixture).await
}

#[tokio::test]
async fn test_consistent_cardinality_with_extend_windows() {
    // Regression test for extend and windows affecting cardinality
    let data_fixture = event_data_fixture().await;

    let query1 = r#"
    let t = Events
          | extend({
                engagement_score: 0,
            })
    in {
        count: t | count(window=since(daily())),
    } | when(is_valid(t))
    "#;

    let query2 = r#"
    let t = Events
          | extend({
                engagement_score: 0,
            })
    in {
        count: t | count(window=since(daily())),
    } | when(is_valid(t.anonymousId))

    "#;
    assert_equal_result_counts(100001, query1, query2, &data_fixture).await
}

#[tokio::test]
async fn test_consistent_result_count_with_when() {
    let data_fixture = event_data_fixture().await;

    let query1 = r#"
     let track_event = Events
        | when($input.type == "track")

    let login_features = { key: track_event.anonymousId }

    let is_login_event = is_valid(Events.type == "identify")
    in login_features | last()
        | extend({is_login_event: count(is_login_event)})
        | when(count(is_login_event) >= 1)
     "#;

    let query2 = r#"
    let track_event = Events
        | when($input.type == "track")

    let login_features = { key: track_event.anonymousId }

    let is_login_event = is_valid(Events.type == "identify")

    in login_features | last()
        | when(count(is_login_event) >= 1)
    "#;

    // results from the 2 queries as dissimilar -- query1 extends the result record
    // while query2 does not extend
    let query1_count = QueryFixture::new(query1)
        .run_to_csv(&data_fixture)
        .await
        .unwrap()
        .lines()
        .count();
    let query2_count = QueryFixture::new(query2)
        .run_to_csv(&data_fixture)
        .await
        .unwrap()
        .lines()
        .count();
    assert_eq!(
        query1_count, query1_count,
        "Total numbers of rows differ between query1 {query1_count} and query2 {query2_count}"
    );
}

async fn event_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_files(
            TableConfig::new_with_table_source(
                "Events",
                &Uuid::new_v4(),
                "timestamp",
                Some("subsort_id"),
                "anonymousId",
                "user",
            ),
            &["eventdata/event_data.parquet"],
        )
        .await
        .unwrap()
}

async fn assert_equal_result_counts(
    expected: usize,
    query1: &str,
    query2: &str,
    data: &DataFixture,
) {
    let csv1 = QueryFixture::new(query1).run_to_csv(data).await.unwrap();
    let csv2 = QueryFixture::new(query2).run_to_csv(data).await.unwrap();

    similar_asserts::assert_eq!(csv1, csv2);

    let num_rows = csv1.lines().count();

    assert_eq!(
        num_rows, expected,
        "Expected query1 to produce {expected} rows but was {num_rows}"
    );
}
