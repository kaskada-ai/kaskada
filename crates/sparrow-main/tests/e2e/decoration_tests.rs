//! Basic e2e tests for testing decorations on queries,
//! such as final results, changed_since results, etc.

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use indoc::indoc;
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::fixture::DataFixture;
use crate::fixtures::{i64_data_fixture, timestamp_ns_data_fixture};
use crate::QueryFixture;

#[tokio::test]
async fn test_last_timestamp_ns() {
    insta::assert_snapshot!(QueryFixture::new("{ last: last(Times.n)}").run_to_csv(&timestamp_ns_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,last
    1994-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,2
    1995-10-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,4
    1996-08-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,5
    1997-12-12T00:42:57.000000000,9223372036854775808,11753611437813598533,B,5
    1998-12-13T00:43:57.000000000,9223372036854775808,11753611437813598533,B,8
    2004-12-06T00:44:57.000000000,9223372036854775808,11753611437813598533,B,23
    "###);
}

#[tokio::test]
async fn test_last_timestamp_ns_finished() {
    insta::assert_snapshot!(QueryFixture::new("{ last: last(Times.n) }").with_final_results().run_to_csv(&timestamp_ns_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,last
    2004-12-06T00:44:57.000000001,18446744073709551615,3650215962958587783,A,2
    2004-12-06T00:44:57.000000001,18446744073709551615,11753611437813598533,B,23
    "###);
}

fn time_for_test(hour: u32, min: u32, sec: u32) -> NaiveTime {
    NaiveTime::from_hms_opt(hour, min, sec).expect("valid")
}

fn date_for_test(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).expect("valid date")
}

#[tokio::test]
async fn test_last_timestamp_ns_changed_since() {
    // Expects all rows past the changed_since time to be output.
    let changed_since = NaiveDateTime::new(date_for_test(1995, 1, 1), time_for_test(0, 0, 0));
    insta::assert_snapshot!(QueryFixture::new("{ last: last(Times.n) }").with_changed_since(changed_since).run_to_csv(&timestamp_ns_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,last
    1995-10-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,4
    1996-08-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,5
    1997-12-12T00:42:57.000000000,9223372036854775808,11753611437813598533,B,5
    1998-12-13T00:43:57.000000000,9223372036854775808,11753611437813598533,B,8
    2004-12-06T00:44:57.000000000,9223372036854775808,11753611437813598533,B,23
    "###);
}

#[tokio::test]
async fn test_last_timestamp_ns_changed_since_finished() {
    // Expects only the entity with an input past the changed_since time to be
    // output.

    let changed_since = NaiveDateTime::new(date_for_test(1995, 1, 1), time_for_test(0, 0, 0));
    insta::assert_snapshot!(QueryFixture::new("{ key: Times.key, last: last(Times.n), last_time: last(Times.time) }").with_changed_since(changed_since).with_final_results().run_to_csv(&timestamp_ns_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,key,last,last_time
    2004-12-06T00:44:57.000000001,18446744073709551615,11753611437813598533,B,B,23,2004-12-05T16:44:57-08:00
    "###);
}

#[tokio::test]
async fn test_last_timestamp_ns_changed_since_equal_to_event_time() {
    // Expects inputs at the changed_since time to be included, expecting
    // an inclusive bound.

    let changed_since = NaiveDateTime::new(date_for_test(1997, 12, 12), time_for_test(0, 42, 57));
    insta::assert_snapshot!(QueryFixture::new("{ last: last(Times.n) }").with_changed_since(changed_since).run_to_csv(&timestamp_ns_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,last
    1997-12-12T00:42:57.000000000,9223372036854775808,11753611437813598533,B,5
    1998-12-13T00:43:57.000000000,9223372036854775808,11753611437813598533,B,8
    2004-12-06T00:44:57.000000000,9223372036854775808,11753611437813598533,B,23
    "###);
}

#[tokio::test]
async fn test_last_timestamp_ns_windowed_changed_since() {
    // Expects all entities to be enumerated, regardless if new inputs are seen past
    // the changed_since time.

    let changed_since = NaiveDateTime::new(date_for_test(2001, 12, 12), time_for_test(0, 42, 57));
    insta::assert_snapshot!(QueryFixture::new("{ last: last(Times.n, window=since(yearly())) }").with_changed_since(changed_since).run_to_csv(&timestamp_ns_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,last
    2002-01-01T00:00:00.000000000,18446744073709551615,3650215962958587783,A,
    2002-01-01T00:00:00.000000000,18446744073709551615,11753611437813598533,B,
    2003-01-01T00:00:00.000000000,18446744073709551615,3650215962958587783,A,
    2003-01-01T00:00:00.000000000,18446744073709551615,11753611437813598533,B,
    2004-01-01T00:00:00.000000000,18446744073709551615,3650215962958587783,A,
    2004-01-01T00:00:00.000000000,18446744073709551615,11753611437813598533,B,
    2004-12-06T00:44:57.000000000,9223372036854775808,11753611437813598533,B,23
    "###);
}

#[tokio::test]
async fn test_last_timestamp_ns_windowed_changed_since_finished() {
    // Expects all entities to be included in the final results since the ticks
    // count as new events.

    let changed_since = NaiveDateTime::new(date_for_test(2001, 12, 12), time_for_test(0, 42, 57));
    insta::assert_snapshot!(QueryFixture::new("{ last: last(Times.n, window=since(yearly())) }").with_changed_since(changed_since).with_final_results().run_to_csv(&timestamp_ns_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,last
    2004-12-06T00:44:57.000000001,18446744073709551615,3650215962958587783,A,
    2004-12-06T00:44:57.000000001,18446744073709551615,11753611437813598533,B,23
    "###);
}

#[tokio::test]
async fn test_last_timestamp_ns_changed_since_expect_no_results() {
    // Expects no results since the changed_since time is greater than all input
    // events.

    let changed_since = NaiveDateTime::new(date_for_test(2050, 1, 1), time_for_test(0, 0, 0));
    insta::assert_snapshot!(QueryFixture::new("{ time: Times.time, last: last(Times.n) }").with_changed_since(changed_since).run_to_csv(&timestamp_ns_data_fixture()).await.unwrap(), @"_time,_subsort,_key_hash,_key,time,last
");
}

const FILTERED_RESULTS: &str =
    "{ key: Times.key, time: Times.time, last: last(Times.n) } | when(Times.key == \"B\")";

#[tokio::test]
async fn test_last_timestamp_ns_changed_since_final_expect_filtered_results() {
    // Expects only results for entity B since entity A is filtered out.

    let changed_since = NaiveDateTime::new(date_for_test(2001, 12, 12), time_for_test(0, 0, 0));
    insta::assert_snapshot!(QueryFixture::new(FILTERED_RESULTS)
        .with_changed_since(changed_since)
        .with_final_results().run_to_csv(&timestamp_ns_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,key,time,last
    2004-12-06T00:44:57.000000001,18446744073709551615,11753611437813598533,B,B,2004-12-05T16:44:57-08:00,23
    "###);
}

#[tokio::test]
async fn test_last_timestamp_ns_changed_since_expect_filtered_results() {
    // Expects results only results for entity B since entity A is filtered out.

    let changed_since = NaiveDateTime::new(date_for_test(2001, 12, 12), time_for_test(0, 0, 0));
    insta::assert_snapshot!(QueryFixture::new(FILTERED_RESULTS)
        .with_changed_since(changed_since).run_to_csv(&timestamp_ns_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,key,time,last
    2004-12-06T00:44:57.000000000,9223372036854775808,11753611437813598533,B,B,2004-12-05T16:44:57-08:00,23
    "###);
}

#[tokio::test]
async fn test_last_timestamp_ns_final_expect_filtered_results() {
    // Expects results only results for entity B since entity A is filtered out.
    //
    // Regression test: The `is_new` of the `when` function was *not* filtering out
    // rows properly, so was allowing the later pass to "discover" these fields.
    insta::assert_snapshot!(QueryFixture::new(FILTERED_RESULTS)
        .with_final_results().run_to_csv(&timestamp_ns_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,key,time,last
    2004-12-06T00:44:57.000000001,18446744073709551615,11753611437813598533,B,B,2004-12-05T16:44:57-08:00,23
    "###);
}

#[tokio::test]
async fn test_last_timestamp_filtered_results() {
    // Expects results only results for entity B since entity A is filtered out.

    insta::assert_snapshot!(QueryFixture::new(FILTERED_RESULTS).run_to_csv(&timestamp_ns_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,key,time,last
    1995-10-20T00:40:57.000000000,9223372036854775808,11753611437813598533,B,B,1995-10-19T16:40:57-08:00,4
    1996-08-20T00:41:57.000000000,9223372036854775808,11753611437813598533,B,B,1996-08-19T16:41:57-08:00,5
    1997-12-12T00:42:57.000000000,9223372036854775808,11753611437813598533,B,B,1997-12-11T16:42:57-08:00,5
    1998-12-13T00:43:57.000000000,9223372036854775808,11753611437813598533,B,B,1998-12-12T16:43:57-08:00,8
    2004-12-06T00:44:57.000000000,9223372036854775808,11753611437813598533,B,B,2004-12-05T16:44:57-08:00,23
    "###);
}

#[tokio::test]
async fn test_final_equivalent_to_changed_since_zero() {
    // Expects `final` queries to behave the same as `final + changed_since_time of
    // 0`
    let changed_since = NaiveDateTime::from_timestamp_opt(0, 0).unwrap();
    insta::assert_snapshot!(QueryFixture::new("{ last: last(Times.n, window=since(yearly())) }").with_final_results().with_changed_since(changed_since).run_to_csv(&timestamp_ns_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,last
    2004-12-06T00:44:57.000000001,18446744073709551615,3650215962958587783,A,
    2004-12-06T00:44:57.000000001,18446744073709551615,11753611437813598533,B,23
    "###);

    insta::assert_snapshot!(QueryFixture::new("{ last: last(Times.n, window=since(yearly())) }").with_final_results().run_to_csv(&timestamp_ns_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,last
    2004-12-06T00:44:57.000000001,18446744073709551615,3650215962958587783,A,
    2004-12-06T00:44:57.000000001,18446744073709551615,11753611437813598533,B,23
    "###);
}

#[tokio::test]
async fn test_sum_i64_final_at_time() {
    let datetime = NaiveDateTime::new(date_for_test(1996, 12, 20), time_for_test(0, 39, 58));
    insta::assert_snapshot!(QueryFixture::new("{ sum_field: sum(Numbers.m) }").with_final_results_at_time(datetime).run_to_csv(&i64_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,sum_field
    1996-12-20T00:39:58.000000001,18446744073709551615,3650215962958587783,A,5
    1996-12-20T00:39:58.000000001,18446744073709551615,11753611437813598533,B,24
    "###);
}

#[tokio::test]
async fn test_sum_i64_all_filtered_final_at_time() {
    let datetime = NaiveDateTime::new(date_for_test(1970, 12, 20), time_for_test(0, 39, 58));
    insta::assert_snapshot!(QueryFixture::new("{ sum_field: sum(Numbers.m) }").with_final_results_at_time(datetime).run_to_csv(&i64_data_fixture()).await.unwrap(), @"");
}

fn shift_data_fixture_at_time() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new(
                "ShiftFixture",
                &Uuid::new_v4(),
                "time",
                Some("subsort"),
                "key",
                "",
            ),
            indoc! {"
    time,subsort,key,cond,bool,i64,string,other_time
    1996-12-19T16:39:57-08:00,0,A,true,false,57,hello,1997-12-19T16:39:57-08:00
    1996-12-19T16:39:58-08:00,0,B,false,true,58,world,1997-10-19T16:39:57-08:00
    1996-12-19T16:39:59-08:00,0,A,,true,59,world,1995-12-19T16:39:57-08:00
    1996-12-19T16:40:00-08:00,0,B,true,,,,2000-12-19T16:39:57-08:00
    1996-12-19T16:40:01-08:00,0,A,false,,,,
    1996-12-19T16:40:02-08:00,0,A,true,,02,hello,1999-01-19T16:39:57-08:00
    1996-12-19T16:40:03-08:00,0,A,true,,02,hello,2000-01-19T16:39:57-08:00
    1996-12-19T16:40:04-08:00,0,A,true,,02,hello,2000-01-19T16:39:57-08:00
    2000-12-19T16:40:05-08:00,0,A,true,,02,hello,2000-01-19T16:39:58-08:00
    "},
        )
        .unwrap()
}

#[tokio::test]
#[ignore = "Shift to causing unexpected batch bounds"]
async fn test_shift_until_data_i64_with_final_results_at_time() {
    let datetime = NaiveDateTime::new(date_for_test(1998, 12, 20), time_for_test(0, 39, 58));
    insta::assert_snapshot!(QueryFixture::new("{ i64: ShiftFixture.i64 | shift_to(ShiftFixture.other_time) }").with_final_results_at_time(datetime).run_to_csv(&shift_data_fixture_at_time()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,i64
    1998-12-20T00:39:58.000000001,18446744073709551615,16072519723445549088,57
    1998-12-20T00:39:58.000000001,18446744073709551615,18113259709342437355,58
    "###)
}

#[tokio::test]
async fn test_last_timestamp_ns_changed_since_with_final_at_time() {
    // Expects all rows past the changed_since time to be output.
    let changed_since = NaiveDateTime::new(date_for_test(1995, 1, 1), time_for_test(0, 0, 0));
    let final_time = NaiveDateTime::new(date_for_test(2000, 1, 1), time_for_test(0, 0, 0));
    insta::assert_snapshot!(QueryFixture::new("{ last: last(Times.n) }").with_changed_since(changed_since).with_final_results_at_time(final_time).run_to_csv(&timestamp_ns_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,last
    2000-01-01T00:00:00.000000001,18446744073709551615,11753611437813598533,B,8
    "###);
}

#[tokio::test]
async fn test_final_at_time_past_input_times() {
    // Expect rows to be produced at the final time, even if it's past the input times
    let final_time = NaiveDateTime::new(date_for_test(2020, 1, 1), time_for_test(0, 0, 0));
    insta::assert_snapshot!(QueryFixture::new("{ last: last(Times.n) }").with_final_results_at_time(final_time).run_to_csv(&timestamp_ns_data_fixture()).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,last
    2020-01-01T00:00:00.000000001,18446744073709551615,3650215962958587783,A,2
    2020-01-01T00:00:00.000000001,18446744073709551615,11753611437813598533,B,23
    "###);
}
