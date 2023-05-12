//! Basic e2e tests for fake resumeable compute
use indoc::indoc;
use sparrow_api::kaskada::v1alpha::{source_data, TableConfig};
use uuid::Uuid;

use crate::{DataFixture, QueryFixture};

/// Asserts that the `query` when executed on `csv1` and then incrementally on
/// `csv2` produces the same results as when executed on `csv1` and `csv2`.
///
/// This removes the `csv1` file from the fixture before resuming, so if the
/// results are correct it must be due to the storage / persistence.
async fn assert_final_incremental_same_as_complete(
    query: QueryFixture,
    config: TableConfig,
    csv1: &str,
    csv2: &str,
) -> String {
    let snapshot_dir = tempfile::Builder::new()
        .prefix("snapshots_")
        .tempdir()
        .unwrap();

    let non_persistent_query = query.clone().with_final_results();
    let persistent_query = query
        .with_final_results()
        .with_rocksdb(snapshot_dir.path(), None);

    let mut data_fixture = DataFixture::new()
        .with_table_from_csv(config, csv1)
        .await
        .unwrap();

    // Run the query with the first file, updating the Rocks DB.
    let snapshot_path = match persistent_query.run_snapshot_to_csv(&data_fixture).await {
        Err(err) => panic!("Persistent query failed {err}"),
        Ok(mut result) => {
            // Only one snapshot per query execution is currently supported.
            assert_eq!(result.snapshots.len(), 1);
            std::path::PathBuf::from(result.snapshots.remove(0).path)
        }
    };

    // Add the second file
    let csv2 = source_data::Source::CsvData(csv2.to_owned());
    data_fixture
        .table_mut("Numbers")
        .add_file_source(&csv2)
        .await
        .unwrap();

    let non_persistent_results = non_persistent_query
        .run_to_csv(&data_fixture)
        .await
        .unwrap();

    // Clear the table and re-add just the second file.
    //
    // TODO: This is a bit hacky, but we do it to make sure the persistent
    // results really use the snapshot. Currently, this forces the files
    // to be reprepared. We would like to better cache these and/or have
    // some way of just saying "run the query, but make sure it doesn't
    // open this file".
    let numbers = data_fixture.table_mut("Numbers");
    numbers.clear();
    numbers.add_file_source(&csv2).await.unwrap();
    let persistent_results = persistent_query
        .with_rocksdb(snapshot_dir.path(), Some(&snapshot_path))
        .run_to_csv(&data_fixture)
        .await
        .unwrap();

    similar_asserts::assert_eq!(&persistent_results, &non_persistent_results);
    persistent_results
}

#[tokio::test]
async fn test_basic_resumeable_final() {
    // This is a naive test for resumable queries that may "accidentally" pass
    // even in some cases it shouldn't. For instance, since the entities "reappear"
    // in the same order in the new file, it doesn't need to rely on the hash->index
    // persistence. We keep it (for now) as a "naive" or "smoke" test that just
    // makes sure things are generally work. Specific cases are handled in other
    // tests.
    //
    // At least this accidental case is handled by
    // `test_resumeable_entity_reordered`.
    let query_fixture =
        QueryFixture::new("{ key: last(Numbers.key), m: Numbers.m, sum_m: sum(Numbers.m) }");
    let result = assert_final_incremental_same_as_complete(
        query_fixture,
        TableConfig::new_with_table_source(
            "Numbers",
            &Uuid::new_v4(),
            "time",
            Some("subsort"),
            "key",
            "",
        ),
        indoc! {"
        time,subsort,key,m,n
        1996-12-19T16:39:57-08:00,0,A,5,10
        1996-12-19T16:39:58-08:00,0,B,24,3
        1996-12-19T16:39:59-08:00,0,A,17,6
        1996-12-19T16:40:00-08:00,0,A,,9
        "},
        indoc! {"
        time,subsort,key,m,n
        1996-12-19T16:40:01-08:00,0,A,12,
        1997-12-19T16:40:01-08:00,0,B,2,5
        1997-12-19T16:40:02-08:00,0,B,2,
        1996-12-19T16:40:02-08:00,0,B,2,
        1996-12-19T16:40:03-08:00,0,A,,
    "},
    )
    .await;

    insta::assert_snapshot!(result, @r###"
    _time,_subsort,_key_hash,_key,key,m,sum_m
    1997-12-20T00:40:02.000000001,18446744073709551615,3650215962958587783,A,A,,34
    1997-12-20T00:40:02.000000001,18446744073709551615,11753611437813598533,B,B,2,30
    "###);
}

#[tokio::test]
async fn test_resumeable_entity_reordered() {
    // Test that things work when entities appear in a different order.
    // This will require persisting the key hash to entity index map.
    let query_fixture =
        QueryFixture::new("{ key: last(Numbers.key), m: Numbers.m, sum_m: sum(Numbers.m) }");
    let result = assert_final_incremental_same_as_complete(
        query_fixture,
        TableConfig::new_with_table_source(
            "Numbers",
            &Uuid::new_v4(),
            "time",
            Some("subsort"),
            "key",
            "",
        ),
        indoc! {"
        time,subsort,key,m,n
        1996-12-19T16:39:57-08:00,0,A,5,10
        1996-12-19T16:39:58-08:00,0,B,24,3
        1996-12-19T16:39:59-08:00,0,A,17,6
        1996-12-19T16:40:00-08:00,0,A,,9
        "},
        indoc! {"
        time,subsort,key,m,n
        1996-12-19T16:40:01-08:00,0,B,12,
        1997-12-19T16:40:01-08:00,0,A,2,5
        1997-12-19T16:40:02-08:00,0,A,2,
        1996-12-19T16:40:02-08:00,0,B,2,
        1996-12-19T16:40:03-08:00,0,A,,
    "},
    )
    .await;

    insta::assert_snapshot!(result, @r###"
    _time,_subsort,_key_hash,_key,key,m,sum_m
    1997-12-20T00:40:02.000000001,18446744073709551615,3650215962958587783,A,A,2,26
    1997-12-20T00:40:02.000000001,18446744073709551615,11753611437813598533,B,B,2,38
    "###);
}

#[tokio::test]
async fn test_resumeable_with_unordered_file_sets() {
    let snapshot_dir = tempfile::Builder::new()
        .prefix("snapshots_")
        .tempdir()
        .unwrap();

    let query =
        QueryFixture::new("{ key: last(Numbers.key), m: Numbers.m, sum_m: sum(Numbers.m) }");
    let config = TableConfig::new_with_table_source(
        "Numbers",
        &Uuid::new_v4(),
        "time",
        Some("subsort"),
        "key",
        "",
    );
    let csv1 = indoc! {"
        time,subsort,key,m,n
        1996-12-19T16:39:57-08:00,0,A,5,10
        1996-12-19T16:39:58-08:00,0,B,24,3
        1996-12-19T16:39:59-08:00,0,A,17,6
        1996-12-19T16:40:00-08:00,0,A,,9
        "};
    let csv2 = indoc! {"
        time,subsort,key,m,n
        1996-12-19T16:40:01-08:00,0,B,12,
        1997-12-19T16:40:01-08:00,0,A,2,5
        1997-12-19T16:40:02-08:00,0,A,2,
        1996-12-19T16:40:02-08:00,0,B,2,
        1996-12-19T16:40:03-08:00,0,A,,
    "};

    let persistent_query = query
        .with_final_results()
        .with_rocksdb(snapshot_dir.path(), None);
    let mut data_fixture = DataFixture::new()
        .with_table_from_csv(config, csv1)
        .await
        .unwrap();

    // Run the query with the first file, updating the Rocks DB.
    let snapshot_path = match persistent_query.run_snapshot_to_csv(&data_fixture).await {
        Err(err) => panic!("Persistent query failed {err}"),
        Ok(mut result) => {
            // Only one snapshot is currently supported.
            assert_eq!(result.snapshots.len(), 1);
            std::path::PathBuf::from(result.snapshots.remove(0).path)
        }
    };

    // Clear the table, add the second file first, then the first file.
    let csv1 = source_data::Source::CsvData(csv1.to_owned());
    let csv2 = source_data::Source::CsvData(csv2.to_owned());

    data_fixture.table_mut("Numbers").clear();
    data_fixture
        .table_mut("Numbers")
        .add_file_source(&csv2)
        .await
        .unwrap();
    data_fixture
        .table_mut("Numbers")
        .add_file_source(&csv1)
        .await
        .unwrap();

    let persistent_results = persistent_query
        .with_rocksdb(snapshot_dir.path(), Some(&snapshot_path))
        .run_to_csv(&data_fixture)
        .await
        .unwrap();

    insta::assert_snapshot!(persistent_results, @r###"
    _time,_subsort,_key_hash,_key,key,m,sum_m
    1997-12-20T00:40:02.000000001,18446744073709551615,3650215962958587783,A,A,2,26
    1997-12-20T00:40:02.000000001,18446744073709551615,11753611437813598533,B,B,2,38
    "###);
}

#[tokio::test]
async fn test_resumeable_ticks_old_entities() {
    // Test that entities only appearing in older files are still
    // ticked.
    //
    // This uses a 3-hourly sliding window.
    let query_fixture = QueryFixture::new(
        "{ key: last(Numbers.key), m: Numbers.m, hourly_count: count(Numbers.m, window=sliding(3, \
         hourly())) }",
    );
    let result = assert_final_incremental_same_as_complete(
        query_fixture,
        TableConfig::new_with_table_source(
            "Numbers",
            &Uuid::new_v4(),
            "time",
            Some("subsort"),
            "key",
            "",
        ),
        indoc! {"
        time,subsort,key,m,n
        1996-12-19T18:38:57-08:00,0,E,5,10
        1996-12-19T18:39:57-08:00,0,A,5,10
        1996-12-19T18:39:58-08:00,0,B,24,3
        1996-12-19T18:39:59-08:00,0,B,17,6
        1996-12-19T19:40:00-08:00,0,C,8,7
        1996-12-19T19:41:00-08:00,0,A,1,9
        "},
        indoc! {"
        time,subsort,key,m,n
        1996-12-19T20:40:01-08:00,0,D,12,
        1996-12-19T20:40:02-08:00,0,B,2,5
        1996-12-19T20:40:03-08:00,0,D,2,
        1996-12-19T21:41:04-08:00,0,D,2,
        1996-12-19T21:42:05-08:00,0,A,3,
    "},
    )
    .await;

    // In the first batch:
    //  Hour 18-19: A=1, B=2, E=1
    //  Hour 19-20: A=1, C=1
    // In the second batch:
    //  Hour 20-21: B=1, D=2,
    //  Hour 21-22: A=1,D=1
    //
    // The output is produced at the "final" tick at 21:42, at which point we output
    // the last value that came out of the expression. In this case, in the absence
    // of data, the last value produced was when the hourly count ticked down at
    // 21:00. Note that this *does not* represent the value *currently* in the
    // window.
    insta::assert_snapshot!(result, @r###"
    _time,_subsort,_key_hash,_key,key,m,hourly_count
    1996-12-20T05:42:05.000000001,18446744073709551615,3650215962958587783,A,A,3,2
    1996-12-20T05:42:05.000000001,18446744073709551615,9192031977313001967,C,C,,1
    1996-12-20T05:42:05.000000001,18446744073709551615,11430173353997062025,D,D,2,3
    1996-12-20T05:42:05.000000001,18446744073709551615,11753611437813598533,B,B,,3
    1996-12-20T05:42:05.000000001,18446744073709551615,17976613645339558306,E,E,,1
    "###);
}

#[tokio::test]
async fn test_resumeable_when() {
    // Test that a resuming a query with a `when` filter works as expected.
    let query_fixture = QueryFixture::new(
        "{ key: last(Numbers.key), m: Numbers.m, count: count(Numbers.m) } | when($input.count > \
         1)",
    );
    let result = assert_final_incremental_same_as_complete(
        query_fixture,
        TableConfig::new_with_table_source(
            "Numbers",
            &Uuid::new_v4(),
            "time",
            Some("subsort"),
            "key",
            "",
        ),
        indoc! {"
        time,subsort,key,m,n
        1996-12-19T18:38:57-08:00,0,E,5,10
        1996-12-19T18:39:57-08:00,0,A,5,10
        1996-12-19T18:39:58-08:00,0,B,24,3
        1996-12-19T18:39:59-08:00,0,B,17,6
        1996-12-19T19:40:00-08:00,0,C,8,7
        1996-12-19T19:41:00-08:00,0,A,1,9
        "},
        indoc! {"
        time,subsort,key,m,n
        1996-12-19T20:40:01-08:00,0,D,12,
        1996-12-19T20:40:02-08:00,0,B,2,5
        1996-12-19T20:40:03-08:00,0,D,2,
        1996-12-19T21:41:04-08:00,0,D,2,
        1996-12-19T21:42:05-08:00,0,A,3,
    "},
    )
    .await;

    insta::assert_snapshot!(result, @r###"
    _time,_subsort,_key_hash,_key,key,m,count
    1996-12-20T05:42:05.000000001,18446744073709551615,3650215962958587783,A,A,3,3
    1996-12-20T05:42:05.000000001,18446744073709551615,11430173353997062025,D,D,2,3
    1996-12-20T05:42:05.000000001,18446744073709551615,11753611437813598533,B,B,2,3
    "###);
}

#[tokio::test]
async fn test_resumeable_lookup() {
    // Test that resuming a query with a `lookup` works as expected.
    let query_fixture = QueryFixture::new(
        "{ key: last(Numbers.key), m: Numbers.m, count: count(Numbers.m) }
            | extend({ other_count: lookup(Numbers.other, $input.count)})",
    );
    let result = assert_final_incremental_same_as_complete(
        query_fixture,
        TableConfig::new_with_table_source(
            "Numbers",
            &Uuid::new_v4(),
            "time",
            Some("subsort"),
            "key",
            "",
        ),
        indoc! {"
    time,subsort,key,m,other
    1996-12-19T18:38:57-08:00,0,E,5,A
    1996-12-19T18:39:57-08:00,0,A,5,E
    1996-12-19T18:39:58-08:00,0,B,24,B
    1996-12-19T18:39:59-08:00,0,B,17,A
    1996-12-19T19:40:00-08:00,0,C,8,B
    1996-12-19T19:41:00-08:00,0,A,1,C
    "},
        indoc! {"
    time,subsort,key,m,other
    1996-12-19T20:40:01-08:00,0,D,12,A
    1996-12-19T20:40:02-08:00,0,B,2,B
    1996-12-19T20:40:03-08:00,0,D,2,A
    1996-12-19T21:41:04-08:00,0,D,2,B
    1996-12-19T21:42:05-08:00,0,A,3,D
"},
    )
    .await;

    insta::assert_snapshot!(result, @r###"
    _time,_subsort,_key_hash,_key,other_count,key,m,count
    1996-12-20T05:42:05.000000001,18446744073709551615,3650215962958587783,A,3,A,3,3
    1996-12-20T05:42:05.000000001,18446744073709551615,9192031977313001967,C,2,C,8,1
    1996-12-20T05:42:05.000000001,18446744073709551615,11430173353997062025,D,3,D,2,3
    1996-12-20T05:42:05.000000001,18446744073709551615,11753611437813598533,B,3,B,2,3
    1996-12-20T05:42:05.000000001,18446744073709551615,17976613645339558306,E,,E,5,1
    "###);
}

#[tokio::test]
async fn test_resumeable_with_key() {
    // Test that resuming a query with a `with_key` works as expected.
    let query_fixture = QueryFixture::new("Numbers | with_key(Numbers.m)");
    let result = assert_final_incremental_same_as_complete(
        query_fixture,
        TableConfig::new_with_table_source(
            "Numbers",
            &Uuid::new_v4(),
            "time",
            Some("subsort"),
            "key",
            "",
        ),
        indoc! {"
    time,subsort,key,m,other
    1996-12-19T18:38:57-08:00,0,E,5,A
    1996-12-19T18:39:57-08:00,0,A,5,E
    1996-12-19T18:39:58-08:00,0,B,24,B
    1996-12-19T18:39:59-08:00,0,B,17,A
    1996-12-19T19:40:00-08:00,0,C,8,B
    1996-12-19T19:41:00-08:00,0,A,1,C
    "},
        indoc! {"
    time,subsort,key,m,other
    1996-12-19T20:40:01-08:00,0,D,12,A
    1996-12-19T20:40:02-08:00,0,B,2,B
    1996-12-19T20:40:03-08:00,0,D,2,A
    1996-12-19T21:41:04-08:00,0,D,2,B
    1996-12-19T21:42:05-08:00,0,A,3,D
"},
    )
    .await;

    insta::assert_snapshot!(result, @r###"
    _time,_subsort,_key_hash,_key,time,subsort,key,m,other
    1996-12-20T05:42:05.000000001,18446744073709551615,650022633471272026,17,1996-12-19T18:39:59-08:00,0,B,17,A
    1996-12-20T05:42:05.000000001,18446744073709551615,1575016611515860288,2,1996-12-19T21:41:04-08:00,0,D,2,B
    1996-12-20T05:42:05.000000001,18446744073709551615,2359047937476779835,1,1996-12-19T19:41:00-08:00,0,A,1,C
    1996-12-20T05:42:05.000000001,18446744073709551615,4864632034659211723,8,1996-12-19T19:40:00-08:00,0,C,8,B
    1996-12-20T05:42:05.000000001,18446744073709551615,9175685813237050681,24,1996-12-19T18:39:58-08:00,0,B,24,B
    1996-12-20T05:42:05.000000001,18446744073709551615,10021492687541564645,5,1996-12-19T18:39:57-08:00,0,A,5,E
    1996-12-20T05:42:05.000000001,18446744073709551615,14956259290599888306,3,1996-12-19T21:42:05-08:00,0,A,3,D
    1996-12-20T05:42:05.000000001,18446744073709551615,17018031324644251917,12,1996-12-19T20:40:01-08:00,0,D,12,A
    "###);
}

#[tokio::test]
#[ignore = "Shift to literal unsupported"]
async fn test_resumeable_shift_to_literal() {
    // Test that a resuming a query with a `shift to literal`.
    //
    // Note that the test suite does not have a function to discard snapshots
    // or check whether one is invalid. Therefore, we cannot shift the rows in
    // the first file past the rows in the second. In a real query scenario,
    // Wren handles choosing the valid snapshot to send to sparrow.
    let query_fixture =
        QueryFixture::new("Numbers | shift_to(time=851060636000000001 as timestamp_ns)");
    let result = assert_final_incremental_same_as_complete(
        query_fixture,
        TableConfig::new_with_table_source(
            "Numbers",
            &Uuid::new_v4(),
            "time",
            Some("subsort"),
            "key",
            "",
        ),
        indoc! {"
        time,subsort,key,m,n
        1996-12-19T18:38:57-08:00,0,E,5,10
        1996-12-19T18:39:57-08:00,0,A,5,10
        1996-12-19T18:39:58-08:00,0,B,24,3
        1996-12-19T18:39:59-08:00,0,B,17,6
        1996-12-19T19:40:00-08:00,0,C,8,7
        1996-12-19T19:41:00-08:00,0,A,1,9
        "},
        indoc! {"
        time,subsort,key,m,n
        1996-12-19T20:40:01-08:00,0,D,12,
        1996-12-19T20:40:02-08:00,0,B,2,5
        1996-12-19T20:40:03-08:00,0,D,2,
        1996-12-19T21:41:04-08:00,0,D,2,
        1996-12-19T21:42:05-08:00,0,A,3,
    "},
    )
    .await;

    insta::assert_snapshot!(result, @r###"
    _time,_subsort,_key_hash,time,subsort,key,m,n
    1996-12-20T05:42:15.000000001,18446744073709551615,7320041501611000293,1996-12-19T19:40:00-08:00,0,C,8,7
    1996-12-20T05:42:15.000000001,18446744073709551615,8857646378891708708,1996-12-19T18:38:57-08:00,0,E,5,10
    1996-12-20T05:42:15.000000001,18446744073709551615,12016876461696839584,1996-12-19T21:41:04-08:00,0,D,2,
    1996-12-20T05:42:15.000000001,18446744073709551615,16072519723445549088,1996-12-19T21:42:05-08:00,0,A,3,
    1996-12-20T05:42:15.000000001,18446744073709551615,18113259709342437355,1996-12-19T20:40:02-08:00,0,B,2,5
    "###);
}

#[tokio::test]
async fn test_resumeable_shift_to_column() {
    // Test that a resuming a query with a `shift to column`.
    //
    // Note that the test suite does not have a function to discard snapshots
    // or check whether one is invalid. Therefore, we cannot shift the rows in
    // the first file past the rows in the second. In a real query scenario,
    // Wren handles choosing the valid snapshot to send to sparrow.
    let query_fixture =
        QueryFixture::new("{ shifted: Numbers.time | shift_to(add_time(seconds(10))) }");
    let result = assert_final_incremental_same_as_complete(
        query_fixture,
        TableConfig::new_with_table_source(
            "Numbers",
            &Uuid::new_v4(),
            "time",
            Some("subsort"),
            "key",
            "",
        ),
        indoc! {"
        time,subsort,key,m,n
        1996-12-19T18:38:57-08:00,0,E,5,10
        1996-12-19T18:39:57-08:00,0,A,5,10
        1996-12-19T18:39:58-08:00,0,B,24,3
        1996-12-19T18:39:59-08:00,0,B,17,6
        1996-12-19T19:40:00-08:00,0,C,8,7
        1996-12-19T19:41:00-08:00,0,A,1,9
        "},
        indoc! {"
        time,subsort,key,m,n
        1996-12-19T20:40:01-08:00,0,D,12,
        1996-12-19T20:40:02-08:00,0,B,2,5
        1996-12-19T20:40:03-08:00,0,D,2,
        1996-12-19T21:41:04-08:00,0,D,2,
        1996-12-19T21:42:05-08:00,0,A,3,
    "},
    )
    .await;

    insta::assert_snapshot!(result, @r###"
    _time,_subsort,_key_hash,_key,shifted
    1996-12-20T05:42:15.000000001,18446744073709551615,3650215962958587783,A,1996-12-19T21:42:05-08:00
    1996-12-20T05:42:15.000000001,18446744073709551615,9192031977313001967,C,1996-12-19T19:40:00-08:00
    1996-12-20T05:42:15.000000001,18446744073709551615,11430173353997062025,D,1996-12-19T21:41:04-08:00
    1996-12-20T05:42:15.000000001,18446744073709551615,11753611437813598533,B,1996-12-19T20:40:02-08:00
    1996-12-20T05:42:15.000000001,18446744073709551615,17976613645339558306,E,1996-12-19T18:38:57-08:00
    "###);
}

#[tokio::test]
async fn test_resumeable_lag_basic() {
    // Test for resuming a query with a lag operator.
    let query_fixture =
        QueryFixture::new("{ lag_1_m: Numbers.m | lag(1), lag_2_n: Numbers.n | lag(2) }");
    let result = assert_final_incremental_same_as_complete(
        query_fixture,
        TableConfig::new_with_table_source(
            "Numbers",
            &Uuid::new_v4(),
            "time",
            Some("subsort"),
            "key",
            "",
        ),
        indoc! {"
        time,subsort,key,m,n
        1996-12-19T18:38:57-08:00,0,E,5,10
        1996-12-19T18:39:57-08:00,0,A,5,10
        1996-12-19T18:39:58-08:00,0,B,24,3
        1996-12-19T18:39:59-08:00,0,B,17,6
        1996-12-19T19:40:00-08:00,0,C,8,7
        1996-12-19T19:41:00-08:00,0,A,1,9
        "},
        indoc! {"
        time,subsort,key,m,n
        1996-12-19T20:40:01-08:00,0,D,12,
        1996-12-19T20:40:02-08:00,0,B,2,5
        1996-12-19T20:40:03-08:00,0,D,2,
        1996-12-19T21:41:04-08:00,0,D,2,
        1996-12-19T21:42:05-08:00,0,A,3,
    "},
    )
    .await;

    insta::assert_snapshot!(result, @r###"
    _time,_subsort,_key_hash,_key,lag_1_m,lag_2_n
    1996-12-20T05:42:05.000000001,18446744073709551615,3650215962958587783,A,1,10
    1996-12-20T05:42:05.000000001,18446744073709551615,9192031977313001967,C,,
    1996-12-20T05:42:05.000000001,18446744073709551615,11430173353997062025,D,2,
    1996-12-20T05:42:05.000000001,18446744073709551615,11753611437813598533,B,17,3
    1996-12-20T05:42:05.000000001,18446744073709551615,17976613645339558306,E,,
    "###);
}

#[tokio::test]
async fn test_resumeable_final_no_new_data() {
    // Test that producing final results from a snapshot with no new data works.
    let snapshot_dir = tempfile::Builder::new()
        .prefix("snapshots_")
        .tempdir()
        .unwrap();

    let query = QueryFixture::new(
        "{ key: last(Numbers.key), m: Numbers.m, count: count(Numbers.m) }
                | extend({ other_count: lookup(Numbers.other, $input.count)})",
    )
    .with_rocksdb(snapshot_dir.path(), None)
    .with_final_results();

    let config = TableConfig::new_with_table_source(
        "Numbers",
        &Uuid::new_v4(),
        "time",
        Some("subsort"),
        "key",
        "",
    );
    let csv1 = indoc! {"
        time,subsort,key,m,other
        1996-12-19T18:38:57-08:00,0,E,5,A
        1996-12-19T18:39:57-08:00,0,A,5,E
        1996-12-19T18:39:58-08:00,0,B,24,B
        1996-12-19T18:39:59-08:00,0,B,17,A
        1996-12-19T19:40:00-08:00,0,C,8,B
        1996-12-19T19:41:00-08:00,0,A,1,C
        "};

    let mut data_fixture = DataFixture::new()
        .with_table_from_csv(config, csv1)
        .await
        .unwrap();

    // Run the query, updating the RocksDb
    let (result1, snapshot_path) = match query.run_snapshot_to_csv(&data_fixture).await {
        Err(err) => panic!("Persistent query failed {err}"),
        Ok(mut result) => {
            // Only one snapshot is currently supported.
            assert_eq!(result.snapshots.len(), 1);
            let snapshot_path = std::path::PathBuf::from(result.snapshots.remove(0).path);
            (result.inner, snapshot_path)
        }
    };

    // Clear the file from the table. This is hacky, but it ensures that the query
    // actually uses the stored state.
    let table = data_fixture.table_mut("Numbers");
    table.clear();

    let result2 = query
        .clone()
        .with_rocksdb(snapshot_dir.path(), Some(&snapshot_path))
        .run_to_csv(&data_fixture)
        .await
        .unwrap();

    assert_eq!(&result1, &result2);

    insta::assert_snapshot!(result1, @r###"
    _time,_subsort,_key_hash,_key,other_count,key,m,count
    1996-12-20T03:41:00.000000001,18446744073709551615,3650215962958587783,A,1,A,1,2
    1996-12-20T03:41:00.000000001,18446744073709551615,9192031977313001967,C,2,C,8,1
    1996-12-20T03:41:00.000000001,18446744073709551615,11753611437813598533,B,1,B,17,2
    1996-12-20T03:41:00.000000001,18446744073709551615,17976613645339558306,E,,E,5,1
    "###);

    // Run the query again
    let result3 = query
        .with_rocksdb(snapshot_dir.path(), Some(&snapshot_path))
        .run_to_csv(&data_fixture)
        .await
        .unwrap();
    insta::assert_snapshot!(result3, @r###"
    _time,_subsort,_key_hash,_key,other_count,key,m,count
    1996-12-20T03:41:00.000000001,18446744073709551615,3650215962958587783,A,1,A,1,2
    1996-12-20T03:41:00.000000001,18446744073709551615,9192031977313001967,C,2,C,8,1
    1996-12-20T03:41:00.000000001,18446744073709551615,11753611437813598533,B,1,B,17,2
    1996-12-20T03:41:00.000000001,18446744073709551615,17976613645339558306,E,,E,5,1
    "###);
}

#[tokio::test]
async fn test_resumeable_with_preview_rows() {
    // Tests that resumeable with preview rows stores state.
    // Uses a large `preview_rows` that ensures we get all results, so we can
    // compare with a non-resumeable query.
    //
    // TODO: There may be unexpected behavior when the number of input rows
    //       is greater than the batch size.
    let query_fixture = QueryFixture::new(
        "{ key: last(Numbers.key), m: Numbers.m, count: count(Numbers.m) }
            | extend({ other_count: lookup(Numbers.other, $input.count)})",
    );
    let result = assert_final_incremental_same_as_complete(
        query_fixture.with_preview_rows(100),
        TableConfig::new_with_table_source(
            "Numbers",
            &Uuid::new_v4(),
            "time",
            Some("subsort"),
            "key",
            "",
        ),
        indoc! {"
    time,subsort,key,m,other
    1996-12-19T18:38:57-08:00,0,E,5,A
    1996-12-19T18:39:57-08:00,0,A,5,E
    1996-12-19T18:39:58-08:00,0,B,24,B
    1996-12-19T18:39:59-08:00,0,B,17,A
    1996-12-19T19:40:00-08:00,0,C,8,B
    1996-12-19T19:41:00-08:00,0,A,1,C
    "},
        indoc! {"
    time,subsort,key,m,other
    1996-12-19T20:40:01-08:00,0,D,12,A
    1996-12-19T20:40:02-08:00,0,B,2,B
    1996-12-19T20:40:03-08:00,0,D,2,A
    1996-12-19T21:41:04-08:00,0,D,2,B
    1996-12-19T21:42:05-08:00,0,A,3,D
"},
    )
    .await;

    insta::assert_snapshot!(result, @r###"
    _time,_subsort,_key_hash,_key,other_count,key,m,count
    1996-12-20T05:42:05.000000001,18446744073709551615,3650215962958587783,A,3,A,3,3
    1996-12-20T05:42:05.000000001,18446744073709551615,9192031977313001967,C,2,C,8,1
    1996-12-20T05:42:05.000000001,18446744073709551615,11430173353997062025,D,3,D,2,3
    1996-12-20T05:42:05.000000001,18446744073709551615,11753611437813598533,B,3,B,2,3
    1996-12-20T05:42:05.000000001,18446744073709551615,17976613645339558306,E,,E,5,1
    "###);
}

#[tokio::test]
async fn test_shift_until() {
    let query_fixture = QueryFixture::new("{ key: Numbers.key } | shift_until(count(Numbers) > 3)");
    let result = assert_final_incremental_same_as_complete(
        query_fixture,
        TableConfig::new_with_table_source(
            "Numbers",
            &Uuid::new_v4(),
            "time",
            Some("subsort"),
            "key",
            "",
        ),
        indoc! {"
        time,subsort,key,m,n
        1996-12-19T16:39:57-08:00,0,A,5,10
        1996-12-19T16:39:58-08:00,0,B,24,3
        1996-12-19T16:39:59-08:00,0,A,17,6
        1996-12-19T16:40:00-08:00,0,A,,9
        "},
        indoc! {"
        time,subsort,key,m,n
        1996-12-19T16:40:01-08:00,0,A,12,
        1997-12-19T16:40:01-08:00,0,B,2,5
        1997-12-19T16:40:02-08:00,0,B,2,
        1996-12-19T16:40:02-08:00,0,B,2,
        1996-12-19T16:40:03-08:00,0,A,,
    "},
    )
    .await;

    insta::assert_snapshot!(result, @r###"
    _time,_subsort,_key_hash,_key,key
    1997-12-20T00:40:02.000000001,18446744073709551615,3650215962958587783,A,A
    1997-12-20T00:40:02.000000001,18446744073709551615,11753611437813598533,B,B
    "###);
}

#[tokio::test]
#[ignore = "Persisting partially processed input files unsupported"]
async fn test_resumeable_partial_overlap() {
    // Test that restoring from a snapshot that partly overlaps a file
    // works by re-reading only those rows after the snapshot.
    todo!()
}

#[tokio::test]
#[ignore = "verify final tick behavior with resumeable"]
async fn test_resumeable_persist_before_final() {
    // Test that behavior is correct and snapshots are made before
    // processing the "final" ticks.
    todo!()
}
