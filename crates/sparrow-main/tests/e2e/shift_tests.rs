//! Tests for basic shift behaviors.

use indoc::indoc;
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::{DataFixture, QueryFixture};

/// Fixture for testing when.
///
/// Includes a column of every type being and a condition column.
async fn shift_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new_with_table_source(
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
    "},
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_shift_by_months() {
    insta::assert_snapshot!(QueryFixture::new("{ i64: ShiftFixture.i64 | shift_by(months(5)) }").run_to_csv(&shift_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,i64
    1997-05-20T00:39:57.000000000,0,12960666915911099378,A,57
    1997-05-20T00:39:58.000000000,1,2867199309159137213,B,58
    1997-05-20T00:39:59.000000000,2,12960666915911099378,A,59
    1997-05-20T00:40:00.000000000,3,2867199309159137213,B,
    1997-05-20T00:40:01.000000000,4,12960666915911099378,A,
    1997-05-20T00:40:02.000000000,5,12960666915911099378,A,2
    "###)
}

#[tokio::test]
async fn test_shift_by_seconds() {
    insta::assert_snapshot!(QueryFixture::new("{ i64: ShiftFixture.i64 | shift_by(seconds(5)) }").run_to_csv(&shift_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,i64
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,57
    1996-12-20T00:40:03.000000000,1,2867199309159137213,B,58
    1996-12-20T00:40:04.000000000,2,12960666915911099378,A,59
    1996-12-20T00:40:05.000000000,3,2867199309159137213,B,
    1996-12-20T00:40:06.000000000,4,12960666915911099378,A,
    1996-12-20T00:40:07.000000000,5,12960666915911099378,A,2
    "###)
}

#[tokio::test]
async fn test_shift_to_plus_seconds() {
    insta::assert_snapshot!(QueryFixture::new("{ i64: ShiftFixture.i64 | shift_to(add_time(seconds(5), time_of($input))) }").run_to_csv(&shift_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,i64
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,57
    1996-12-20T00:40:03.000000000,1,2867199309159137213,B,58
    1996-12-20T00:40:04.000000000,2,12960666915911099378,A,59
    1996-12-20T00:40:05.000000000,3,2867199309159137213,B,
    1996-12-20T00:40:06.000000000,4,12960666915911099378,A,
    1996-12-20T00:40:07.000000000,5,12960666915911099378,A,2
    "###)
}

#[tokio::test]
async fn test_shift_until_data_i64() {
    insta::assert_snapshot!(QueryFixture::new("{ i64: ShiftFixture.i64 | shift_until(ShiftFixture.cond) }").run_to_csv(&shift_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,i64
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,57
    1996-12-20T00:40:00.000000000,1,2867199309159137213,B,58
    1996-12-20T00:40:00.000000000,2,2867199309159137213,B,
    1996-12-20T00:40:02.000000000,3,12960666915911099378,A,59
    1996-12-20T00:40:02.000000000,4,12960666915911099378,A,
    1996-12-20T00:40:02.000000000,5,12960666915911099378,A,2
    "###)
}

#[tokio::test]
async fn test_shift_until_data_boolean() {
    insta::assert_snapshot!(QueryFixture::new("{ bool: ShiftFixture.bool | shift_until(ShiftFixture.cond) }").run_to_csv(&shift_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,bool
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,false
    1996-12-20T00:40:00.000000000,1,2867199309159137213,B,true
    1996-12-20T00:40:00.000000000,2,2867199309159137213,B,
    1996-12-20T00:40:02.000000000,3,12960666915911099378,A,true
    1996-12-20T00:40:02.000000000,4,12960666915911099378,A,
    1996-12-20T00:40:02.000000000,5,12960666915911099378,A,
    "###)
}

#[tokio::test]
async fn test_shift_until_data_string() {
    insta::assert_snapshot!(QueryFixture::new("{ string: ShiftFixture.string | shift_until(ShiftFixture.cond) }").run_to_csv(&shift_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,string
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,hello
    1996-12-20T00:40:00.000000000,1,2867199309159137213,B,world
    1996-12-20T00:40:00.000000000,2,2867199309159137213,B,
    1996-12-20T00:40:02.000000000,3,12960666915911099378,A,world
    1996-12-20T00:40:02.000000000,4,12960666915911099378,A,
    1996-12-20T00:40:02.000000000,5,12960666915911099378,A,hello
    "###)
}

#[tokio::test]
async fn test_shift_until_data_record() {
    insta::assert_snapshot!(QueryFixture::new("ShiftFixture | shift_until($input.cond)").run_to_csv(&shift_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,time,subsort,key,cond,bool,i64,string,other_time
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,1996-12-20T00:39:57.000000000,0,A,true,false,57,hello,1997-12-20T00:39:57.000000000
    1996-12-20T00:40:00.000000000,1,2867199309159137213,B,1996-12-20T00:39:58.000000000,0,B,false,true,58,world,1997-10-20T00:39:57.000000000
    1996-12-20T00:40:00.000000000,2,2867199309159137213,B,1996-12-20T00:40:00.000000000,0,B,true,,,,2000-12-20T00:39:57.000000000
    1996-12-20T00:40:02.000000000,3,12960666915911099378,A,1996-12-20T00:39:59.000000000,0,A,,true,59,world,1995-12-20T00:39:57.000000000
    1996-12-20T00:40:02.000000000,4,12960666915911099378,A,1996-12-20T00:40:01.000000000,0,A,false,,,,
    1996-12-20T00:40:02.000000000,5,12960666915911099378,A,1996-12-20T00:40:02.000000000,0,A,true,,2,hello,1999-01-20T00:39:57.000000000
    "###)
}

#[tokio::test]
#[ignore = "Shift to literal unsupported"]
async fn test_shift_to_literal_i64() {
    insta::assert_snapshot!(QueryFixture::new("{ n: ShiftFixture.i64 | shift_to(\"1996-12-19T16:40:00-08:00\") }").run_to_csv(&shift_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,n
    1996-12-20T00:40:00.000000000,0,16072519723445549088,57
    1996-12-20T00:40:00.000000000,1,18113259709342437355,58
    1996-12-20T00:40:00.000000000,2,16072519723445549088,59
    1996-12-20T00:40:00.000000000,3,18113259709342437355,
    "###)
}

#[tokio::test]
#[ignore = "Shift to literal unsupported"]
async fn test_shift_to_literal_boolean() {
    insta::assert_snapshot!(QueryFixture::new("{ bool: ShiftFixture.bool | shift_to(\"1996-12-19T16:40:00-08:00\") }").run_to_csv(&shift_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,bool
    1996-12-20T00:40:00.000000000,0,16072519723445549088,false
    1996-12-20T00:40:00.000000000,1,18113259709342437355,true
    1996-12-20T00:40:00.000000000,2,16072519723445549088,true
    1996-12-20T00:40:00.000000000,3,18113259709342437355,
    "###)
}

#[tokio::test]
#[ignore = "https://gitlab.com/kaskada/kaskada/-/issues/572"]
async fn test_shift_to_literal_string() {
    insta::assert_snapshot!(QueryFixture::new("{ string: ShiftFixture.string | shift_to(\"1996-12-19T16:40:00-08:00\") }").run_to_csv(&shift_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,string
    1996-12-20T00:40:00.000000000,0,16072519723445549088,hello
    1996-12-20T00:40:00.000000000,1,18113259709342437355,world
    1996-12-20T00:40:00.000000000,2,16072519723445549088,world
    1996-12-20T00:40:00.000000000,3,18113259709342437355,
    "###)
}

#[tokio::test]
#[ignore = "https://gitlab.com/kaskada/kaskada/-/issues/572"]
async fn test_shift_to_literal_record() {
    insta::assert_snapshot!(QueryFixture::new("ShiftFixture | shift_to(\"1996-12-19T16:40:00-08:00\")").run_to_csv(&shift_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,time,subsort,key,cond,bool,i64,string,other_time
    1996-12-20T00:40:00.000000000,0,16072519723445549088,1996-12-19T16:39:57-08:00,0,A,true,false,57,hello,1997-12-19T16:39:57-08:00
    1996-12-20T00:40:00.000000000,1,18113259709342437355,1996-12-19T16:39:58-08:00,0,B,false,true,58,world,1997-10-19T16:39:57-08:00
    1996-12-20T00:40:00.000000000,2,16072519723445549088,1996-12-19T16:39:59-08:00,0,A,,true,59,world,1995-12-19T16:39:57-08:00
    1996-12-20T00:40:00.000000000,3,18113259709342437355,1996-12-19T16:40:00-08:00,0,B,true,,,,2000-12-19T16:39:57-08:00
    "###)
}

#[tokio::test]
async fn test_shift_to_data_i64() {
    insta::assert_snapshot!(QueryFixture::new("{ i64: ShiftFixture.i64 | shift_to(ShiftFixture.other_time) }").run_to_csv(&shift_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,i64
    1997-10-20T00:39:57.000000000,0,2867199309159137213,B,58
    1997-12-20T00:39:57.000000000,1,12960666915911099378,A,57
    1999-01-20T00:39:57.000000000,2,12960666915911099378,A,2
    2000-12-20T00:39:57.000000000,3,2867199309159137213,B,
    "###)
}

#[tokio::test]
async fn test_shift_to_data_boolean() {
    insta::assert_snapshot!(QueryFixture::new("{ bool: ShiftFixture.bool | shift_to(ShiftFixture.other_time) }").run_to_csv(&shift_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,bool
    1997-10-20T00:39:57.000000000,0,2867199309159137213,B,true
    1997-12-20T00:39:57.000000000,1,12960666915911099378,A,false
    1999-01-20T00:39:57.000000000,2,12960666915911099378,A,
    2000-12-20T00:39:57.000000000,3,2867199309159137213,B,
    "###)
}

#[tokio::test]
async fn test_shift_to_data_string() {
    insta::assert_snapshot!(QueryFixture::new("{ string: ShiftFixture.string | shift_to(ShiftFixture.other_time) }").run_to_csv(&shift_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,string
    1997-10-20T00:39:57.000000000,0,2867199309159137213,B,world
    1997-12-20T00:39:57.000000000,1,12960666915911099378,A,hello
    1999-01-20T00:39:57.000000000,2,12960666915911099378,A,hello
    2000-12-20T00:39:57.000000000,3,2867199309159137213,B,
    "###)
}

#[tokio::test]
async fn test_shift_to_data_record() {
    insta::assert_snapshot!(QueryFixture::new("ShiftFixture | shift_to(ShiftFixture.other_time)").run_to_csv(&shift_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,time,subsort,key,cond,bool,i64,string,other_time
    1997-10-20T00:39:57.000000000,0,2867199309159137213,B,1996-12-20T00:39:58.000000000,0,B,false,true,58,world,1997-10-20T00:39:57.000000000
    1997-12-20T00:39:57.000000000,1,12960666915911099378,A,1996-12-20T00:39:57.000000000,0,A,true,false,57,hello,1997-12-20T00:39:57.000000000
    1999-01-20T00:39:57.000000000,2,12960666915911099378,A,1996-12-20T00:40:02.000000000,0,A,true,,2,hello,1999-01-20T00:39:57.000000000
    2000-12-20T00:39:57.000000000,3,2867199309159137213,B,1996-12-20T00:40:00.000000000,0,B,true,,,,2000-12-20T00:39:57.000000000
    "###)
}

#[tokio::test]
async fn test_shift_until_false() {
    insta::assert_snapshot!(QueryFixture::new("
        let gt_10 = ShiftFixture.i64 > 10
        let shift_until_gt_10 = ShiftFixture.string | shift_until(gt_10)
        # For the purposes of this test, we want this to be all false.
        let gt_75 = ShiftFixture.i64 > 75
        let shift_until_gt_75 = ShiftFixture.string | shift_until(gt_75)
        in { gt_10, shift_until_gt_10, gt_75, shift_until_gt_75 } | when(gt_10 or gt_75)").run_to_csv(&shift_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,gt_10,shift_until_gt_10,gt_75,shift_until_gt_75
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,true,,false,
    1996-12-20T00:39:58.000000000,9223372036854775808,2867199309159137213,B,true,,false,
    1996-12-20T00:39:59.000000000,9223372036854775808,12960666915911099378,A,true,,false,
    "###)
}

#[tokio::test]
async fn test_shift_until_false_sum() {
    insta::assert_snapshot!(QueryFixture::new("
        let gt_10 = ShiftFixture.i64 > 10
        let shift_until_gt_10 = ShiftFixture.i64 | shift_until(gt_10) | sum()
        # For the purpsoses of this test, we want this to be all false.
        let gt_75 = ShiftFixture.i64 > 75
        let shift_until_gt_75 = ShiftFixture.string | shift_until(gt_75)
        in { gt_10, shift_until_gt_10, gt_75, shift_until_gt_75 }").run_to_csv(&shift_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,gt_10,shift_until_gt_10,gt_75,shift_until_gt_75
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,,57,,
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,true,57,false,
    1996-12-20T00:39:58.000000000,1,2867199309159137213,B,,58,,
    1996-12-20T00:39:58.000000000,9223372036854775808,2867199309159137213,B,true,58,false,
    1996-12-20T00:39:59.000000000,2,12960666915911099378,A,,116,,
    1996-12-20T00:39:59.000000000,9223372036854775808,12960666915911099378,A,true,116,false,
    1996-12-20T00:40:00.000000000,9223372036854775808,2867199309159137213,B,,58,,
    1996-12-20T00:40:01.000000000,9223372036854775808,12960666915911099378,A,,116,,
    1996-12-20T00:40:02.000000000,9223372036854775808,12960666915911099378,A,false,116,false,
    "###)
}

#[tokio::test]
async fn test_shift_to_sparse() {
    // This test (revealed by the catalog tests) catches a case where
    // the batches for the shift to are (for instance) [10, 15] and [20, 30].
    //
    // The second batch attempts to output a shifted value in between 15 and 20,
    // but that isn't within the bounds of that batch. We could handle that by
    // making the time range in the work area contiguous, but for now we pick the
    // lower bound of the shift to outputs based on the first element.
    let data = DataFixture::new()
        .with_table_from_csv(
            TableConfig::new_with_table_source(
                "ShiftFixture",
                &Uuid::new_v4(),
                "time",
                Some("subsort"),
                "key",
                "",
            ),
            indoc! {"
                time,subsort,key,date,condition,n
                1996-03-21T00:00:00-00:00,0,Ben,1996-08-19T00:00:00-00:00,true,1
                1996-04-21T00:00:00-00:00,0,Ryan,1996-07-20T00:00:00-00:00,true,2
                1996-05-21T00:00:00-00:00,0,Ryan,1996-07-22T00:00:00-00:00,false,3
                1996-06-21T00:00:00-00:00,0,Ryan,1996-06-22T00:00:00-00:00,true,4
                1996-07-21T00:00:00-00:00,0,Ben,1996-07-22T00:00:00-00:00,false,5
                1996-08-21T00:00:00-00:00,0,Ben,1996-08-22T00:00:00-00:00,true,6
            "},
        )
        .await
        .unwrap();

    insta::assert_snapshot!(QueryFixture::new("{ result: ShiftFixture.n | shift_to(ShiftFixture.date) }").run_to_csv(&data).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,result
    1996-06-22T00:00:00.000000000,0,4840222152338143374,Ryan,4
    1996-07-20T00:00:00.000000000,1,4840222152338143374,Ryan,2
    1996-07-22T00:00:00.000000000,0,4840222152338143374,Ryan,3
    1996-07-22T00:00:00.000000000,1,16858319356675959811,Ben,5
    1996-08-19T00:00:00.000000000,2,16858319356675959811,Ben,1
    1996-08-22T00:00:00.000000000,0,16858319356675959811,Ben,6
    "###);
}

async fn tables() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new_with_table_source(
                "T",
                &Uuid::new_v4(),
                "time",
                Some("sub_sort"),
                "id",
                "",
            ),
            indoc! {"
        time,sub_sort,id,v
        2000-01-01T00:00:00.000000000,0,a,111
        2000-01-01T01:00:00.000000000,1,c,333
        2000-01-02T00:00:00.000000000,2,b,222"},
        )
        .await
        .unwrap()
}

const Q1: &str = indoc! {"
  let table = T |  shift_to(time_of($input) | add_time(days(10)))
  in lookup(table.id, table)
"};

#[tokio::test]
async fn shift_to_and_lookup() {
    insta::assert_snapshot!(
        QueryFixture::new(Q1)
        .run_to_csv(&tables().await)
        .await.unwrap(),
    @r###"
    _time,_subsort,_key_hash,_key,time,sub_sort,id,v
    2000-01-11T00:00:00.000000000,0,13074916891489937275,a,2000-01-01T00:00:00.000000000,0,a,111
    2000-01-11T01:00:00.000000000,1,298518813902531243,c,2000-01-01T01:00:00.000000000,1,c,333
    2000-01-12T00:00:00.000000000,2,12352002978215245678,b,2000-01-02T00:00:00.000000000,2,b,222
    "###);
}

#[tokio::test]
async fn shift_to() {
    insta::assert_snapshot!(
        QueryFixture::new("Input | shift_to(Input.date)")
        .run_to_csv(
            &DataFixture::new()
                .with_table_from_csv(
                    TableConfig::new_with_table_source("Input", &Uuid::new_v4(), "time", Some("sub_sort"), "key", "grouping"),
                    indoc! ("
                    time,key,sub_sort,date,n
                    1996-03-21T00:00:00-00:00,Ben,0,1996-08-19T00:00:00-00:00,1
                    1996-04-21T00:00:00-00:00,Ryan,1,1996-07-20T00:00:00-00:00,2
                    1996-05-21T00:00:00-00:00,Ryan,2,1996-07-22T00:00:00-00:00,3
                    1996-06-21T00:00:00-00:00,Ryan,3,1996-05-22T00:00:00-00:00,4
                    1996-07-21T00:00:00-00:00,Ben,4,1996-07-22T00:00:00-00:00,5
                    1996-08-21T00:00:00-00:00,Ben,5,1996-08-22T00:00:00-00:00,6
                ")).await.unwrap())
        .await.unwrap(),
        @r###"
    _time,_subsort,_key_hash,_key,time,key,sub_sort,date,n
    1996-07-20T00:00:00.000000000,0,4840222152338143374,Ryan,1996-04-21T00:00:00.000000000,Ryan,1,1996-07-20T00:00:00.000000000,2
    1996-07-22T00:00:00.000000000,0,4840222152338143374,Ryan,1996-05-21T00:00:00.000000000,Ryan,2,1996-07-22T00:00:00.000000000,3
    1996-07-22T00:00:00.000000000,1,16858319356675959811,Ben,1996-07-21T00:00:00.000000000,Ben,4,1996-07-22T00:00:00.000000000,5
    1996-08-19T00:00:00.000000000,2,16858319356675959811,Ben,1996-03-21T00:00:00.000000000,Ben,0,1996-08-19T00:00:00.000000000,1
    1996-08-22T00:00:00.000000000,0,16858319356675959811,Ben,1996-08-21T00:00:00.000000000,Ben,5,1996-08-22T00:00:00.000000000,6
    "###);
}
