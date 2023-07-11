//! Fixtures for testin
use indoc::indoc;
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::DataFixture;

/// Create a simple table with two `i64` columns `m` and `n`.
///
/// ```csv
/// time,subsort,key,m,n
/// 1996-12-19T16:39:57-08:00,0,A,5,10
/// 1996-12-19T16:39:58-08:00,0,B,24,3
/// 1996-12-19T16:39:59-08:00,0,A,17,6
/// 1996-12-19T16:40:00-08:00,0,A,,9
/// 1996-12-19T16:40:01-08:00,0,A,12,
/// 1996-12-19T16:40:02-08:00,0,A,,
/// ```
pub(crate) async fn i64_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new_with_table_source(
                "Numbers",
                &Uuid::new_v4(),
                "time",
                Some("subsort"),
                "key",
                "Numbers",
            ),
            indoc! {"
    time,subsort,key,m,n
    1996-12-19T16:39:57-08:00,0,A,5,10
    1996-12-19T16:39:58-08:00,0,B,24,3
    1996-12-19T16:39:59-08:00,0,A,17,6
    1996-12-19T16:40:00-08:00,0,A,,9
    1996-12-19T16:40:01-08:00,0,A,12,
    1996-12-19T16:40:02-08:00,0,A,,
    "},
        )
        .await
        .unwrap()
}

/// Create a simple table with two `f64` columns `m` and `n`.
///
/// ```csv
/// time,subsort,key,m,n
/// 1996-12-19T16:39:57-08:00,0,A,5.2,10
/// 1996-12-19T16:39:58-08:00,0,B,24.3,3.9
/// 1996-12-19T16:39:59-08:00,0,A,17.6,6.2
/// 1996-12-19T16:40:00-08:00,0,A,,9.25
/// 1996-12-19T16:40:01-08:00,0,A,12.4,
/// 1996-12-19T16:40:02-08:00,0,A,,
/// ```
pub(crate) async fn f64_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
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
    1996-12-19T16:39:57-08:00,0,A,5.2,10
    1996-12-19T16:39:58-08:00,0,B,24.3,3.9
    1996-12-19T16:39:59-08:00,0,A,17.6,6.2
    1996-12-19T16:40:00-08:00,0,A,,9.25
    1996-12-19T16:40:01-08:00,0,A,12.4,
    1996-12-19T16:40:02-08:00,0,A,,
    "},
        )
        .await
        .unwrap()
}

/// Create a simple table with a string column `s` and an integer column `n` (in
/// range for indices) `n`.
///
/// ```csv
/// time,subsort,key,s,n
/// 1996-12-19T16:39:57-08:00,0,A,hEllo,0
/// 1996-12-19T16:40:57-08:00,0,B,World,5
/// 1996-12-19T16:41:57-08:00,0,B,hello world,-2
/// 1996-12-19T16:42:57-08:00,0,B,,-2
/// 1996-12-19T16:43:57-08:00,0,B,,2
/// 1996-12-19T16:44:57-08:00,0,B,goodbye,
/// ```
pub(crate) async fn strings_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new_with_table_source(
                "Strings",
                &Uuid::new_v4(),
                "time",
                Some("subsort"),
                "key",
                "",
            ),
            indoc! {"
    time,subsort,key,s,n,t
    1996-12-19T16:39:57-08:00,0,A,hEllo,0,hEllo
    1996-12-19T16:40:57-08:00,0,B,World,5,world
    1996-12-19T16:41:57-08:00,0,B,hello world,-2,hello world
    1996-12-19T16:42:57-08:00,0,B,,-2,greetings
    1996-12-19T16:43:57-08:00,0,B,,2,salutations
    1996-12-19T16:44:57-08:00,0,B,goodbye,,
    "},
        )
        .await
        .unwrap()
}

/// Create a simple table with two boolean columns, `a` and `b`.
///
/// ```csv
/// ```
pub(crate) async fn boolean_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new_with_table_source(
                "Booleans",
                &Uuid::new_v4(),
                "time",
                Some("subsort"),
                "key",
                "",
            ),
            indoc! {"
    time,subsort,key,a,b
    1996-12-19T16:39:57-08:00,0,A,true,true
    1996-12-19T16:40:57-08:00,0,B,false,false
    1996-12-19T16:41:57-08:00,0,B,,true
    1996-12-19T16:42:57-08:00,0,B,true,false
    1996-12-19T16:43:57-08:00,0,B,false,true
    1996-12-19T16:44:57-08:00,0,B,false,
    1996-12-19T16:45:57-08:00,0,B,,
    "},
        )
        .await
        .unwrap()
}

/// Create a simple table with a time column ranging over a wider varitey
/// of times. This is useful for testing functions on `timestamp_ns`.
///
/// ```csv
/// time,subsort,key,n,m,other_time
/// 1994-12-19T16:39:57-08:00,0,A,2,4,2003-12-19T16:39:57-08:00
/// 1995-10-19T16:40:57-08:00,0,B,4,3,1994-11-19T16:39:57-08:00
/// 1996-08-19T16:41:57-08:00,0,B,5,,1998-12-19T16:39:57-08:00
/// 1997-12-11T16:42:57-08:00,0,B,,,1992-12-19T16:39:57-08:00
/// 1998-12-12T16:43:57-08:00,0,B,8,8,
/// 2004-12-05T16:44:57-08:00,0,B,23,11,1994-12-19T16:39:57-08:00
/// ```
pub(crate) async fn timestamp_ns_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new_with_table_source(
                "Times",
                &Uuid::new_v4(),
                "time",
                Some("subsort"),
                "key",
                "",
            ),
            indoc! {"
    time,subsort,key,n,m,other_time,fruit
    1994-12-19T16:39:57-08:00,0,A,2,4,2003-12-19T16:39:57-08:00,pear
    1995-10-19T16:40:57-08:00,0,B,4,3,1994-11-19T16:39:57-08:00,watermelon
    1996-08-19T16:41:57-08:00,0,B,5,,1998-12-19T16:39:57-08:00,mango
    1997-12-11T16:42:57-08:00,0,B,,,1992-12-19T16:39:57-08:00,
    1998-12-12T16:43:57-08:00,0,B,8,8,,
    2004-12-05T16:44:57-08:00,0,B,23,11,1994-12-19T16:39:57-08:00,mango
    "},
        )
        .await
        .unwrap()
}

/// Create a simple table with a collection type (map).
///
/// ```json
/// {"time": 2000, "key": 1, "e0": {"f1": 0,  "f2": 22}, "e1": 1,  "e2": 2.7,  "e3": "f1" }
/// {"time": 3000, "key": 1, "e0": {"f1": 1,  "f2": 10}, "e1": 2,  "e2": 3.8,  "e3": "f2" }
/// {"time": 3000, "key": 1, "e0": {"f1": 5,  "f2": 3},  "e1": 42, "e2": 4.0,  "e3": "f3" }
/// {"time": 3000, "key": 1, "e0": {"f2": 13},           "e1": 42, "e2": null, "e3": "f2" }
/// {"time": 4000, "key": 1, "e0": {"f1": 15, "f3": 11}, "e1": 3,  "e2": 7,    "e3": "f3" }
/// ```
pub(crate) async fn collection_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_files(
            TableConfig::new_with_table_source("Input", &Uuid::new_v4(), "time", None, "key", ""),
            &[&"parquet/data_with_map.parquet"],
        )
        .await
        .unwrap()
}
