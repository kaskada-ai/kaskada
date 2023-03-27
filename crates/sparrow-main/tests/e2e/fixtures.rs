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
pub(crate) fn i64_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new(
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
pub(crate) fn f64_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new(
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
pub(crate) fn strings_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new(
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
        .unwrap()
}

pub(crate) fn top_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new("Table", &Uuid::new_v4(), "time", Some("subsort"), "key", ""),
            indoc! {"
    time,subsort,key,str,i64,f64
    1996-12-19T16:39:57-08:00,0,A,value1,1,2.0
    1996-12-19T16:40:57-08:00,0,A,value2,1,2.0
    1996-12-19T16:41:57-08:00,0,A,value2,1,2.0
    1996-12-19T16:42:57-08:00,0,A,value3,1,2.0
    1996-12-19T16:43:57-08:00,0,A,value3,1,2.0
    1996-12-19T16:44:57-08:00,0,A,value1,1,2.0
    1996-12-19T16:46:57-08:00,0,A,value2,1,2.0
    "},
        )
        .unwrap()
}

/// Create a simple table with two boolean columns, `a` and `b`.
///
/// ```csv
/// ```
pub(crate) fn boolean_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new(
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
pub(crate) fn timestamp_ns_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new("Times", &Uuid::new_v4(), "time", Some("subsort"), "key", ""),
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
        .unwrap()
}
