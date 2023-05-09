//! Tests based on queries in the churn notebook.

use arrow::array::{StringArray, TimestampMicrosecondArray};
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::{DataFixture, QueryFixture};

/// Create a table from a Parquet Decimal column backed by a fixed length array.
///
/// This uses multiple files to ensure we trigger merge code which may have
/// problems with decimal columns.
///
/// This uses a Parquet file produced by the following Pandas code:
///
/// ```python
/// import pandas as pd
/// from decimal import Decimal
///
/// data = [
///   {'time': '1996-12-19T16:39:57-08:00', 'subsort': 0, 'key': 'A', 'm': Decimal('5.2'),  'n': Decimal('10.7'),  'x': 5 },
///   {'time': '1996-12-19T16:39:58-08:00', 'subsort': 0, 'key': 'B', 'm': Decimal('24.3'), 'n': Decimal('3.8'),   'x': 8  },
///   {'time': '1996-12-19T16:39:59-08:00', 'subsort': 0, 'key': 'A', 'm': Decimal('17.4'), 'n': Decimal('10.92'), 'x': 10 },
///   {'time': '1996-12-19T16:40:00-08:00', 'subsort': 0, 'key': 'A',                       'n': Decimal('9.8')            },
///   {'time': '1996-12-19T16:40:01-08:00', 'subsort': 0, 'key': 'A', 'm': Decimal('12.7'),                        'x': 11 },
///   {'time': '1996-12-19T16:40:02-08:00', 'subsort': 0, 'key': 'A',                                                      }]
/// df = pd.DataFrame.from_records(data)
/// df['time'] = pd.to_datetime(df['time'])
/// df.to_parquet(path = 'decimal_fixed_len_part1.parquet',
///               engine='pyarrow',
///               allow_truncated_timestamps=True,
///               use_deprecated_int96_timestamps=True)
///
/// data = [
///   {'time': '1997-12-19T16:39:57-08:00', 'subsort': 0, 'key': 'A', 'm': Decimal('5.2'),  'n': Decimal('10.7'),  'x': 5 },
///   {'time': '1997-12-19T16:39:58-08:00', 'subsort': 0, 'key': 'B', 'm': Decimal('24.3'), 'n': Decimal('3.8'),   'x': 8  },
///   {'time': '1997-12-19T16:39:59-08:00', 'subsort': 0, 'key': 'A', 'm': Decimal('17.4'), 'n': Decimal('10.92'), 'x': 10 },
///   {'time': '1997-12-19T16:40:00-08:00', 'subsort': 0, 'key': 'A',                       'n': Decimal('9.8')            },
///   {'time': '1997-12-19T16:40:01-08:00', 'subsort': 0, 'key': 'A', 'm': Decimal('12.7'),                        'x': 11 },
///   {'time': '1997-12-19T16:40:02-08:00', 'subsort': 0, 'key': 'A',                                                      }]
/// df = pd.DataFrame.from_records(data)
/// df['time'] = pd.to_datetime(df['time'])
/// df.to_parquet(path = 'decimal_fixed_len_part2.parquet',
///               engine='pyarrow',
///               allow_truncated_timestamps=True,
///               use_deprecated_int96_timestamps=True)
/// ```
///
/// `pqrs` reports the Parquet schema as follows. Specifically, note that it
/// uses `FIXED_LEN_BYTE_ARRAY` as the underyling type.
///
/// ```no_run
/// message schema {
///   OPTIONAL INT64 time (TIMESTAMP(MICROS,true));
///   OPTIONAL INT64 subsort;
///   OPTIONAL BYTE_ARRAY key (STRING);
///   OPTIONAL FIXED_LEN_BYTE_ARRAY (2) m (DECIMAL(3,1));
///   OPTIONAL FIXED_LEN_BYTE_ARRAY (2) n (DECIMAL(4,2));
/// }
/// ```
///
/// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal
pub(crate) async fn decimal_fixed_len_multipart_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_files(
            TableConfig::new_with_table_source(
                "Numbers",
                &Uuid::new_v4(),
                "time",
                Some("subsort"),
                "key",
                "",
            ),
            &[
                "regressions/decimal_fixed_len_part1.parquet",
                "regressions/decimal_fixed_len_part2.parquet",
            ],
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_decimal_fixed_len_ignored() {
    // This test shows that a query with decimal columns and multiple files works.
    // This requires ignoring the fields because they can't be merged.
    insta::assert_snapshot!(QueryFixture::new("Numbers").run_to_csv(&decimal_fixed_len_multipart_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,time,subsort,key,x
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,1996-12-20T00:39:57.000000000,0,A,5.0
    1996-12-20T00:39:58.000000000,9223372036854775808,11753611437813598533,B,1996-12-20T00:39:58.000000000,0,B,8.0
    1996-12-20T00:39:59.000000000,9223372036854775808,3650215962958587783,A,1996-12-20T00:39:59.000000000,0,A,10.0
    1996-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,1996-12-20T00:40:00.000000000,0,A,
    1996-12-20T00:40:01.000000000,9223372036854775808,3650215962958587783,A,1996-12-20T00:40:01.000000000,0,A,11.0
    1996-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,1996-12-20T00:40:02.000000000,0,A,
    1997-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,1997-12-20T00:39:57.000000000,0,A,5.0
    1997-12-20T00:39:58.000000000,9223372036854775808,11753611437813598533,B,1997-12-20T00:39:58.000000000,0,B,8.0
    1997-12-20T00:39:59.000000000,9223372036854775808,3650215962958587783,A,1997-12-20T00:39:59.000000000,0,A,10.0
    1997-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,1997-12-20T00:40:00.000000000,0,A,
    1997-12-20T00:40:01.000000000,9223372036854775808,3650215962958587783,A,1997-12-20T00:40:01.000000000,0,A,11.0
    1997-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,1997-12-20T00:40:02.000000000,0,A,
    "###)
}

#[tokio::test]
async fn test_decimal_fixed_len_unused() {
    insta::assert_snapshot!(QueryFixture::new("{ x: Numbers.x, sqrt_x: sqrt(Numbers.x) }").run_to_csv(&decimal_fixed_len_multipart_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,x,sqrt_x
    1996-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,5.0,2.23606797749979
    1996-12-20T00:39:58.000000000,9223372036854775808,11753611437813598533,B,8.0,2.8284271247461903
    1996-12-20T00:39:59.000000000,9223372036854775808,3650215962958587783,A,10.0,3.1622776601683795
    1996-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,,
    1996-12-20T00:40:01.000000000,9223372036854775808,3650215962958587783,A,11.0,3.3166247903554
    1996-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,,
    1997-12-20T00:39:57.000000000,9223372036854775808,3650215962958587783,A,5.0,2.23606797749979
    1997-12-20T00:39:58.000000000,9223372036854775808,11753611437813598533,B,8.0,2.8284271247461903
    1997-12-20T00:39:59.000000000,9223372036854775808,3650215962958587783,A,10.0,3.1622776601683795
    1997-12-20T00:40:00.000000000,9223372036854775808,3650215962958587783,A,,
    1997-12-20T00:40:01.000000000,9223372036854775808,3650215962958587783,A,11.0,3.3166247903554
    1997-12-20T00:40:02.000000000,9223372036854775808,3650215962958587783,A,,
    "###)
}

#[tokio::test]
// Regression test for timestamps with microseconds. See <https://gitlab.com/kaskada/kaskada/-/issues/463>
async fn test_timestamp_microseconds() {
    let table = crate::ParquetTableBuilder::new()
        .add_column(
            "time",
            false,
            TimestampMicrosecondArray::from(vec![1000, 1001, 1002, 1003]),
        )
        .add_column("user_id", true, StringArray::from(vec!["a", "b", "c", "d"]));

    let data_fixture = DataFixture::new()
        .with_table_from_parquet(
            TableConfig::new_with_table_source(
                "Events",
                &Uuid::new_v4(),
                "time",
                None,
                "user_id",
                "user",
            ),
            table,
        )
        .await
        .unwrap();

    insta::assert_snapshot!(QueryFixture::new("Events").run_to_csv(&data_fixture).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,time,user_id
    1970-01-01T00:00:00.001000000,15758869519095776131,7636293598395510443,a,1970-01-01T00:00:00.001000000,a
    1970-01-01T00:00:00.001001000,15758869519095776132,2637710838665036908,b,1970-01-01T00:00:00.001001000,b
    1970-01-01T00:00:00.001002000,15758869519095776133,5899024403724905519,c,1970-01-01T00:00:00.001002000,c
    1970-01-01T00:00:00.001003000,15758869519095776134,2459037462255564612,d,1970-01-01T00:00:00.001003000,d
    "###);
}

#[tokio::test]
async fn test_multi_file_purchases() {
    let data_fixture = DataFixture::new()
        .with_table_from_files(
            TableConfig::new_with_table_source(
                "Purchases",
                &Uuid::new_v4(),
                "purchase_time",
                Some("subsort_id"),
                "customer_id",
                "",
            ),
            &[
                "purchases/purchases_part1.parquet",
                "purchases/purchases_part2.parquet",
            ],
        )
        .await
        .unwrap();

    insta::assert_snapshot!(QueryFixture::new("{
        time: Purchases.purchase_time,
        entity: Purchases.customer_id,
        max_amount: Purchases.amount | max()
    }").run_to_csv(&data_fixture).await.unwrap(),
    @r###"
    _time,_subsort,_key_hash,_key,time,entity,max_amount
    2020-01-01T00:00:00.000000000,9223372036854775808,10966214875107816766,karen,2020-01-01T00:00:00.000000000,karen,9
    2020-01-01T00:00:00.000000000,9223372036854775809,15119067519137142314,patrick,2020-01-01T00:00:00.000000000,patrick,3
    2020-01-02T00:00:00.000000000,9223372036854775810,10966214875107816766,karen,2020-01-02T00:00:00.000000000,karen,9
    2020-01-02T00:00:00.000000000,9223372036854775811,15119067519137142314,patrick,2020-01-02T00:00:00.000000000,patrick,5
    2020-01-03T00:00:00.000000000,9223372036854775812,10966214875107816766,karen,2020-01-03T00:00:00.000000000,karen,9
    2020-01-03T00:00:00.000000000,9223372036854775813,15119067519137142314,patrick,2020-01-03T00:00:00.000000000,patrick,12
    2020-01-04T00:00:00.000000000,9223372036854775814,15119067519137142314,patrick,2020-01-04T00:00:00.000000000,patrick,5000
    2020-01-04T00:00:00.000000000,9223372036854775815,10966214875107816766,karen,2020-01-04T00:00:00.000000000,karen,9
    2020-01-05T00:00:00.000000000,9223372036854775816,10966214875107816766,karen,2020-01-05T00:00:00.000000000,karen,9
    2020-01-05T00:00:00.000000000,9223372036854775817,15119067519137142314,patrick,2020-01-05T00:00:00.000000000,patrick,5000
    2020-01-06T00:00:00.000000000,9223372036854775808,15119067519137142314,patrick,2020-01-06T00:00:00.000000000,patrick,5000
    2020-01-06T00:00:00.000000000,9223372036854775809,14116511794099673386,spongebob,2020-01-06T00:00:00.000000000,spongebob,7
    2020-01-07T00:00:00.000000000,9223372036854775810,14116511794099673386,spongebob,2020-01-07T00:00:00.000000000,spongebob,34
    2020-01-08T00:00:00.000000000,9223372036854775811,10966214875107816766,karen,2020-01-08T00:00:00.000000000,karen,9
    2020-01-08T00:00:00.000000000,9223372036854775812,15119067519137142314,patrick,2020-01-08T00:00:00.000000000,patrick,5000
    "###);
}
