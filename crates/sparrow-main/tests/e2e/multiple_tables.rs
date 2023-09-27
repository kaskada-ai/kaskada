//! Tests that exercise 2 or more table joins e.g., `a.x + b.y`

use indoc::indoc;
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::fixture::QueryFixture;
use crate::DataFixture;

static PURCHASE_CSV: &str = indoc!(
    "
    time,subsort,key,total
    2022-01-02T16:39:57-08:00,0,A,1
    2022-01-02T17:39:57-08:00,0,B,10
    2022-01-02T18:39:57-08:00,0,A,2
    2022-01-02T19:39:57-08:00,0,B,20
    2022-01-02T20:39:57-08:00,0,C,100
    2022-01-02T21:39:57-08:00,0,C,200
    2022-01-02T22:39:57-08:00,0,A,3
    "
);

// same as PURCHASE_CSV but +1 day
static NEXT_DAY_PURCHASE_CSV: &str = indoc!(
    "
    time,subsort,key,total
    2022-01-03T10:39:57-08:00,0,B,5
    2022-01-03T12:39:57-08:00,0,A,4
    2022-01-03T13:39:57-08:00,0,B,15
    2022-01-03T14:39:57-08:00,0,A,8
    2022-01-03T15:39:57-08:00,0,C,100
    2022-01-03T16:39:57-08:00,0,C,200
    2022-01-03T17:39:57-08:00,0,B,20
    "
);

// same as PURCHASE_CSV but no overlapping keys
static DIFFERENT_KEY_SAME_DAY: &str = indoc!(
    "
    time,subsort,key,total
    2022-01-02T16:39:57-08:00,0,X,1
    2022-01-02T17:39:57-08:00,0,Y,10
    2022-01-02T18:39:57-08:00,0,X,2
    2022-01-02T19:39:57-08:00,0,Y,20
    2022-01-02T20:39:57-08:00,0,Z,100
    2022-01-02T21:39:57-08:00,0,Z,200
    2022-01-02T22:39:57-08:00,0,X,3
    "
);

// same as PURCHASE_CSV with some (not all) keys overlapping
static OVERLAPPING_KEYS: &str = indoc!(
    "
    time,subsort,key,total
    2022-01-02T16:39:57-08:00,0,X,1
    2022-01-02T17:39:57-08:00,0,B,10
    2022-01-02T18:39:57-08:00,0,A,2
    2022-01-02T19:39:57-08:00,0,Y,20
    2022-01-02T20:39:57-08:00,0,C,100
    2022-01-02T21:39:57-08:00,0,Z,200
    2022-01-02T22:39:57-08:00,0,X,3
    "
);

// includes all of PURCHASE_CSV + more rows
static SUPERSET: &str = indoc!(
    "
    time,subsort,key,total
    2022-01-02T16:37:57-08:00,0,B,1
    2022-01-02T16:38:57-08:00,0,A,1
    2022-01-02T16:39:57-08:00,0,A,1
    2022-01-02T16:39:58-08:00,0,A,666
    2022-01-02T17:39:57-08:00,0,B,10
    2022-01-02T17:40:57-08:00,0,B,1000
    2022-01-02T18:38:57-08:00,0,A,777
    2022-01-02T18:39:57-08:00,0,A,2
    2022-01-02T18:44:57-08:00,0,A,888
    2022-01-02T19:39:57-08:00,0,B,20
    2022-01-02T20:39:57-08:00,0,C,100
    2022-01-02T21:39:57-08:00,0,C,200
    2022-01-02T22:39:57-08:00,0,A,3
    2022-01-02T22:43:57-08:00,0,B,3
    2022-01-02T22:45:57-08:00,0,C,3
    "
);

fn make_table_config(name: &str) -> TableConfig {
    TableConfig::new_with_table_source(
        name,
        &Uuid::new_v4(),
        "time",
        Some("subsort"),
        "key",
        "Purchases",
    )
}

async fn purchase_table() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(make_table_config("Purchases"), PURCHASE_CSV)
        .await
        .unwrap()
}

async fn purchase_tables_two_days() -> DataFixture {
    purchase_table()
        .await
        .with_table_from_csv(make_table_config("NDPurchases"), NEXT_DAY_PURCHASE_CSV)
        .await
        .unwrap()
}

async fn purchase_tables_different_keys() -> DataFixture {
    purchase_table()
        .await
        .with_table_from_csv(make_table_config("DKPurchases"), DIFFERENT_KEY_SAME_DAY)
        .await
        .unwrap()
}

async fn purchase_tables_overlapping_keys() -> DataFixture {
    purchase_table()
        .await
        .with_table_from_csv(make_table_config("OKPurchases"), OVERLAPPING_KEYS)
        .await
        .unwrap()
}

async fn purchase_tables_superset() -> DataFixture {
    purchase_table()
        .await
        .with_table_from_csv(make_table_config("SSPurchases"), SUPERSET)
        .await
        .unwrap()
}
#[tokio::test]
// joins the purchases table to a copy of its self and adds totals
async fn test_identical_table_join() {
    let test_data = purchase_table()
        .await
        .with_table_from_csv(make_table_config("P2"), PURCHASE_CSV)
        .await
        .unwrap();

    insta::assert_snapshot!(QueryFixture::new(
            "{ t1_val: Purchases.total, t2_val: P2.total, sum: Purchases.total + P2.total}"
        )
        .run_to_csv(&test_data).await.unwrap(),
        @r###"
    _time,_subsort,_key_hash,_key,t1_val,t2_val,sum
    2022-01-03T00:39:57.000000000,0,12960666915911099378,A,1,1,2
    2022-01-03T01:39:57.000000000,0,2867199309159137213,B,10,10,20
    2022-01-03T02:39:57.000000000,0,12960666915911099378,A,2,2,4
    2022-01-03T03:39:57.000000000,0,2867199309159137213,B,20,20,40
    2022-01-03T04:39:57.000000000,0,2521269998124177631,C,100,100,200
    2022-01-03T05:39:57.000000000,0,2521269998124177631,C,200,200,400
    2022-01-03T06:39:57.000000000,0,12960666915911099378,A,3,3,6
    "### 
    );
}

#[tokio::test]
async fn test_tables_different_dates() {
    insta::assert_snapshot!(
        QueryFixture::new(
        "{t1_val: Purchases.total, t2_val: NDPurchases.total, sum: Purchases.total + \
         NDPurchases.total}")
         .run_to_csv(&purchase_tables_two_days().await).await.unwrap(),
         @r###"
    _time,_subsort,_key_hash,_key,t1_val,t2_val,sum
    2022-01-03T00:39:57.000000000,0,12960666915911099378,A,1,,
    2022-01-03T01:39:57.000000000,0,2867199309159137213,B,10,,
    2022-01-03T02:39:57.000000000,0,12960666915911099378,A,2,,
    2022-01-03T03:39:57.000000000,0,2867199309159137213,B,20,,
    2022-01-03T04:39:57.000000000,0,2521269998124177631,C,100,,
    2022-01-03T05:39:57.000000000,0,2521269998124177631,C,200,,
    2022-01-03T06:39:57.000000000,0,12960666915911099378,A,3,,
    2022-01-03T18:39:57.000000000,0,2867199309159137213,B,,5,
    2022-01-03T20:39:57.000000000,0,12960666915911099378,A,,4,
    2022-01-03T21:39:57.000000000,0,2867199309159137213,B,,15,
    2022-01-03T22:39:57.000000000,0,12960666915911099378,A,,8,
    2022-01-03T23:39:57.000000000,0,2521269998124177631,C,,100,
    2022-01-04T00:39:57.000000000,0,2521269998124177631,C,,200,
    2022-01-04T01:39:57.000000000,0,2867199309159137213,B,,20,
    "###);
}

#[tokio::test]
async fn test_tables_no_overlapping_keys() {
    insta::assert_snapshot!(
        QueryFixture::new(
        "{t1_val: Purchases.total, t2_val: DKPurchases.total, sum: Purchases.total + \
         DKPurchases.total}")
         .run_to_csv(&purchase_tables_different_keys().await).await.unwrap(),
         @r###"
    _time,_subsort,_key_hash,_key,t1_val,t2_val,sum
    2022-01-03T00:39:57.000000000,0,5844668342709334339,X,,1,
    2022-01-03T00:39:57.000000000,0,12960666915911099378,A,1,,
    2022-01-03T01:39:57.000000000,0,2867199309159137213,B,10,,
    2022-01-03T01:39:57.000000000,0,8493950773958210388,Y,,10,
    2022-01-03T02:39:57.000000000,0,5844668342709334339,X,,2,
    2022-01-03T02:39:57.000000000,0,12960666915911099378,A,2,,
    2022-01-03T03:39:57.000000000,0,2867199309159137213,B,20,,
    2022-01-03T03:39:57.000000000,0,8493950773958210388,Y,,20,
    2022-01-03T04:39:57.000000000,0,2521269998124177631,C,100,,
    2022-01-03T04:39:57.000000000,0,5050198837546418057,Z,,100,
    2022-01-03T05:39:57.000000000,0,2521269998124177631,C,200,,
    2022-01-03T05:39:57.000000000,0,5050198837546418057,Z,,200,
    2022-01-03T06:39:57.000000000,0,5844668342709334339,X,,3,
    2022-01-03T06:39:57.000000000,0,12960666915911099378,A,3,,
    "###);
}

#[tokio::test]
async fn test_tables_overlapping_keys() {
    insta::assert_snapshot!(
        QueryFixture::new(
        "{t1_val: Purchases.total, t2_val: OKPurchases.total, sum: Purchases.total + \
         OKPurchases.total}")
         .run_to_csv(&purchase_tables_overlapping_keys().await).await.unwrap(),
         @r###"
    _time,_subsort,_key_hash,_key,t1_val,t2_val,sum
    2022-01-03T00:39:57.000000000,0,5844668342709334339,X,,1,
    2022-01-03T00:39:57.000000000,0,12960666915911099378,A,1,,
    2022-01-03T01:39:57.000000000,0,2867199309159137213,B,10,10,20
    2022-01-03T02:39:57.000000000,0,12960666915911099378,A,2,2,4
    2022-01-03T03:39:57.000000000,0,2867199309159137213,B,20,,
    2022-01-03T03:39:57.000000000,0,8493950773958210388,Y,,20,
    2022-01-03T04:39:57.000000000,0,2521269998124177631,C,100,100,200
    2022-01-03T05:39:57.000000000,0,2521269998124177631,C,200,,
    2022-01-03T05:39:57.000000000,0,5050198837546418057,Z,,200,
    2022-01-03T06:39:57.000000000,0,5844668342709334339,X,,3,
    2022-01-03T06:39:57.000000000,0,12960666915911099378,A,3,,
    "###);
}

#[tokio::test]
async fn test_tables_superset() {
    insta::assert_snapshot!(
        QueryFixture::new(
        "{t1_val: Purchases.total, t2_val: SSPurchases.total, sum: Purchases.total + \
         SSPurchases.total}")
         .run_to_csv(&purchase_tables_superset().await).await.unwrap(),
         @r###"
    _time,_subsort,_key_hash,_key,t1_val,t2_val,sum
    2022-01-03T00:37:57.000000000,0,2867199309159137213,B,,1,
    2022-01-03T00:38:57.000000000,0,12960666915911099378,A,,1,
    2022-01-03T00:39:57.000000000,0,12960666915911099378,A,1,1,2
    2022-01-03T00:39:58.000000000,0,12960666915911099378,A,,666,
    2022-01-03T01:39:57.000000000,0,2867199309159137213,B,10,10,20
    2022-01-03T01:40:57.000000000,0,2867199309159137213,B,,1000,
    2022-01-03T02:38:57.000000000,0,12960666915911099378,A,,777,
    2022-01-03T02:39:57.000000000,0,12960666915911099378,A,2,2,4
    2022-01-03T02:44:57.000000000,0,12960666915911099378,A,,888,
    2022-01-03T03:39:57.000000000,0,2867199309159137213,B,20,20,40
    2022-01-03T04:39:57.000000000,0,2521269998124177631,C,100,100,200
    2022-01-03T05:39:57.000000000,0,2521269998124177631,C,200,200,400
    2022-01-03T06:39:57.000000000,0,12960666915911099378,A,3,3,6
    2022-01-03T06:43:57.000000000,0,2867199309159137213,B,,3,
    2022-01-03T06:45:57.000000000,0,2521269998124177631,C,,3,
    "###);
}

#[tokio::test]
async fn test_triple_add_same_table() {
    let test_data = purchase_table()
        .await
        .with_table_from_csv(make_table_config("P2"), PURCHASE_CSV)
        .await
        .unwrap();
    insta::assert_snapshot!(
        QueryFixture::new(
        "{t1_val: Purchases.total, t2_val: P2.total, sum: Purchases.total + \
         (P2.total + Purchases.total)}")
         .run_to_csv(&test_data).await.unwrap(),
         @r###"
    _time,_subsort,_key_hash,_key,t1_val,t2_val,sum
    2022-01-03T00:39:57.000000000,0,12960666915911099378,A,1,1,3
    2022-01-03T01:39:57.000000000,0,2867199309159137213,B,10,10,30
    2022-01-03T02:39:57.000000000,0,12960666915911099378,A,2,2,6
    2022-01-03T03:39:57.000000000,0,2867199309159137213,B,20,20,60
    2022-01-03T04:39:57.000000000,0,2521269998124177631,C,100,100,300
    2022-01-03T05:39:57.000000000,0,2521269998124177631,C,200,200,600
    2022-01-03T06:39:57.000000000,0,12960666915911099378,A,3,3,9
    "###);
}

#[tokio::test]
async fn test_triple_add_different_tables() {
    let test_data = purchase_table()
        .await
        .with_table_from_csv(make_table_config("SSPurchases"), SUPERSET)
        .await
        .unwrap()
        .with_table_from_csv(make_table_config("OKPurchases"), OVERLAPPING_KEYS)
        .await
        .unwrap();

    insta::assert_snapshot!(
        QueryFixture::new(
        "{t1_val: Purchases.total, t2_val: SSPurchases.total, t3_val: OKPurchases.total, 
            sum: (Purchases.total + SSPurchases.total) + OKPurchases.total}")
         .run_to_csv(&test_data).await.unwrap(),
         @r###"
    _time,_subsort,_key_hash,_key,t1_val,t2_val,t3_val,sum
    2022-01-03T00:37:57.000000000,0,2867199309159137213,B,,1,,
    2022-01-03T00:38:57.000000000,0,12960666915911099378,A,,1,,
    2022-01-03T00:39:57.000000000,0,5844668342709334339,X,,,1,
    2022-01-03T00:39:57.000000000,0,12960666915911099378,A,1,1,,
    2022-01-03T00:39:58.000000000,0,12960666915911099378,A,,666,,
    2022-01-03T01:39:57.000000000,0,2867199309159137213,B,10,10,10,30
    2022-01-03T01:40:57.000000000,0,2867199309159137213,B,,1000,,
    2022-01-03T02:38:57.000000000,0,12960666915911099378,A,,777,,
    2022-01-03T02:39:57.000000000,0,12960666915911099378,A,2,2,2,6
    2022-01-03T02:44:57.000000000,0,12960666915911099378,A,,888,,
    2022-01-03T03:39:57.000000000,0,2867199309159137213,B,20,20,,
    2022-01-03T03:39:57.000000000,0,8493950773958210388,Y,,,20,
    2022-01-03T04:39:57.000000000,0,2521269998124177631,C,100,100,100,300
    2022-01-03T05:39:57.000000000,0,2521269998124177631,C,200,200,,
    2022-01-03T05:39:57.000000000,0,5050198837546418057,Z,,,200,
    2022-01-03T06:39:57.000000000,0,5844668342709334339,X,,,3,
    2022-01-03T06:39:57.000000000,0,12960666915911099378,A,3,3,,
    2022-01-03T06:43:57.000000000,0,2867199309159137213,B,,3,,
    2022-01-03T06:45:57.000000000,0,2521269998124177631,C,,3,,
    "###);
}

#[tokio::test]
async fn test_3_tables_with_3_additions_with_1_common_operand() {
    let test_data = purchase_table()
        .await
        .with_table_from_csv(make_table_config("SSPurchases"), SUPERSET)
        .await
        .unwrap()
        .with_table_from_csv(make_table_config("OKPurchases"), OVERLAPPING_KEYS)
        .await
        .unwrap();

    insta::assert_snapshot!(
        QueryFixture::new(
        "{t1_val: Purchases.total, t2_val: SSPurchases.total, t3_val: OKPurchases.total, 
            sum: (Purchases.total + SSPurchases.total) + (SSPurchases.total + OKPurchases.total)}")
         .run_to_csv(&test_data).await.unwrap(),
         @r###"
    _time,_subsort,_key_hash,_key,t1_val,t2_val,t3_val,sum
    2022-01-03T00:37:57.000000000,0,2867199309159137213,B,,1,,
    2022-01-03T00:38:57.000000000,0,12960666915911099378,A,,1,,
    2022-01-03T00:39:57.000000000,0,5844668342709334339,X,,,1,
    2022-01-03T00:39:57.000000000,0,12960666915911099378,A,1,1,,
    2022-01-03T00:39:58.000000000,0,12960666915911099378,A,,666,,
    2022-01-03T01:39:57.000000000,0,2867199309159137213,B,10,10,10,40
    2022-01-03T01:40:57.000000000,0,2867199309159137213,B,,1000,,
    2022-01-03T02:38:57.000000000,0,12960666915911099378,A,,777,,
    2022-01-03T02:39:57.000000000,0,12960666915911099378,A,2,2,2,8
    2022-01-03T02:44:57.000000000,0,12960666915911099378,A,,888,,
    2022-01-03T03:39:57.000000000,0,2867199309159137213,B,20,20,,
    2022-01-03T03:39:57.000000000,0,8493950773958210388,Y,,,20,
    2022-01-03T04:39:57.000000000,0,2521269998124177631,C,100,100,100,400
    2022-01-03T05:39:57.000000000,0,2521269998124177631,C,200,200,,
    2022-01-03T05:39:57.000000000,0,5050198837546418057,Z,,,200,
    2022-01-03T06:39:57.000000000,0,5844668342709334339,X,,,3,
    2022-01-03T06:39:57.000000000,0,12960666915911099378,A,3,3,,
    2022-01-03T06:43:57.000000000,0,2867199309159137213,B,,3,,
    2022-01-03T06:45:57.000000000,0,2521269998124177631,C,,3,,
    "###);
}
