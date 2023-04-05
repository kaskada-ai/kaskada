//! tests from code snippets found in our public documentation

use indoc::indoc;
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::fixture::{DataFixture, QueryFixture};

// From https://docs.kaskada.com/docs/exporting-for-training
// in section "Feature Definitions"
const QUERY: &str = indoc! {"
let average_purchase_by_customer = PurchaseByCustomer.amount
  | mean()
let predictors = {
    entity: Purchase.id,
    purchase_total: Purchase.amount | last(),
    mean_purchase: lookup(Purchase.customer_id, average_purchase_by_customer),
}

let target = {
  target: count(FraudReport),
}

let shifted =  predictors
 | shift_to(time_of($input) | add_time(days(30)))
in shifted | extend(lookup($input.entity, target))
"};

async fn data_fixture_purchases_and_fraud() -> DataFixture {
    DataFixture::new()
        .with_table_from_files(
            TableConfig::new(
                "Purchase",
                &Uuid::new_v4(),
                "purchase_time",
                Some("subsort_id"),
                "id",
                "",
            ),
            &["purchases/purchases_part1.parquet"],
        )
        .await
        .unwrap()
        .with_table_from_files(
            TableConfig::new(
                "PurchaseByCustomer",
                &Uuid::new_v4(),
                "purchase_time",
                Some("subsort_id"),
                "customer_id",
                "",
            ),
            &["purchases/purchases_part1.parquet"],
        )
        .await
        .unwrap()
        .with_table_from_files(
            TableConfig::new(
                "FraudReport",
                &Uuid::new_v4(),
                "time",
                Some("subsort_id"),
                "purchase_id",
                "",
            ),
            &["purchases/frauds.parquet"],
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_feature_query() {
    insta::assert_snapshot!(
        QueryFixture::new(QUERY)
        .run_to_csv(&data_fixture_purchases_and_fraud().await)
        .await.unwrap(),
    @r###"
    _time,_subsort,_key_hash,_key,target,entity,purchase_total,mean_purchase
    2020-01-31T00:00:00.000000000,0,12403606648443509540,cb_001,,cb_001,9,9.0
    2020-01-31T00:00:00.000000000,1,3378313965393548399,kk_001,,kk_001,3,3.0
    2020-02-01T00:00:00.000000000,2,10125507257854989870,cb_002,,cb_002,2,5.5
    2020-02-01T00:00:00.000000000,3,12726274317228806426,kk_002,,kk_002,5,4.0
    2020-02-02T00:00:00.000000000,4,3211606271575657135,cb_003,,cb_003,4,5.0
    2020-02-02T00:00:00.000000000,5,8266159855839342533,kk_003,,kk_003,12,6.666666666666666
    2020-02-03T00:00:00.000000000,6,10823917382312772685,cb_004,1,cb_004,5000,1255.0
    2020-02-03T00:00:00.000000000,7,15196731045994046513,cb_005,,cb_005,3,4.5
    2020-02-04T00:00:00.000000000,8,14310739973228482455,cb_006,,cb_006,5,4.6
    2020-02-04T00:00:00.000000000,9,13038643822299281194,kk_004,,kk_004,9,1005.8
    "###);
}
