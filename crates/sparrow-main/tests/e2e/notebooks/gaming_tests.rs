use indoc::indoc;
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::{DataFixture, QueryFixture};

const GAMING_EVENTS_CSV: &str = indoc! {"
event_at,entity_id,duration,won
2022-01-01 02:30:00+00:00,Alice,10,true
2022-01-01 02:35:00+00:00,Bob,3,false
2022-01-01 03:46:00+00:00,Bob,8,false
2022-01-01 03:58:00+00:00,Bob,23,true
2022-01-01 04:25:00+00:00,Bob,8,true
2022-01-01 05:05:00+00:00,Alice,53,true
2022-01-01 05:36:00+00:00,Alice,2,false
2022-01-01 07:22:00+00:00,Bob,7,false
2022-01-01 08:35:00+00:00,Alice,5,false
2022-01-01 10:01:00+00:00,Alice,43,true
"};

/// Create a fixture with the sample events csv.
async fn gaming_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new_with_table_source(
                "GamePlay",
                &Uuid::new_v4(),
                "event_at",
                None,
                "entity_id",
                "User",
            ),
            GAMING_EVENTS_CSV,
        )
        .await
        .unwrap()
}

const GAMING_EVENTS: &str = indoc! {"
    let GameDefeat = GamePlay | when(not(GamePlay.won))
    
    let features = {
        loss_duration: sum(GameDefeat.duration) }
    
    let is_prediction_time = not(GamePlay.won) and (count(GameDefeat, window=since(GamePlay.won)) == 2)
    
    let example = features | when(is_prediction_time)| shift_by(seconds(60*10))
    in example
"};

#[tokio::test]
async fn test_gaming_events_to_csv() {
    insta::assert_snapshot!(QueryFixture::new(GAMING_EVENTS).with_dump_dot("gaming").run_to_csv(&gaming_data_fixture().await).await.unwrap(),
    @r###"
    _time,_subsort,_key_hash,_key,loss_duration
    2022-01-01T03:56:00.000000000,0,7772866443370847918,Bob,11
    2022-01-01T08:45:00.000000000,0,17853343368786040891,Alice,7
    "###);
}
