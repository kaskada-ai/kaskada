//! Basic e2e tests for the aggregation operators.
// any: i64, f64, string, record, boolean, timestamp
// ordered: i64, f64, timestamp
// number: i64, f64

use indoc::indoc;
use sparrow_api::kaskada::v1alpha::TableConfig;
use uuid::Uuid;

use crate::{DataFixture, QueryFixture};

async fn window_data_fixture() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new_with_table_source(
                "Foo",
                &Uuid::new_v4(),
                "time",
                Some("subsort"),
                "key",
                "",
            ),
            indoc! {"
    time,subsort,key,n,vegetable,bool
    1996-12-19T16:39:57-08:00,0,A,10,arugula,true
    1996-12-19T16:39:58-08:00,0,B,3.9,beet,true
    1996-12-19T16:39:59-08:00,0,A,6.2,carrot,false
    1996-12-19T16:40:00-08:00,0,A,9.25,dill,false
    1996-12-19T16:40:01-08:00,0,A,3,edamame,true
    1996-12-19T16:40:02-08:00,0,A,8,fennel,false
    1996-12-19T16:40:03-08:00,0,A,,green beans,true
    1996-12-19T16:40:04-08:00,0,A,10,habanero,false
    "},
        )
        .await
        .unwrap()
}

async fn window_data_fixture_with_nulls() -> DataFixture {
    DataFixture::new()
        .with_table_from_csv(
            TableConfig::new_with_table_source(
                "Foo",
                &Uuid::new_v4(),
                "time",
                Some("subsort"),
                "key",
                "",
            ),
            indoc! {"
    time,subsort,key,n,vegetable,bool
    1996-12-19T16:39:57-08:00,0,A,10,arugula,true
    1996-12-19T16:39:58-08:00,0,B,3.9,beet,true
    1996-12-19T16:39:59-08:00,0,A,,carrot,false
    1996-12-19T16:40:00-08:00,0,A,9.25,dill,
    1996-12-19T16:40:01-08:00,0,A,,edamame,
    1996-12-19T16:40:02-08:00,0,A,,fennel,false
    1996-12-19T16:40:03-08:00,0,A,1,green beans,true
    1996-12-19T16:40:04-08:00,0,A,10,habanero,true
    "},
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_sliding_window_with_predicate() {
    insta::assert_snapshot!(QueryFixture::new("{ since: count(Foo, window=since(daily())), slide: Foo | count(window=sliding(2, $input | is_valid())) }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,since,slide
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,1,1
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,1,1
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,2,2
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,3,2
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,4,2
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,5,2
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,6,2
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,7,2
    "###);
}

#[tokio::test]
async fn test_sliding_window_with_predicate_final_results() {
    insta::assert_snapshot!(QueryFixture::new("{ since: count(Foo, window=since(daily())), slide: Foo | count(window=sliding(2, $input | is_valid())) }").with_final_results().run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,since,slide
    1996-12-20T00:40:04.000000001,18446744073709551615,2867199309159137213,B,1,1
    1996-12-20T00:40:04.000000001,18446744073709551615,12960666915911099378,A,7,2
    "###);
}

#[tokio::test]
async fn test_count_since_window() {
    insta::assert_snapshot!(QueryFixture::new("{ n: Foo.n, cond: Foo.n < 7.0, count: count(Foo.n), count_since: count(Foo.n, window=since(Foo.n < 7.0))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,cond,count,count_since
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,10.0,false,1,1
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,3.9,true,1,1
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,6.2,true,2,2
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,9.25,false,3,1
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,3.0,true,4,2
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,8.0,false,5,1
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,,,5,1
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,10.0,false,6,2
    "###);
}

#[tokio::test]
async fn test_sum_since_window() {
    insta::assert_snapshot!(QueryFixture::new("{ n: Foo.n, cond: Foo.n < 7.0, sum: sum(Foo.n), sum_since: sum(Foo.n, window=since(Foo.n < 7.0))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,cond,sum,sum_since
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,10.0,false,10.0,10.0
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,3.9,true,3.9,3.9
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,6.2,true,16.2,16.2
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,9.25,false,25.45,9.25
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,3.0,true,28.45,12.25
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,8.0,false,36.45,8.0
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,,,36.45,8.0
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,10.0,false,46.45,18.0
    "###);
}

#[tokio::test]
async fn test_min_since_window() {
    insta::assert_snapshot!(QueryFixture::new("{ n: Foo.n, cond: Foo.n < 7.0, min: min(Foo.n), min_since: min(Foo.n, window=since(Foo.n < 7.0))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,cond,min,min_since
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,10.0,false,10.0,10.0
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,3.9,true,3.9,3.9
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,6.2,true,6.2,6.2
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,9.25,false,6.2,9.25
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,3.0,true,3.0,3.0
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,8.0,false,3.0,8.0
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,,,3.0,8.0
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,10.0,false,3.0,8.0
    "###);
}

#[tokio::test]
async fn test_max_since_window() {
    insta::assert_snapshot!(QueryFixture::new("{ n: Foo.n, cond: Foo.n < 7.0, max: max(Foo.n), max_since: max(Foo.n, window=since(Foo.n < 7.0))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,cond,max,max_since
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,10.0,false,10.0,10.0
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,3.9,true,3.9,3.9
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,6.2,true,10.0,10.0
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,9.25,false,10.0,9.25
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,3.0,true,10.0,9.25
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,8.0,false,10.0,8.0
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,,,10.0,8.0
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,10.0,false,10.0,10.0
    "###);
}

#[tokio::test]
async fn test_mean_since_window() {
    insta::assert_snapshot!(QueryFixture::new("{ n: Foo.n, cond: Foo.n < 7.0, mean: mean(Foo.n), mean_since: mean(Foo.n, window=since(Foo.n < 7.0))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,cond,mean,mean_since
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,10.0,false,10.0,10.0
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,3.9,true,3.9,3.9
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,6.2,true,8.1,8.1
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,9.25,false,8.483333333333333,9.25
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,3.0,true,7.112499999999999,6.125
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,8.0,false,7.289999999999999,8.0
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,,,7.289999999999999,8.0
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,10.0,false,7.741666666666666,9.0
    "###);
}

#[tokio::test]
async fn test_variance_since_window() {
    insta::assert_snapshot!(QueryFixture::new("{ n: Foo.n, cond: Foo.n < 7.0, variance: variance(Foo.n), variance_since: variance(Foo.n, window=since(Foo.n < 7.0))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,cond,variance,variance_since
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,10.0,false,,
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,3.9,true,,
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,6.2,true,3.609999999999999,3.609999999999999
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,9.25,false,2.7005555555555554,
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,3.0,true,7.662968749999997,9.765625
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,8.0,false,6.256399999999998,
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,,,6.256399999999998,
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,10.0,false,6.233680555555555,1.0
    "###);
}

#[tokio::test]
async fn test_last_since_window() {
    insta::assert_snapshot!(QueryFixture::new("{ n: Foo.n, cond: Foo.n < 7.0, last: last(Foo.n), last_since: last(Foo.n, window=since(Foo.n < 7.0))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,cond,last,last_since
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,10.0,false,10.0,10.0
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,3.9,true,3.9,3.9
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,6.2,true,6.2,6.2
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,9.25,false,9.25,9.25
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,3.0,true,3.0,3.0
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,8.0,false,8.0,8.0
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,,,8.0,8.0
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,10.0,false,10.0,10.0
    "###);
}

#[tokio::test]
async fn test_f64_first_since_window() {
    insta::assert_snapshot!(QueryFixture::new("{ n: Foo.n, cond: Foo.n < 7.0, first: first(Foo.n), first_since: first(Foo.n, window=since(Foo.n < 7.0))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,cond,first,first_since
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,10.0,false,10.0,10.0
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,3.9,true,3.9,3.9
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,6.2,true,10.0,10.0
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,9.25,false,10.0,9.25
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,3.0,true,10.0,9.25
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,8.0,false,10.0,8.0
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,,,10.0,8.0
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,10.0,false,10.0,8.0
    "###);
}

#[tokio::test]
async fn test_string_first_since_window() {
    insta::assert_snapshot!(QueryFixture::new("{ vegetable: Foo.vegetable, cond: Foo.n < 7.0, first: first(Foo.vegetable), first_since: first(Foo.vegetable, window=since(Foo.n < 7.0))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,vegetable,cond,first,first_since
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,arugula,false,arugula,arugula
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,beet,true,beet,beet
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,carrot,true,arugula,arugula
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,dill,false,arugula,dill
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,edamame,true,arugula,dill
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,fennel,false,arugula,fennel
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,green beans,,arugula,fennel
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,habanero,false,arugula,fennel
    "###);
}

#[tokio::test]
async fn test_string_last_since_window() {
    insta::assert_snapshot!(QueryFixture::new("{ vegetable: Foo.vegetable, cond: Foo.n < 7.0, last: last(Foo.vegetable), last_since: last(Foo.vegetable, window=since(Foo.n < 7.0))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,vegetable,cond,last,last_since
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,arugula,false,arugula,arugula
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,beet,true,beet,beet
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,carrot,true,carrot,carrot
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,dill,false,dill,dill
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,edamame,true,edamame,edamame
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,fennel,false,fennel,fennel
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,green beans,,green beans,green beans
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,habanero,false,habanero,habanero
    "###);
}

#[tokio::test]
async fn test_bool_first_since_window() {
    insta::assert_snapshot!(QueryFixture::new("{ bool: Foo.bool, cond: Foo.n < 7.0, first: first(Foo.bool), first_since: first(Foo.bool, window=since(Foo.n < 7.0))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,bool,cond,first,first_since
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,false,true,true
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,true,true,true,true
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,false,true,true,true
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,false,false,true,false
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,true,true,true,false
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,false,false,true,false
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,true,,true,false
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,false,false,true,false
    "###);
}

#[tokio::test]
async fn test_bool_last_since_window() {
    insta::assert_snapshot!(QueryFixture::new("{ bool: Foo.bool, cond: Foo.n < 7.0, last: last(Foo.bool), last_since: last(Foo.bool, window=since(Foo.n < 7.0))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,bool,cond,last,last_since
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,false,true,true
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,true,true,true,true
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,false,true,false,false
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,false,false,false,false
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,true,true,true,true
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,false,false,false,false
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,true,,true,true
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,false,false,false,false
    "###);
}

/// Verifies that when a since window is reset, the value is emitted, regardless
/// of input validity.
#[tokio::test]
async fn test_first_since_window_emits_value_on_reset() {
    insta::assert_snapshot!(QueryFixture::new("{ n: Foo.n, cond: Foo.bool, first_since: first(Foo.n, window=since(Foo.bool))  }").run_to_csv(&window_data_fixture_with_nulls().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,cond,first_since
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,10.0,true,10.0
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,3.9,true,3.9
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,,false,
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,9.25,,9.25
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,,,9.25
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,false,9.25
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,1.0,true,9.25
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,10.0,true,10.0
    "###);
}

/// Verifies that when a sliding window is evicted, the oldest value is emitted,
/// regardless of input validity.
#[ignore = "https://github.com/kaskada-ai/kaskada/issues/754"]
#[tokio::test]
async fn test_first_sliding_window_emits_value_on_reset() {
    insta::assert_snapshot!(QueryFixture::new("{ n: Foo.n, first_sliding: first(Foo.n, window=sliding(2, is_valid(Foo)))  }").run_to_csv(&window_data_fixture_with_nulls().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,first_sliding
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,10.0,10.0
    1996-12-20T00:39:58.000000000,9223372036854775808,2867199309159137213,B,3.9,3.9
    1996-12-20T00:39:59.000000000,9223372036854775808,12960666915911099378,A,,10.0
    1996-12-20T00:40:00.000000000,9223372036854775808,12960666915911099378,A,9.25,9.25
    1996-12-20T00:40:01.000000000,9223372036854775808,12960666915911099378,A,,9.25
    1996-12-20T00:40:02.000000000,9223372036854775808,12960666915911099378,A,,
    1996-12-20T00:40:03.000000000,9223372036854775808,12960666915911099378,A,1.0,1.0
    1996-12-20T00:40:04.000000000,9223372036854775808,12960666915911099378,A,10.0,1.0
    "###);
}

/// Verifies that when a since window is reset, the value is emitted, regardless
/// of input validity.
#[tokio::test]
async fn test_last_since_window_emits_value_on_reset() {
    insta::assert_snapshot!(QueryFixture::new("{ n: Foo.n, cond: Foo.bool, last_since: last(Foo.n, window=since(Foo.bool))  }").run_to_csv(&window_data_fixture_with_nulls().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,cond,last_since
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,10.0,true,10.0
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,3.9,true,3.9
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,,false,
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,9.25,,9.25
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,,,9.25
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,,false,9.25
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,1.0,true,1.0
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,10.0,true,10.0
    "###);
}

/// Verifies that when a sliding window is evicted, the oldest value is emitted,
/// regardless of input validity.
#[ignore = "https://github.com/kaskada-ai/kaskada/issues/754"]
#[tokio::test]
async fn test_last_sliding_window_emits_value_on_reset() {
    insta::assert_snapshot!(QueryFixture::new("{ n: Foo.n, last_sliding: last(Foo.n, window=sliding(2, is_valid(Foo)))  }").run_to_csv(&window_data_fixture_with_nulls().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,n,last_sliding
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,10.0,10.0
    1996-12-20T00:39:58.000000000,9223372036854775808,2867199309159137213,B,3.9,3.9
    1996-12-20T00:39:59.000000000,9223372036854775808,12960666915911099378,A,,10.0
    1996-12-20T00:40:00.000000000,9223372036854775808,12960666915911099378,A,9.25,9.25
    1996-12-20T00:40:01.000000000,9223372036854775808,12960666915911099378,A,,9.25
    1996-12-20T00:40:02.000000000,9223372036854775808,12960666915911099378,A,,
    1996-12-20T00:40:03.000000000,9223372036854775808,12960666915911099378,A,1.0,1.0
    1996-12-20T00:40:04.000000000,9223372036854775808,12960666915911099378,A,10.0,10.0
    "###);
}

#[tokio::test]
async fn test_count_sliding_window_every_few_events() {
    insta::assert_snapshot!(QueryFixture::new("{ cond: is_valid(Foo), total_count: count(Foo), sliding_count: count(Foo, window=sliding(3, is_valid(Foo)))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,cond,total_count,sliding_count
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,1,1
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,true,1,1
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,true,2,2
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,true,3,3
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,true,4,3
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,true,5,3
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,true,6,3
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,true,7,3
    "###);
}

#[tokio::test]
async fn test_count_sliding_window_with_condition() {
    insta::assert_snapshot!(QueryFixture::new("{ cond: Foo.n > 5, sliding_count: count(Foo.n, window=sliding(2, Foo.n > 5)) }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,cond,sliding_count
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,true,1
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,false,1
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,true,2
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,true,2
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,false,2
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,true,3
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,,2
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,true,3
    "###);
}

#[tokio::test]
async fn test_count_sliding_duration_1_equivalent_to_since() {
    insta::assert_snapshot!(QueryFixture::new("{ since: count(Foo.bool, window=since(Foo.n > 5)), sliding: count(Foo.bool, window=sliding(1, Foo.n > 5))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,since,sliding
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,1,1
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,1,1
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,1,1
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,1,1
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,1,1
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,2,2
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,1,1
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,2,2
    "###);
}

#[tokio::test]
async fn test_sum_sliding_every_few_events() {
    insta::assert_snapshot!(QueryFixture::new("{ sliding_sum: sum(Foo.n, window=sliding(2, is_valid(Foo)))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,sliding_sum
    1996-12-20T00:39:57.000000000,0,12960666915911099378,A,10.0
    1996-12-20T00:39:58.000000000,0,2867199309159137213,B,3.9
    1996-12-20T00:39:59.000000000,0,12960666915911099378,A,16.2
    1996-12-20T00:40:00.000000000,0,12960666915911099378,A,15.45
    1996-12-20T00:40:01.000000000,0,12960666915911099378,A,12.25
    1996-12-20T00:40:02.000000000,0,12960666915911099378,A,11.0
    1996-12-20T00:40:03.000000000,0,12960666915911099378,A,8.0
    1996-12-20T00:40:04.000000000,0,12960666915911099378,A,10.0
    "###);
}

#[ignore = "https://github.com/kaskada-ai/kaskada/issues/754"]
#[tokio::test]
async fn test_first_f64_sliding_every_few_events() {
    insta::assert_snapshot!(QueryFixture::new("{ sliding_first: first(Foo.n, window=sliding(2, is_valid(Foo)))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,sliding_first
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,10.0
    1996-12-20T00:39:58.000000000,9223372036854775808,2867199309159137213,B,3.9
    1996-12-20T00:39:59.000000000,9223372036854775808,12960666915911099378,A,10.0
    1996-12-20T00:40:00.000000000,9223372036854775808,12960666915911099378,A,6.2
    1996-12-20T00:40:01.000000000,9223372036854775808,12960666915911099378,A,9.25
    1996-12-20T00:40:02.000000000,9223372036854775808,12960666915911099378,A,3.0
    1996-12-20T00:40:03.000000000,9223372036854775808,12960666915911099378,A,8.0
    1996-12-20T00:40:04.000000000,9223372036854775808,12960666915911099378,A,10.0
    "###);
}

#[ignore = "https://github.com/kaskada-ai/kaskada/issues/754"]
#[tokio::test]
async fn test_first_string_sliding_every_few_events() {
    insta::assert_snapshot!(QueryFixture::new("{ sliding_first: first(Foo.vegetable, window=sliding(2, is_valid(Foo)))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,sliding_first
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,arugula
    1996-12-20T00:39:58.000000000,9223372036854775808,2867199309159137213,B,beet
    1996-12-20T00:39:59.000000000,9223372036854775808,12960666915911099378,A,arugula
    1996-12-20T00:40:00.000000000,9223372036854775808,12960666915911099378,A,carrot
    1996-12-20T00:40:01.000000000,9223372036854775808,12960666915911099378,A,dill
    1996-12-20T00:40:02.000000000,9223372036854775808,12960666915911099378,A,edamame
    1996-12-20T00:40:03.000000000,9223372036854775808,12960666915911099378,A,fennel
    1996-12-20T00:40:04.000000000,9223372036854775808,12960666915911099378,A,green beans
    "###);
}

#[ignore = "https://github.com/kaskada-ai/kaskada/issues/754"]
#[tokio::test]
async fn test_first_boolean_sliding_every_few_events() {
    insta::assert_snapshot!(QueryFixture::new("{ sliding_first: first(Foo.bool, window=sliding(2, is_valid(Foo)))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,sliding_first
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,true
    1996-12-20T00:39:58.000000000,9223372036854775808,2867199309159137213,B,true
    1996-12-20T00:39:59.000000000,9223372036854775808,12960666915911099378,A,true
    1996-12-20T00:40:00.000000000,9223372036854775808,12960666915911099378,A,false
    1996-12-20T00:40:01.000000000,9223372036854775808,12960666915911099378,A,false
    1996-12-20T00:40:02.000000000,9223372036854775808,12960666915911099378,A,true
    1996-12-20T00:40:03.000000000,9223372036854775808,12960666915911099378,A,false
    1996-12-20T00:40:04.000000000,9223372036854775808,12960666915911099378,A,true
    "###);
}

#[ignore = "https://github.com/kaskada-ai/kaskada/issues/754"]
#[tokio::test]
async fn test_last_f64_sliding_every_few_events() {
    insta::assert_snapshot!(QueryFixture::new("{ sliding_last: last(Foo.n, window=sliding(2, is_valid(Foo)))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,sliding_last
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,10.0
    1996-12-20T00:39:58.000000000,9223372036854775808,2867199309159137213,B,3.9
    1996-12-20T00:39:59.000000000,9223372036854775808,12960666915911099378,A,6.2
    1996-12-20T00:40:00.000000000,9223372036854775808,12960666915911099378,A,9.25
    1996-12-20T00:40:01.000000000,9223372036854775808,12960666915911099378,A,3.0
    1996-12-20T00:40:02.000000000,9223372036854775808,12960666915911099378,A,8.0
    1996-12-20T00:40:03.000000000,9223372036854775808,12960666915911099378,A,8.0
    1996-12-20T00:40:04.000000000,9223372036854775808,12960666915911099378,A,10.0
    "###);
}

#[ignore = "https://github.com/kaskada-ai/kaskada/issues/754"]
#[tokio::test]
async fn test_last_string_sliding_every_few_events() {
    insta::assert_snapshot!(QueryFixture::new("{ sliding_last: last(Foo.vegetable, window=sliding(2, is_valid(Foo)))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,sliding_last
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,arugula
    1996-12-20T00:39:58.000000000,9223372036854775808,2867199309159137213,B,beet
    1996-12-20T00:39:59.000000000,9223372036854775808,12960666915911099378,A,carrot
    1996-12-20T00:40:00.000000000,9223372036854775808,12960666915911099378,A,dill
    1996-12-20T00:40:01.000000000,9223372036854775808,12960666915911099378,A,edamame
    1996-12-20T00:40:02.000000000,9223372036854775808,12960666915911099378,A,fennel
    1996-12-20T00:40:03.000000000,9223372036854775808,12960666915911099378,A,green beans
    1996-12-20T00:40:04.000000000,9223372036854775808,12960666915911099378,A,habanero
    "###);
}

#[ignore = "https://github.com/kaskada-ai/kaskada/issues/754"]
#[tokio::test]
async fn test_last_bool_sliding_every_few_events() {
    insta::assert_snapshot!(QueryFixture::new("{ sliding_last: last(Foo.bool, window=sliding(2, is_valid(Foo)))  }").run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,sliding_last
    1996-12-20T00:39:57.000000000,9223372036854775808,12960666915911099378,A,true
    1996-12-20T00:39:58.000000000,9223372036854775808,2867199309159137213,B,true
    1996-12-20T00:39:59.000000000,9223372036854775808,12960666915911099378,A,false
    1996-12-20T00:40:00.000000000,9223372036854775808,12960666915911099378,A,false
    1996-12-20T00:40:01.000000000,9223372036854775808,12960666915911099378,A,true
    1996-12-20T00:40:02.000000000,9223372036854775808,12960666915911099378,A,false
    1996-12-20T00:40:03.000000000,9223372036854775808,12960666915911099378,A,true
    1996-12-20T00:40:04.000000000,9223372036854775808,12960666915911099378,A,false
    "###);
}

#[tokio::test]
async fn test_aggregation_arguments_wrong() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ count: Foo.n | count(since(Foo.n < 5))  }").run_to_csv(&window_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: warning
        code: W2001
        message: Unused binding
        formatted:
          - "warning[W2001]: Unused binding"
          - "  --> Query:1:16"
          - "  |"
          - "1 | { count: Foo.n | count(since(Foo.n < 5))  }"
          - "  |                ^ Left-hand side of pipe not used"
          - ""
          - ""
      - severity: error
        code: E0010
        message: Invalid argument type(s)
        formatted:
          - "error[E0010]: Invalid argument type(s)"
          - "  --> Query:1:18"
          - "  |"
          - "1 | { count: Foo.n | count(since(Foo.n < 5))  }"
          - "  |                  ^^^^^ ---------------- Type: window"
          - "  |                  |      "
          - "  |                  Invalid types for call to 'count'"
          - "  |"
          - "  = Expected 'any'"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_sliding_arguments_wrong() {
    // This verifies we only produce *one error*, even though the result
    // of sliding is an error (non-constant duration).
    insta::assert_yaml_snapshot!(QueryFixture::new("{ count: Foo.n | count(sliding(Foo.n))  }").run_to_csv(&window_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0014
        message: Invalid non-constant argument
        formatted:
          - "error[E0014]: Invalid non-constant argument"
          - "  --> Query:1:32"
          - "  |"
          - "1 | { count: Foo.n | count(sliding(Foo.n))  }"
          - "  |                                ^^^^^ Argument 'duration' to 'sliding' must be constant, but was not"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_non_constant_sliding_duration_produces_diagnostic() {
    insta::assert_yaml_snapshot!(QueryFixture::new("{ count: count(Foo, window=sliding(Foo.n, Foo.n))  }").run_to_csv(&window_data_fixture().await).await.unwrap_err(), @r###"
    ---
    code: Client specified an invalid argument
    message: 1 errors in Fenl statements; see diagnostics
    fenl_diagnostics:
      - severity: error
        code: E0014
        message: Invalid non-constant argument
        formatted:
          - "error[E0014]: Invalid non-constant argument"
          - "  --> Query:1:36"
          - "  |"
          - "1 | { count: count(Foo, window=sliding(Foo.n, Foo.n))  }"
          - "  |                                    ^^^^^ Argument 'duration' to 'sliding' must be constant, but was not"
          - ""
          - ""
    "###);
}

#[tokio::test]
async fn test_sliding_count_final_results() {
    // Changes to an aggregate due to window evictions should be treated
    // as new in later aggregations (such as the `last` added for final results)
    let data_fixture = DataFixture::new()
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
            time,subsort,key,m
            1996-12-14T18:38:57-08:00,0,B,2
            1996-12-14T18:39:57-08:00,0,B,1
            1996-12-19T22:42:05-08:00,0,A,3
            "},
        )
        .await
        .unwrap();

    // `key` and `m` are absent for B because it's last tick was at the hour,
    // at which point `Numbers` was null.
    //
    // On the other hand, the daily count is 0 because there are three windows
    // active at the time of the last row:
    // (19-20], (20-21], (21-22]
    insta::assert_snapshot!(QueryFixture::new("{ key: Numbers.key, m: Numbers.m, daily_count: count(Numbers, window=sliding(3, hourly()))}").with_final_results().run_to_csv(&data_fixture).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,key,m,daily_count
    1996-12-20T06:42:05.000000001,18446744073709551615,2867199309159137213,B,,,0
    1996-12-20T06:42:05.000000001,18446744073709551615,12960666915911099378,A,A,3,1
    "###);
}

#[tokio::test]
async fn test_final_sliding_window_constant() {
    insta::assert_snapshot!(QueryFixture::new("{ sliding_const: Foo.n | sum(window = sliding(5, true)) }").with_final_results().run_to_csv(&window_data_fixture().await).await.unwrap(), @r###"
    _time,_subsort,_key_hash,_key,sliding_const
    1996-12-20T00:40:04.000000001,18446744073709551615,2867199309159137213,B,3.9
    1996-12-20T00:40:04.000000001,18446744073709551615,12960666915911099378,A,30.25
    "###);
}
