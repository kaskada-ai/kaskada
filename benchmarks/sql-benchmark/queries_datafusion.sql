-- Run with `datafusion-cli --data-path <directory>`.
-- Then send these commands.

-- Load the data.
-- These tables weren't usable, so just used inline definitions.
--
-- CREATE EXTERNAL TABLE Purchases
-- STORED AS parquet
-- LOCATION 'purchases.parquet';

-- CREATE EXTERNAL TABLE Reviews
-- STORED AS parquet
-- LOCATION 'reviews.parquet';

-- CREATE EXTERNAL TABLE PageViews
-- STORED AS parquet
-- LOCATION 'page_views.parquet';

-- Aggregation / History
COPY (SELECT
  user,
  time,
  SUM(amount) OVER (
    PARTITION BY user
    ORDER BY time
  )
FROM 'purchases.parquet') TO 'output/agg_history_df.parquet';

-- Aggregation / Snapshot
COPY (SELECT
  user,
  SUM(amount)
FROM Purchases
GROUP BY user) TO 'output/agg_snapshot_duckdb.parquet';

-- Time-Windowed Aggregation / History
COPY (SELECT
  user,
  time,
  sum(amount) OVER (
    PARTITION BY
      user,
      time_bucket(INTERVAL '1 month', time)
    ORDER BY time
  )
FROM Purchases
ORDER BY time) TO 'output/windowed_history_duckdb.parquet';

-- Time-Windowed Aggregation / Snapshot
COPY (SELECT
  user,
  sum(amount)
FROM Purchases
WHERE time_bucket(INTERVAL '1 month', time) >= time_bucket(INTERVAL '1 month', DATE '2022-05-03')
GROUP BY user) TO 'output/windowed_snapshot_duckdb.parquet';

-- Data-Defined Windowed Aggregation / History
COPY (WITH activity AS (
  (SELECT user, time, 1 as is_page_view FROM PageViews)
  UNION
  (SELECT user, time, 0 as is_page_view FROM Purchases)
), purchase_counts AS (
  SELECT
    user, time, is_page_view,
    SUM(CASE WHEN is_page_view = 0 THEN 1 ELSE 0 END)
      OVER (PARTITION BY user ORDER BY time) AS purchase_count
    FROM activity
), page_views_since_purchase AS (
  SELECT
    user, time,
    SUM(CASE WHEN is_page_view = 1 THEN 1 ELSE 0 END)
      OVER (PARTITION BY user, purchase_count ORDER BY time) AS views
    FROM purchase_counts
)
SELECT user, time,
  AVG(views) OVER (PARTITION BY user ORDER BY time)
    as avg_views_since_purchase
FROM page_views_since_purchase
ORDER BY time) TO 'output/data_defined_history_duckdb.parquet';

-- Temporal Join / Snapshot [Spline]
--
-- Not reported -- ASOF join is more efficient.
COPY (WITH review_avg AS (
  SELECT item, time,
    AVG(rating) OVER (PARTITION BY item ORDER BY time) as avg_score
  FROM Reviews
), review_times AS (
  SELECT item, review_avg.time AS time, review_avg.time AS r_time,
    CAST(NULL AS TIMESTAMP) as p_time
  FROM review_avg
), purchase_times AS (
  SELECT item, Purchases.time as time, Purchases.time as p_time,
    CAST(NULL AS TIMESTAMP) AS r_time,
  FROM Purchases
), all_times AS (
  (SELECT * FROM review_times) UNION (SELECT * FROM purchase_times)
), spline AS (
  SELECT item, time, max(r_time) OVER w AS last_r_time,
  FROM all_times
  WINDOW w AS (PARTITION BY item ORDER BY time)
)
SELECT user, Purchases.time, avg_score
FROM Purchases
LEFT JOIN spline
  ON Purchases.time = spline.time AND Purchases.item = spline.item
LEFT JOIN review_avg
  ON spline.last_r_time = review_avg.time
 AND Purchases.item = review_avg.item) TO 'output/temporal_join_spline_snapshot_duckdb.parquet';

-- Temporal Join / Snapshot [ASOF Join]
COPY (WITH review_avg AS (
  SELECT item, time,
    AVG(rating) OVER (PARTITION BY item ORDER BY time) as avg_score
  FROM Reviews
)
SELECT p.user, p.time, r.avg_score
FROM review_avg r ASOF RIGHT JOIN Purchases p
ON p.item = r.item AND r.time >= p.time) TO 'output/temporal_join_asof_snapshot_duckdb.parquet';