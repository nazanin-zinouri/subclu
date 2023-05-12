-- Pull the user IDs + time on subreddit
--   Using temp function because it's much faster to expand ToS in BQ than in python
--   But we need to write the data to a table because the response can be huge (billions of rows)
DECLARE PT_TOS DATE DEFAULT "2023-04-10";

CREATE TEMP FUNCTION
tosParser(tosString STRING)
RETURNS STRUCT<
    subreddit_id STRING,
    tos_30_pct FLOAT64
>
LANGUAGE js AS """
   arr = tosString.split(':');
   this.subreddit_id = arr[0].slice(1, -1);
   this.tos_30_pct = parseFloat(arr[1]);
   return this;
"""
;


CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.pn_test_user_tos_30_pct_20230413`
CLUSTER BY user_id
AS (
WITH tos_filtered AS (
SELECT
    u.user_id
    , t.feature_value
FROM `data-prod-165221.user_feature_platform.time_on_subreddit_pct_time_over_30_day_v1` AS t
    INNER JOIN(
        SELECT
            DISTINCT user_id
        FROM `reddit-employee-datasets.david_bermejo.pn_test_users_for_embedding_20230412`
        -- LIMIT 10
    ) AS u
        ON u.user_id = t.entity_id
WHERE DATE(t.pt) = PT_TOS
)
, tos_exploded AS (
SELECT
    user_id
    , tosParser(feature_array_exploded).*
FROM(
    SELECT user_id, feature_array_exploded
    FROM (
        SELECT user_id, SPLIT(RTRIM(LTRIM(feature_value, '{'), '}')) AS feature_array FROM tos_filtered
    ), UNNEST(feature_array) AS feature_array_exploded
)
)


SELECT *
FROM tos_exploded

WHERE
    -- Limit smallest sub b/c at some point it's a waste to try to aggregate such small embeddings
    tos_30_pct >= 0.0001
);


-- Count checks
-- SELECT
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT user_id) AS user_id_count
-- FROM `reddit-employee-datasets.david_bermejo.pn_test_user_tos_30_pct_20230413`
-- ;


-- Export data to GCS because querying such a huge table takes forever and a half
EXPORT DATA OPTIONS(
    uri='gs://i18n-subreddit-clustering/pn_model/runs/20230413/user_tos_30_pct/*.parquet',
    format='PARQUET',
    overwrite=true
) AS
SELECT *
FROM `reddit-employee-datasets.david_bermejo.pn_test_user_tos_30_pct_20230413`
;
