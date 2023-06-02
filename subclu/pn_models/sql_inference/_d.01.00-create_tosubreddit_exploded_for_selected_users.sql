-- D. Get time on subreddit for selected USERs (from user<>subreddit table C)
-- ETA: 2 minutes
--   Use JavaScript temp function because it''s much faster to expand ToS in BQ than in python
--   We need to write the data to a table because the response can be huge (billions of rows)
DECLARE PT_TOS DATE DEFAULT "2023-05-08";

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

-- ==================
-- Only need to create the first time we run it
-- === OR REPLACE
-- CREATE TABLE `reddit-employee-datasets.david_bermejo.pn_ft_user_tos_30_pct_20230509`
-- PARTITION BY pt
-- CLUSTER BY user_id
-- AS (

-- ==================
-- After table is created, we can delete a partition & update it
-- ===
DELETE `reddit-employee-datasets.david_bermejo.pn_ft_user_tos_30_pct_20230509`
WHERE
    pt = PT_TOS
;
-- Insert latest data
INSERT INTO `reddit-employee-datasets.david_bermejo.pn_ft_user_tos_30_pct_20230509`
(

WITH tos_filtered AS (
    SELECT
        u.user_id
        , t.feature_value
    FROM `data-prod-165221.user_feature_platform.time_on_subreddit_pct_time_over_30_day_v1` AS t
        INNER JOIN(
            SELECT
                DISTINCT user_id
            FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230509`
            WHERE pt = PT_TOS

            -- Limit for testing
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


SELECT
    PT_TOS AS pt
    , *
FROM tos_exploded
WHERE
    -- Limit smallest sub b/c at some point it''s a waste to try to aggregate such small embeddings
    -- 0.001 -> 1000 subreddits evenly split
    tos_30_pct > 0.001
);
