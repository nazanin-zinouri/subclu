-- E. Data for model for INFERENCE
--   ETA: 2 mins. For top 1k subreddits
--   ETA: 4 mins. For top 26k subreddits. Slot time: 3 days
-- Combine data into flat format so it's easy to replicate & to export to GCS

-- For inference: select a SINGLE date
DECLARE PT_FEATURES DATE DEFAULT '2023-05-29';

-- For training: select an ARRAY of DATES
-- DECLARE PT_FEATURES DEFAULT [
--     DATE('2023-05-29')
-- ];
-- -- Need explicit max & min for legacy user cohorts
-- DECLARE PT_FEATURES_MAX DEFAULT TIMESTAMP((SELECT MAX(dt) FROM UNNEST(PT_FEATURES) AS dt));
-- DECLARE PT_FEATURES_MIN DEFAULT TIMESTAMP((SELECT MIN(dt) FROM UNNEST(PT_FEATURES) AS dt));


-- ==================
-- Only need to create the first time we run it
-- ===
-- CREATE TABLE `reddit-employee-datasets.david_bermejo.pn_ft_all_20230530`
-- CLUSTER BY pt, target_subreddit_id
-- AS (

-- ==================
-- After table is created, we can delete a partition & update it
-- ===
DELETE
    `reddit-employee-datasets.david_bermejo.pn_ft_all_20230530`
WHERE
    pt = PT_FEATURES
;
-- Insert latest data
INSERT INTO `reddit-employee-datasets.david_bermejo.pn_ft_all_20230530`
(


WITH
subreddit_ft AS (
    -- Query A
    SELECT
        s.* EXCEPT(
            subreddit_name, relevant_geo_country_codes, relevant_geo_country_code_count
            -- TODO(djb): try encoding rating & topics later or let model ecode them
            , over_18, curator_rating, curator_topic_v2
        )
    FROM `reddit-employee-datasets.david_bermejo.pn_ft_subreddits_20230525` AS s
    WHERE s.pt = PT_FEATURES  -- IN UNNEST(PT_FEATURES)

    -- For testing, we can limit to only the top subreddits
    -- ORDER BY users_l7 DESC
    -- LIMIT 1000
)
, user_subreddit_ft AS (
    -- Query C
    SELECT
        us.* EXCEPT(pt_window_start)

    FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230529` AS us

    WHERE us.pt = PT_FEATURES  -- IN UNNEST(PT_FEATURES)
        -- For Inference Only keep users from target geos. Assume that geo has been filtered upstream
        -- AND COALESCE(us.user_geo_country_code, "") IN UNNEST(TARGET_COUNTRY_CODES)
)
, user_ft AS (
    -- Query B
    SELECT
        u.*
    FROM (
        SELECT *
        FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_20230529`
        WHERE pt = PT_FEATURES  -- IN UNNEST(PT_FEATURES)
    ) AS u
)
, final_table AS (
    SELECT
        -- These index columns are needed for the final outputs (not inference)
        us.pt
        , us.target_subreddit_id
        , us.target_subreddit
        , us.user_id
        , COALESCE(u.user_geo_country_code, us.user_geo_country_code) AS user_geo_country_code
        -- Fix subscribed in case missing:
        , COALESCE(us.subscribed, 0) AS subscribed

        -- The rest of the columns should be used for modeling inference
        , u.* EXCEPT(pt, user_id, user_geo_country_code)
        , us.* EXCEPT(pt, user_id, target_subreddit, target_subreddit_id, user_geo_country_code, subscribed)

        , s.* EXCEPT(subreddit_id, pt)

    -- TODO(djb): training: Start with User-level features, b/c we might need to fill nulls for MISSING target_subreddits
    --   For inference, we might want to start with user<>subreddit features and only do inner join with subreddit features
    FROM user_subreddit_ft AS us
        LEFT JOIN user_ft AS u
            ON us.user_id = u.user_id  AND us.pt = u.pt
        -- Subreddit features
        LEFT JOIN subreddit_ft AS s
            ON us.target_subreddit_id = s.subreddit_id AND us.pt = s.pt
)

SELECT *
FROM final_table
);  -- Close CREATE/INSERT parens


-- ============
-- Test CTEs
-- ===
-- SELECT
--     (SELECT COUNT(*) FROM subreddit_ft) AS subreddit_ft_rows

--     , (SELECT COUNT(*) FROM user_subreddit_ft) AS user_subreddit_ft_rows
--     , (SELECT COUNT(DISTINCT user_id) FROM user_subreddit_ft) AS user_subreddit_ft_user_count
--     , (SELECT COUNT(DISTINCT target_subreddit_id) FROM user_subreddit_ft) AS user_subreddit_ft_subreddit_count

--     , (SELECT COUNT(*) FROM user_ft) AS user_ft_rows
--     , (SELECT COUNT(DISTINCT user_id) FROM user_ft) AS user_ft_user_count

--     , (SELECT COUNT(*) FROM final_table) AS final_table_rows
--     , (SELECT COUNT(DISTINCT user_id) FROM final_table) AS final_table_user_count
-- ;


-- SELECT *
-- FROM subreddit_per_user_count;

-- SELECT *
-- FROM user_subreddit_ft;



-- ============
-- Test clicks & receives on full table
-- ===
-- With this sample, we see that ROW is about average for clicks, but null/`MISSING` is the worst performing geo group
--   For now, don't run inference on ROW or NULL because we won't send PNS to these users
-- SELECT
--     user_geo_country_code_top
--     -- , subscribed

--     , APPROX_QUANTILES(user_receives_pn_t7, 100)[OFFSET(50)] AS user_receives_pn_t7_median
--     , APPROX_QUANTILES(user_clicks_pn_t7, 100)[OFFSET(80)] AS user_clicks_pn_t7_p80
--     , APPROX_QUANTILES(user_clicks_pn_t7, 100)[OFFSET(90)] AS user_clicks_pn_t7_p90
--     , APPROX_QUANTILES(user_clicks_pn_t7, 100)[OFFSET(95)] AS user_clicks_pn_t7_p95
--     -- , AVG(user_receives_pn_t7) AS user_receives_pn_t7_avg
--     , COUNT(DISTINCT user_id) AS user_count
--     , COUNT(*) AS row_count
-- FROM `reddit-employee-datasets.david_bermejo.pn_ft_all_20230509`
-- WHERE pt = "2023-05-07"
-- GROUP BY 1 -- , 2
-- ORDER BY user_clicks_pn_t7_p90 DESC, user_count DESC
-- ;
