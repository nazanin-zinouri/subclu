-- E. Data for model inference (or training)
--   ETA: ~2 mins. For top 1k subreddits.
--      Output: 838 M rows (user<>subreddit pairs) with RoW & NULL geos
--      Output: 706 M rows (user<>subreddit pairs) withOUT RoW & NULL geos

-- Combine data into flat format so it's easy to replicate & to export to GCS

DECLARE PT_FEATURES DATE DEFAULT "2023-05-07";

DECLARE TARGET_COUNTRY_CODES DEFAULT [
    "MX", "ES", "AR"
    , "DE", "AT", "CH"
    , "US", "GB", "IN", "CA", "AU", "IE"
    , "FR", "NL", "IT"
    , "BR", "PT"
    , "PH"
];


-- ==================
-- Only need to create the first time we run it
-- === OR REPLACE
-- CREATE TABLE `reddit-employee-datasets.david_bermejo.pn_ft_all_20230509`
-- PARTITION BY pt
-- CLUSTER BY target_subreddit_id
-- AS (

-- ==================
-- After table is created, we can delete a partition & update it
-- ===
DELETE
    `reddit-employee-datasets.david_bermejo.pn_ft_all_20230509`
WHERE
    pt = PT_FEATURES
;
-- Insert latest data
INSERT INTO `reddit-employee-datasets.david_bermejo.pn_ft_all_20230509`
(


WITH
subreddit_ft AS (
    SELECT
        s.* EXCEPT(
            pt, subreddit_name, relevant_geo_country_codes, relevant_geo_country_code_count
            -- TODO(djb): try encoding rating & topics later or let lGBM ecode them
            , over_18, curator_rating, curator_topic_v2
        )
    FROM `reddit-employee-datasets.david_bermejo.pn_ft_subreddits_20230509` AS s
    WHERE s.pt = PT_FEATURES
    -- For testing, we can limit to only the top subreddits
    ORDER BY users_l7 DESC
    LIMIT 1000
)
, subreddit_per_user_count AS (
    SELECT
        tos.user_id
        , COUNT(DISTINCT subreddit_id) AS tos_sub_count
    FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_tos_30_pct_20230509` AS tos
    WHERE tos.pt = PT_FEATURES
    GROUP BY 1
)
, user_subreddit_ft AS (
    SELECT
        us.* EXCEPT(pt_window_start, user_geo_country_code)
        , CASE
            WHEN us.user_geo_country_code IS NULL THEN 'MISSING'
            WHEN us.user_geo_country_code IN UNNEST(TARGET_COUNTRY_CODES) THEN us.user_geo_country_code
            ELSE 'ROW'
        END AS user_geo_country_code_top
        , COALESCE(tos.tos_30_pct, 0) AS tos_30_pct
    FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230509` AS us
        -- Add ToS_pct for target subreddit
        LEFT JOIN `reddit-employee-datasets.david_bermejo.pn_ft_user_tos_30_pct_20230509` AS tos
            ON us.user_id = tos.user_id
                AND us.target_subreddit_id = tos.subreddit_id
    WHERE us.pt = PT_FEATURES
        AND tos.pt = PT_FEATURES
)
, user_ft AS (
    SELECT
        CASE
            WHEN cl.legacy_user_cohort = 'new' THEN 1
            WHEN cl.legacy_user_cohort = 'resurrected' THEN 2
            WHEN cl.legacy_user_cohort = 'casual' THEN 3
            WHEN cl.legacy_user_cohort IS NULL THEN 4  -- '_missing_' or 'dead'
            WHEN cl.legacy_user_cohort = 'core' THEN 5
            ELSE 0
        END AS legacy_user_cohort_ord
        , u.* EXCEPT(pt, user_geo_country_code)
    FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_20230509` AS u
        -- USER cohort, Legacy
        LEFT JOIN (
            SELECT user_id, legacy_user_cohort
            FROM `data-prod-165221.attributes_platform.user_rolling_legacy_user_cohorts`
            WHERE DATE(pt) = PT_FEATURES
        ) AS cl
            ON u.user_id = cl.user_id

    WHERE u.pt = PT_FEATURES
        -- Only keep users from target geos
        AND COALESCE(u.user_geo_country_code, "") IN UNNEST(TARGET_COUNTRY_CODES)
)
, final_table AS (
    SELECT
        -- These index columns are needed for the final outputs (not inference)
        us.pt
        , us.target_subreddit_id
        , us.target_subreddit
        , us.user_id
        , us.user_geo_country_code_top

        -- The rest of the columns should be used for modeling inference
        , u.* EXCEPT(user_id)
        , us.* EXCEPT(pt, user_id, target_subreddit, target_subreddit_id, user_geo_country_code_top)
        , COALESCE(tsc.tos_sub_count, 0) AS tos_30_sub_count
        , s.* EXCEPT(subreddit_id)
    FROM user_subreddit_ft AS us
        -- Subreddit features, also limit subreddits to score by using inner join
        INNER JOIN subreddit_ft AS s
            ON us.target_subreddit_id = s.subreddit_id
        -- User-level features, inner join to focus only on target countries
        INNER JOIN user_ft AS u
            ON us.user_id = u.user_id

        -- Get count of subs in ToS
        LEFT JOIN subreddit_per_user_count AS tsc
            ON us.user_id = tsc.user_id
)

SELECT *
FROM final_table
);  -- Close CREATE/INSERT parens



-- ============
-- Export data to GCS because querying such a huge table takes forever and a half
-- ===
-- EXPORT DATA OPTIONS(
--     uri='gs://i18n-subreddit-clustering/pn_model/runs/inference/20230507/*.parquet',
--     format='PARQUET',
--     overwrite=true
-- ) AS
-- SELECT *
-- FROM `reddit-employee-datasets.david_bermejo.pn_ft_all_20230509`
-- WHERE pt = '2023-05-07'
-- ;


-- ============
-- Test CTEs
-- ===

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
