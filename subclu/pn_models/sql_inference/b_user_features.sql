-- B. Create table with Consumes in L30 + PN activity in L7 days. INFERENCE
--   ETA: NEW: 4-13 minutes, Slot time: 1 day, 8hr
--      (1-2 minutes for training)
--        only selected users from training table. Get receives and clicks from separate tables to replace pn-events

DECLARE PT_FEATURES DATE DEFAULT "2023-06-03";
DECLARE PT_PN_WINDOW_START DATE DEFAULT PT_FEATURES - 7;
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
-- CREATE TABLE `reddit-employee-datasets.david_bermejo.pn_ft_user_20230530`
-- PARTITION BY pt
-- AS (

-- ==================
-- After table is created, we can delete a partition & update it
-- ===
DELETE
    `reddit-employee-datasets.david_bermejo.pn_ft_user_20230530`
WHERE
    pt = PT_FEATURES
;

-- Insert latest data
INSERT INTO `reddit-employee-datasets.david_bermejo.pn_ft_user_20230530`
(

WITH
post_consumes_agg AS (
    SELECT
        COALESCE(sv.entity_id, pc.user_id) AS user_id
        , COALESCE(sv.feature_value, 0) AS screen_view_count_14d

        -- TODO(djb): add subreddit views or consumes
        , SUM(num_post_consumes) AS num_post_consumes_30
        , SUM(num_post_consumes_home) AS num_post_consumes_home_30
        , SUM(num_post_consumes_community) AS num_post_consumes_community_30
        , SUM(num_post_consumes_post_detail) AS num_post_consumes_post_detail_30
        , SUM(IF(app_name = 'ios', num_post_consumes, 0)) AS num_post_consumes_ios_30
        , SUM(IF(app_name = 'android', num_post_consumes, 0)) AS num_post_consumes_android_30
        , SUM(num_post_consumes_nsfw) AS num_post_consumes_nsfw_30

    FROM (
        SELECT c.*
        FROM `data-prod-165221.video.post_consumes_30d_agg` AS c
            -- TODO(djb): Train only. LIMIT to training users
            -- INNER JOIN selected_users AS s
            --     ON c.user_id = s.user_id
        WHERE pt = TIMESTAMP(PT_FEATURES)
            AND c.user_id IS NOT NULL
    ) AS pc
        FULL OUTER JOIN (
            -- Get view counts (all subreddits)
            SELECT entity_id, feature_value
            FROM `data-prod-165221.user_feature_platform.screen_views_count_over_14_days_v1` AS v
                -- TODO(djb): Train only. LIMIT to training users
                -- INNER JOIN selected_users AS s
                --     ON v.entity_id = s.user_id
            WHERE pt = TIMESTAMP(PT_FEATURES)
                AND entity_id IS NOT NULL
        ) AS sv
            ON pc.user_id = sv.entity_id
    GROUP BY 1,2
)
, pn_receives AS (
    -- Split receives & clicks into separate CTEs to prevent duplicate errors
    SELECT
        r.user_id
        -- No need to do coalesce here b/c we'll have nulls later on
        , COUNT(DISTINCT
            CASE WHEN (pt > TIMESTAMP(PT_FEATURES - 7)) THEN r.subreddit_id
                ELSE NULL
            END
        ) AS user_receives_pn_subreddit_count_t7
        , COUNT(
            CASE WHEN pt > TIMESTAMP(PT_FEATURES - 7) THEN r.endpoint_timestamp
                ELSE NULL
            END
        ) AS user_receives_pn_t7
        , COUNT(
            CASE WHEN pt > TIMESTAMP(PT_FEATURES - 14) THEN r.endpoint_timestamp
                ELSE NULL
            END
        ) AS user_receives_pn_t14
        , COALESCE(COUNT(r.endpoint_timestamp), 0) user_receives_pn_t30

    FROM (
        SELECT user_id, endpoint_timestamp, pt, subreddit_id
        FROM `data-prod-165221.channels.pn_receives`
        WHERE pt BETWEEN TIMESTAMP(PT_FEATURES - 29) AND TIMESTAMP(PT_FEATURES)
            AND user_id IS NOT NULL
            AND endpoint_timestamp IS NOT NULL
            AND NOT REGEXP_CONTAINS(notification_type, "email")
    ) AS r
        -- TODO(djb): Train only: limit to users with labels
        --   For inference, try joining on post_consumes_agg
        -- INNER JOIN selected_users AS pc
        --     ON pc.user_id = r.user_id
    GROUP BY 1
)
, pn_clicks AS (
    SELECT
        c.user_id
        , COALESCE(COUNT(c.endpoint_timestamp), 0) user_clicks_pn_t7
        , COALESCE(COUNT(
            CASE
              WHEN notification_type='lifecycle_post_suggestions' THEN c.endpoint_timestamp
              ELSE NULL
            END
        ), 0) AS user_clicks_trnd_t7
    FROM (
        SELECT user_id, endpoint_timestamp, subreddit_id, notification_type
        FROM `data-prod-165221.channels.pn_clicks`
        WHERE pt BETWEEN TIMESTAMP(PT_PN_WINDOW_START) AND TIMESTAMP(PT_FEATURES)
            AND user_id IS NOT NULL
            AND endpoint_timestamp IS NOT NULL
            AND NOT REGEXP_CONTAINS(notification_type, "email")
    ) AS c
        -- TODO(djb): Train only: limit to users with labels
        --   For inference, try joining on post_consumes_agg
        -- INNER JOIN selected_users AS pc
        --     ON pc.user_id = c.user_id
  GROUP BY 1
)

-- Final select to create/insert
SELECT
    PT_FEATURES AS pt
    , pc.user_id
    , g.geo_country_code AS user_geo_country_code
    , CASE
        WHEN cl.legacy_user_cohort = 'new' THEN 1
        WHEN cl.legacy_user_cohort = 'resurrected' THEN 2
        WHEN cl.legacy_user_cohort = 'casual' THEN 3
        WHEN cl.legacy_user_cohort IS NULL THEN 4  -- '_missing_' or 'dead'
        WHEN cl.legacy_user_cohort = 'core' THEN 5
        ELSE 0
    END AS legacy_user_cohort_ord
    , LN(1 + COALESCE(pc.screen_view_count_14d, 0)) AS screen_view_count_14d_log
    , r.* EXCEPT(user_id)
    , c.* EXCEPT(user_id)
    , LN(1 + COALESCE(c.user_clicks_pn_t7, 0)) AS log_user_clicks_pn_t7
    , LN(1 + COALESCE(c.user_clicks_trnd_t7, 0)) AS log_user_clicks_trnd_t7
    , pc.num_post_consumes_30
    , LN(1 + COALESCE(pc.num_post_consumes_30, 0)) AS log_post_consumes_30
    , LN(1 + COALESCE(num_post_consumes_home_30, 0)) AS log_num_post_consumes_home_30

    , SAFE_DIVIDE(num_post_consumes_home_30, num_post_consumes_30) AS pct_post_consumes_home_30
    , SAFE_DIVIDE(num_post_consumes_community_30, num_post_consumes_30) AS pct_post_consumes_community_30
    , SAFE_DIVIDE(num_post_consumes_post_detail_30, num_post_consumes_30) AS pct_post_consumes_post_detail_30
    , SAFE_DIVIDE(num_post_consumes_ios_30, num_post_consumes_30) AS pct_post_consumes_ios_30
    , SAFE_DIVIDE(num_post_consumes_android_30, num_post_consumes_30) AS pct_post_consumes_android_30
    , SAFE_DIVIDE(num_post_consumes_nsfw_30, num_post_consumes_30) AS pct_post_consumes_nsfw_30
    , pc.* EXCEPT(user_id, num_post_consumes_30)

FROM post_consumes_agg AS pc
    LEFT JOIN pn_receives AS r
        ON pc.user_id = r.user_id
    LEFT JOIN pn_clicks AS c
        ON pc.user_id = c.user_id
    LEFT JOIN (
        SELECT
            user_id
            , geo_country_code
        FROM `data-prod-165221.channels.user_geo_6mo_lookback`
        WHERE
            pt = TIMESTAMP(PT_FEATURES)
            AND user_id IS NOT NULL
    ) AS g
        ON pc.user_id = g.user_id
    LEFT JOIN (
        -- get USER cohort, Legacy
        SELECT user_id, legacy_user_cohort
        FROM `data-prod-165221.attributes_platform.user_rolling_legacy_user_cohorts`
        WHERE pt = TIMESTAMP(PT_FEATURES)
    ) AS cl
        ON pc.user_id = cl.user_id

WHERE 1=1
    -- TODO(djb): For training, keep all the users from ALL countries! but for inference limit it
    AND COALESCE(geo_country_code, "") IN UNNEST(TARGET_COUNTRY_CODES)

); -- Close CREATE/INSERT parens



-- ============
-- Check CTEs
-- ===
-- User consumes
-- SELECT
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT user_id) AS user_count
-- FROM post_consumes_agg;

-- SELECT * FROM post_consumes_agg;

-- Check PN receives & clicks
-- SELECT *
-- FROM pn_receives
-- WHERE 1=1
--     -- AND user_clicks_pn_t7 <= 20
-- -- ORDER BY user_clicks_pn_t7 DESC
-- -- LIMIT 5000
-- ;

-- SELECT *
-- FROM pn_clicks
-- WHERE 1=1
--     -- AND user_clicks_pn_t7 <= 20
-- -- ORDER BY user_clicks_pn_t7 DESC
-- -- LIMIT 5000
-- ;


-- ============
-- Test final table
-- ===
-- Check for dupes in general
-- SELECT
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT user_id) as user_id_count
-- FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_20230525`
-- WHERE pt = "2023-05-07"
--     -- AND user_id IS NOT NULL
--     AND user_clicks_pn_t7 >= 1
--     OR (
--         (COALESCE(num_post_consumes, 0) + COALESCE(screen_view_count_14d, 0)) >= 3
--     )
-- ;


-- Find duplicated user IDs
-- SELECT
--     user_id
--     -- , user_geo_country_code

--     , COUNT(*) AS dupe_count
-- FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_20230525`
-- WHERE pt = "2023-05-07"
-- GROUP BY 1  --,2
-- -- HAVING dupe_count > 1

-- ORDER BY dupe_count DESC, user_id
-- ;
