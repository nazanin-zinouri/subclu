-- C. Get user<>subreddit features for top-subreddits (query A) & key users (query B)
-- ETA:
--    *  6 minutes. test data:  1-day window,  03 subreddits, users with 5+ consumes_and_views.  slot time:  17 hours
--    *  ?? HOURS.  full data: 21-day window, 25k subreddits, users with 5+ consumes_and_views.  slot time: ~90 days
--    * 1.5 HOURS.  full data: 30-day window, 26k subreddits, users with 3+ consumes_and_views.  slot time: ~90 days
-- For model INFERENCE we pick users who have some activity in L7 to L30 days (clicks, screenview, consumes)
--   b/c otherwise we waste time processing & scoring users with very low probability of receiving & clicking
-- HOWEVER for model TRAINING we need to keep ALL users that received a PN (even low activity users)
--    b/c we don't know how they were selected
-- Example filters for activity:
--   * 3+ (consumes + views) on all subreddits OR
--   * 1+ PN click in L7

-- 2023-05-10 Changes (running inference on 9 Billion rows is too much)
--  * Increase min consumes & views: 3 -> 5
--  * Add iOS & android min consumes: 0 -> 2
--  * Decrease view count window: -29 -> -20
--  * Increase receives for subscribers: 0 -> 1 (for ANY PN type)

DECLARE PT_DT DATE DEFAULT "2023-05-06";
-- Expand to 30 days total to get at least 1 month's given that in the prev model 1 month was the minimum
-- 1 month = -29 days = (30 days)
-- 3 weeks = -20 days = (21 days)
DECLARE PT_WINDOW_START DATE DEFAULT PT_DT - 20;

DECLARE MIN_CONSUMES_AND_VIEWS NUMERIC DEFAULT 5;
DECLARE MIN_CONSUMES_IOS_OR_ANDROID NUMERIC DEFAULT 2;


-- ==================
-- Only need to create the first time we run it
-- === OR REPLACE
-- CREATE TABLE `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230509`
-- PARTITION BY PT
-- AS (

-- ==================
-- After table is created, we can delete a partition & update it
-- ===
DELETE
    `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230509`
WHERE
    pt = PT_DT
;
-- Insert latest data
INSERT INTO `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230509`
(

WITH
selected_subreddits AS (
    -- Pick subreddits from table where we filter by rating & activity
    SELECT subreddit_id, subreddit_name
    FROM `reddit-employee-datasets.david_bermejo.pn_ft_subreddits_20230509`
    WHERE pt = PT_DT

    -- For testing, we can limit to only the top subreddits
    --     AND relevant_geo_country_code_count >= 2
    --     AND users_l7 >= 1000
    -- ORDER BY users_l7 DESC
    -- LIMIT 3
)
, users_above_threshold AS (
    SELECT
        user_id
        , user_geo_country_code
        , user_receives_pn_t7
        , user_clicks_pn_t7
    FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_20230509`
    WHERE pt = PT_DT
        -- Make sure to apply the other clauses as part of an AND so that we don't look at previous partitions and get
        --  multiple rows per user
        AND (
            user_clicks_pn_t7 >= 1
            OR (
                (COALESCE(num_post_consumes_ios, 0) + COALESCE(num_post_consumes_android, 0)) >= MIN_CONSUMES_IOS_OR_ANDROID
            )
            OR (
                (COALESCE(num_post_consumes, 0) + COALESCE(screen_view_count_14d, 0)) >= MIN_CONSUMES_AND_VIEWS
            )
        )
)
, subscribes_base AS (
    SELECT
        -- We need distinct in case a user subscribes multiple times to the same sub
        DISTINCT
        s.user_id
        , ua.user_geo_country_code
        , subscriptions.subreddit_id AS subreddit_id
    from `data-prod-165221.ds_v2_postgres_tables.account_subscriptions` AS s
        LEFT JOIN UNNEST(subscriptions) as subscriptions
        INNER JOIN users_above_threshold AS ua
            ON s.user_id = ua.user_id
        INNER JOIN selected_subreddits AS sel
            ON subscriptions.subreddit_id = sel.subreddit_id
    WHERE DATE(_PARTITIONTIME) = (CURRENT_DATE() - 2)
        AND DATE(subscribe_date) <= PT_DT
)
, users_with_views AS (
    SELECT
        -- Get the subreddit name at the end b/c we don't want to get errors when a subreddit name changes
        v.user_id
        , v.subreddit_id
        , v.user_geo_country_code

        , COALESCE(
            COUNT(DISTINCT v.post_id), 0
        ) AS view_and_consume_unique_count
        , COALESCE(
            COUNT(DISTINCT(IF(v.action='consume', post_id, NULL))), 0
         ) AS consume_unique_count
        , SUM(IF(v.action='view', 1, 0)) AS view_count
        , SUM(IF(v.action='consume', 1, 0)) AS consume_count
        , SUM(IF(v.action='consume' AND app_name='ios', 1, 0)) AS consume_ios_count
        , SUM(IF(v.action='consume' AND app_name='android', 1, 0)) AS consume_android_count
    FROM (
        SELECT
            -- Don't use subreddit name here b/c it could change in a given window
            pc.subreddit_id
            , pc.user_id
            , ua.user_geo_country_code
            , post_id
            , app_name
            , action
        FROM `data-prod-165221.fact_tables.post_consume_post_detail_view_events` AS pc
            INNER JOIN users_above_threshold AS ua
                ON pc.user_id = ua.user_id
            INNER JOIN selected_subreddits AS sel
                ON pc.subreddit_id = sel.subreddit_id

        WHERE DATE(pt) BETWEEN PT_WINDOW_START AND PT_DT
            AND pc.user_id IS NOT NULL
            AND action IN ('consume', 'view')
    ) AS v
    GROUP BY 1,2,3
)
, users_views_and_subscribes AS (
    SELECT
        -- Merge views & subscribers into a single table
        COALESCE(su.user_id, v.user_id) AS user_id
        , COALESCE(su.subreddit_id, v.subreddit_id) AS subreddit_id
        , COALESCE(su.user_geo_country_code, v.user_geo_country_code) AS user_geo_country_code
        , IF(su.subreddit_id IS NOT NULL, 1, 0) subscribed
        , v.* EXCEPT(user_id, subreddit_id, user_geo_country_code)
    FROM users_with_views AS v
        FULL OUTER JOIN subscribes_base AS su
            ON v.user_id = su.user_id AND v.subreddit_id = su.subreddit_id
)

-- Final CREATE/INSERT query
SELECT
    PT_DT AS pt
    , PT_WINDOW_START AS pt_window_start
    , v.subreddit_id AS target_subreddit_id
    , se.subreddit_name AS target_subreddit
    , v.user_id
    , v.subscribed
    , v.user_geo_country_code
    , COALESCE(cl.sub_dau_perc_l28, -1) AS sub_dau_perc_l28
    , COALESCE(cl.perc_by_country_sd, 0) AS perc_by_country_sd
    , LN(1 + COALESCE(view_and_consume_unique_count, 0)) AS view_and_consume_unique_count_log
    , LN(1 + COALESCE(consume_unique_count, 0)) AS consume_unique_count_log
    , LN(1 + COALESCE(view_count, 0)) AS view_count_log
    , LN(1 + COALESCE(consume_count, 0)) AS consume_count_log

    , SAFE_DIVIDE(consume_ios_count, consume_count) AS consume_ios_pct
    , SAFE_DIVIDE(consume_android_count, consume_count) AS consume_android_pct

    , v.* EXCEPT(subreddit_id, user_id, user_geo_country_code, subscribed)

FROM users_views_and_subscribes AS v
    -- Join to keep only target users
    INNER JOIN users_above_threshold AS ua
        ON v.user_id = ua.user_id

    -- Join to get subreddit_name
    LEFT JOIN selected_subreddits AS se
        ON v.subreddit_id = se.subreddit_id

    -- Get pct & z-score where  user geo = subredit geo
    LEFT JOIN (
        SELECT
            -- We need distinct b/c there can be duplicates in the community_local_scores table :((
            DISTINCT
            subreddit_id
            , geo_country_code
            , sub_dau_perc_l28
            , perc_by_country_sd
        FROM `data-prod-165221.i18n.community_local_scores`
        WHERE DATE(pt) = PT_DT
    ) AS cl
        ON v.subreddit_id = cl.subreddit_id
            AND v.user_geo_country_code = cl.geo_country_code
WHERE
    -- Add some more constraints on top of users-above-threshold:
    -- Keep all users who viewed target subreddit
    v.view_and_consume_unique_count >= 1

    OR (
        -- TODO(djb): Only keep subscribers w/o view IF they have 1+ receive or click on ANY PN
        v.subscribed = 1
        AND (
            COALESCE(ua.user_receives_pn_t7, 0) + COALESCE(ua.user_clicks_pn_t7, 0)
        ) >= 1
    )

);  -- Close CREATE TABLE parens


-- ============
-- Test CTEs
-- ===

-- SELECT *
-- FROM selected_subreddits;

-- SELECT *
-- FROM users_above_threshold
-- WHERE user_geo_country_code NOT IN (
--     "MX", "ES", "AR"
--     , "DE", "AT", "CH"
--     , "US", "GB", "IN", "CA", "AU", "IE"
--     , "FR", "NL", "IT"
--     , "BR", "PT"
--     , "PH"
-- )
-- ;

-- SELECT *
-- FROM subscribes_base;

-- SELECT *
-- FROM users_with_views
-- ORDER BY subreddit_id, view_and_consume_unique_count DESC
-- ;


-- SELECT *
-- FROM users_views_and_subscribes
-- ;

-- SELECT *
-- FROM users_views_and_subscribes
-- LIMIT 2000
-- ;


-- ============
-- Test final table
-- ===
-- Check overall counts rows in user<>subreddit features table
-- SELECT
--     -- We expect many more rows than users & subreddits
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT user_id) as user_id_count
--     , COUNT(DISTINCT target_subreddit_id) as target_subreddit_id_count
-- FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230509`
-- WHERE pt = "2023-05-07"
    -- AND user_id IS NOT NULL
-- ;


-- Check for dupes in this table. ETA: 7 mins
-- We expecte zero dupes WHEN we group by user_id + target_subreddit (or target_subreddit_id)
-- SELECT
--     user_id
--     -- , target_subreddit_id
--     , target_subreddit

--     , COUNT(*) AS dupe_count
-- FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230509`
-- WHERE pt = "2023-05-07"
-- GROUP BY 1,2
-- -- HAVING dupe_count > 1

-- ORDER BY dupe_count DESC, target_subreddit, user_id
-- ;


-- Select users with NULL geo-country
--  We expect them to have pct by country = -1, z-score = 0.0
-- SELECT *
-- FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230509`
-- WHERE pt = "2023-05-07"
--     AND user_geo_country_code IS NULL
-- LIMIT 1000
-- ;
