-- C. Get user<>subreddit features for top-subreddits (query A) & key users (query B) INFERENCE
-- ETA:
--    *  4 minutes. Training 30-day window, 1-3 subreddits. Slot time: 9.5 hours
--    *  7 mins(!!). [NEW] full data: 30-day window, 26k subreddits, users with 2+ consumes_and_views.  slot time: ~3.5 days

-- For model INFERENCE we pick users who have some activity in L7 to L30 days (clicks, screenview, consumes)
--   b/c otherwise we waste time processing & scoring users with very low probability of receiving & clicking
-- HOWEVER for model TRAINING we need to keep ALL users that received a PN (even low activity users)
--    b/c we don't know how they were selected

-- 2023-05-10 Changes (running inference on 9 Billion rows is too much)
--  * Increase min consumes & views: 3 -> 5
--  * Add iOS & android min consumes: 0 -> 1
--  * Set view count window: -29

DECLARE PT_DT DATE DEFAULT "2023-06-03";
-- Expand to 30 days total to get at least 1 month's given that in the prev model 1 month was the minimum
-- 1 month = -29 days = (30 days)
DECLARE PT_WINDOW_START DATE DEFAULT PT_DT - 29;  -- default: 29

-- TODO(djb) Training: These minimums get ignore for training
DECLARE MIN_CONSUMES_AND_VIEWS NUMERIC DEFAULT 2;  -- 2
DECLARE MIN_CONSUMES_IOS_OR_ANDROID NUMERIC DEFAULT 1;  -- 1

-- ==================
-- Only need to create the first time we run it
-- === OR REPLACE
-- CREATE TABLE `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230529`
-- PARTITION BY pt
-- CLUSTER BY target_subreddit_id
-- AS (

-- ==================
-- After table is created, we can delete a partition & update it
-- ===
DELETE
    `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230529`
WHERE
    pt = PT_DT
;
-- Insert latest data
INSERT INTO `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230529`
(

WITH
selected_subreddits AS (
    -- Pick subreddits from table where we filter by rating & activity
    SELECT s.subreddit_id, s.subreddit_name
    FROM `reddit-employee-datasets.david_bermejo.pn_ft_subreddits_20230525` AS s

    WHERE s.pt = PT_DT
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
    FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_20230530`
    WHERE pt = PT_DT
        -- Make sure to apply the other clauses as part of an AND so that we don't look at previous partitions and get
        --  multiple rows per user
        -- NOTE: user Country filter should've been applied in upstream table!
        AND (
            user_clicks_pn_t7 >= 1
            OR user_receives_pn_t30 >= 1
            OR (
                (COALESCE(num_post_consumes_ios_30, 0) + COALESCE(num_post_consumes_android_30, 0)) >= MIN_CONSUMES_IOS_OR_ANDROID
            )
            OR (
                (COALESCE(num_post_consumes_30, 0) + COALESCE(screen_view_count_14d, 0)) >= MIN_CONSUMES_AND_VIEWS
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
    WHERE _PARTITIONTIME = TIMESTAMP(CURRENT_DATE() - 2)
        AND subscribe_date <= TIMESTAMP(PT_DT)
)
, user_sub_agg AS (
    SELECT
        ag.subreddit_id
        , ag.user_id
        , ua.user_geo_country_code

        -- TODO(djb): add linear decay
        -- Use 0.018518518519 to make it linear decay from a 0% reduction to today's data to 50% reduction to 30 days ago data
        -- , 1 - TIMESTAMP_DIFF(seeds.pt, activity.pt, DAY) * 0.018518518519 AS linear_decay_multiplier
        , SUM(features.upvotes) AS us_upvotes_l14
        , SUM(features.comments) AS us_comments_l14
        , SUM(features.posts) AS us_posts_l14

        -- TODO(djb): only compute trending PNs for L7 & l14 days
        --   because going for the full L30 days might give us the wrong impression if user unsubscribed
        -- , SUM(features.email_digest_clicked) AS us_email_digest_click_l14
        , SUM(features.trending_pn_received) AS us_trend_pn_receive_l14
        , SUM(features.trending_pn_clicked) AS us_trend_pn_click_l14

        , SUM(features.sessions) AS us_sessions_l14
        , SUM(features.screenviews) AS us_screenviews_l14
        , SUM(features.post_screenviews) AS us_post_screenviews_l14
        , SUM(features.distinct_posts_viewed) AS us_distinct_posts_viewed_l14
    FROM (
        SELECT user_id, subreddit_id, features
        FROM `reddit-growth-prod.growth_team_tables.data_aggregation_user_subreddit_activity`
        -- TODO(djb): set fixed window at -13 (14 days)
        WHERE pt BETWEEN TIMESTAMP(PT_DT - 13) AND TIMESTAMP(PT_DT)
            AND user_id IS NOT NULL
            AND subreddit_id IS NOT NULL
    ) AS ag
        INNER JOIN users_above_threshold AS ua
            ON ag.user_id = ua.user_id
        INNER JOIN selected_subreddits AS sel
            ON ag.subreddit_id = sel.subreddit_id
    -- WHERE 1=1

    GROUP BY 1,2,3
)
, us_daily AS (
    SELECT
        usd.subreddit_id
        , usd.user_id
        , ua.user_geo_country_code

        , SUM(ios_l1) AS us_ios_days_active_l30
        , SUM(android_l1) AS us_android_days_active_l30
        , SUM(l1) AS us_days_active_l30
        -- , SUM(votes_l1) AS us_votes_l30  -- repeat from ag.features.upvotes
        -- , SUM(comments_l1) AS us_comments_l30  -- repeat from ag.features.comments
        -- , SUM(posts_l1) AS us_posts_l30  -- repeat from ag.features.posts

    FROM (
        SELECT
            slo.subreddit_id
            , subreddit_name
            , user_id
            , ios_l1
            , android_l1
            , l1
        FROM `data-prod-165221.cnc.user_subreddit_daily` AS usd
            INNER JOIN (
                SELECT subreddit_id, name, dt
                FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
                WHERE dt = GREATEST(PT_DT, CURRENT_DATE - 28)
            ) AS slo
                ON usd.subreddit_name = LOWER(slo.name)
        WHERE pt BETWEEN TIMESTAMP(PT_WINDOW_START) AND TIMESTAMP(PT_DT)
    ) AS usd
        INNER JOIN users_above_threshold AS ua
            ON usd.user_id = ua.user_id
        INNER JOIN selected_subreddits AS sel
            ON usd.subreddit_id = sel.subreddit_id
    -- WHERE 1=1

    GROUP BY 1,2,3
)
, user_subreddit_combined_raw AS (
    SELECT
        -- COALESCE views & subscribers into a single table before joining to final geo info
        COALESCE(ag.user_id, usd.user_id) AS user_id
        , COALESCE(ag.subreddit_id, usd.subreddit_id) AS subreddit_id
        , COALESCE(ag.user_geo_country_code, usd.user_geo_country_code) AS user_geo_country_code
        , ag.subscribed

        , ag.* EXCEPT(user_id, subreddit_id, user_geo_country_code, subscribed)
        , usd.* EXCEPT(user_id, subreddit_id, user_geo_country_code)

    FROM (
        -- Coalesce in subquery to make sure we don't duplicate user rows
        SELECT
            COALESCE(su.user_id, ag.user_id) AS user_id
            , COALESCE(su.subreddit_id, ag.subreddit_id) AS subreddit_id
            , COALESCE(su.user_geo_country_code, ag.user_geo_country_code) AS user_geo_country_code
            , IF(su.subreddit_id IS NOT NULL, 1, 0) subscribed
            , ag.* EXCEPT(user_id, subreddit_id, user_geo_country_code)

        FROM user_sub_agg AS ag
        FULL OUTER JOIN subscribes_base AS su
            ON ag.user_id = su.user_id AND ag.subreddit_id = su.subreddit_id
    ) AS ag
        FULL OUTER JOIN us_daily AS usd
            ON ag.user_id = usd.user_id AND ag.subreddit_id = usd.subreddit_id
)
, user_subreddit_final AS (
    SELECT
        PT_DT AS pt
        , PT_WINDOW_START AS pt_window_start
        , v.subreddit_id AS target_subreddit_id
        , se.subreddit_name AS target_subreddit
        , v.user_id
        , v.subscribed
        , v.user_geo_country_code
        , COALESCE(cl.sub_dau_perc_l28, -0.1) AS sub_dau_perc_l28
        , COALESCE(cl.perc_by_country_sd, 0) AS perc_by_country_sd
        , LN(1 + COALESCE(us_screenviews_l14, 0)) AS us_screenviews_l14_log
        , LN(1 + COALESCE(us_distinct_posts_viewed_l14, 0)) AS us_distinct_posts_viewed_l14_log
        , LN(1 + COALESCE(us_post_screenviews_l14, 0)) AS us_post_screenviews_l14_log
        , LN(1 + COALESCE(us_trend_pn_receive_l14, 0)) AS us_trend_pn_receive_l14_log

        , SAFE_DIVIDE(us_ios_days_active_l30, us_days_active_l30) AS us_days_active_ios_l30_pct
        , SAFE_DIVIDE(us_android_days_active_l30, us_days_active_l30) AS us_android_days_active_l30_pct

        , v.* EXCEPT(subreddit_id, user_id, user_geo_country_code, subscribed)

    FROM user_subreddit_combined_raw AS v

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
            WHERE pt = TIMESTAMP(PT_DT)
        ) AS cl
            ON v.subreddit_id = cl.subreddit_id
                AND v.user_geo_country_code = cl.geo_country_code

    -- TODO(djb): decide whether these extra filters are worth it. We might score a lot of low probability subscribers
    -- WHERE 1=1
        -- Add some more constraints on top of users-above-threshold:
        -- Keep all users who viewed target subreddit
        -- v.view_and_consume_unique_count >= 1

        -- OR (
        --     -- TODO(djb): Only keep subscribers w/o view IF they have 1+ receive or click on ANY PN or a consume on ios/android
        --     v.subscribed = 1
        --     AND (
        --         COALESCE(ua.user_receives_pn_t7, 0) + COALESCE(ua.user_clicks_pn_t7, 0)
        --     ) >= 1
        -- )

)

-- Final CREATE/INSERT query
SELECT * FROM user_subreddit_final
);  -- Close CREATE TABLE parens


-- ============
-- Test CTEs
-- ===

-- SELECT
--     (SELECT COUNT(*) FROM selected_subreddits) AS sel_subreddit_count
--     , (SELECT COUNT(*) FROM users_above_threshold) AS users_above_threshold_count
--     , (SELECT COUNT(*) FROM subscribes_base) AS users_subscribed_count
--     -- , (SELECT COUNT(*) FROM user_consumes) AS user_consumes_count  -- consumes is super expensive, skip for now
--     , (SELECT COUNT(*) FROM user_sub_agg) AS user_sub_agg_count
--     , (SELECT COUNT(*) FROM us_daily) AS user_daily_count
--     -- , (SELECT COUNT(*) FROM user_subreddit_combined_raw) AS user_subreddit_combined_raw_count
--     , (SELECT COUNT(*) FROM user_subreddit_final) AS user_subreddit_final_count
-- ;

-- SELECT *
-- FROM selected_subreddits;

-- SELECT *
-- FROM users_above_threshold
-- -- WHERE user_geo_country_code NOT IN (
-- --     "MX", "ES", "AR"
-- --     , "DE", "AT", "CH"
-- --     , "US", "GB", "IN", "CA", "AU", "IE"
-- --     , "FR", "NL", "IT"
-- --     , "BR", "PT"
-- --     , "PH"
-- -- )
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
-- FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230529`
-- WHERE pt = "2023-05-29"
    -- AND user_id IS NOT NULL
-- ;


-- Check for dupes in this table. ETA: 1 mins (slot: 4 hrs)
-- We expect zero dupes WHEN we group by user_id + target_subreddit (or target_subreddit_id)
-- SELECT
--     user_id
--     , target_subreddit

--     , COUNT(*) AS dupe_count
-- FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230529`
-- WHERE pt = "2023-05-29"
-- GROUP BY 1,2
-- HAVING dupe_count > 1

-- ORDER BY dupe_count DESC, target_subreddit, user_id
-- ;


-- Select users with NULL geo-country
--  We expect them to have pct by country = -1, z-score = 0.0
-- SELECT *
-- FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230525`
-- WHERE pt = "2023-05-07"
--     AND user_geo_country_code IS NULL
-- LIMIT 1000
-- ;
