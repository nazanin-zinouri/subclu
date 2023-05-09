-- B. Get user<>subreddit features for top-subreddits (query A)

-- NOTE: for final inference Pick users who have more than 2 (screenview + consumes)
--  b/c otherwise we waste time processing & scoring users with only consumes
--  But for training we want to keep these low activity users b/c we don't know how they were
--  selected/filtered
-- TODO(djb): Filters
--   - Keep subscribers w/o subreddit view ONLY if they have
--       * 5+ (consumes + views) in all subreddits OR
--       * 1+ PN click in L7
--   - Keep only view users with
--       * 3+ (consumes + views) in all subreddits OR
--       * 1+ PN click in L7

DECLARE PT_DT DATE DEFAULT "2023-05-01";
-- Expand to 30 days total to get at least 1 month's given that in the prev model 1 month was the minimum
DECLARE PT_WINDOW_START DATE DEFAULT PT_DT - 1;  -- 1 month = -29 days = (30 days)


-- ==================
-- Only need to create the first time we run it
-- === OR REPLACE
CREATE TABLE `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230502`
PARTITION BY PT
AS (

-- ==================
-- After table is created, we can delete a partition & update it
-- ===
-- DELETE
--     `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230502`
-- WHERE
--     pt = PT_FEATURES
-- ;
-- -- Insert latest data
-- INSERT INTO `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230502`
-- (

WITH
selected_subreddits AS (
    -- Pick subreddits from table where we filter by rating & activity
    SELECT subreddit_id, subreddit_name
    FROM `reddit-employee-datasets.david_bermejo.pn_ft_subreddits_20230502`
    WHERE pt = PT_DT
        -- For testing, we can limit to only the top subreddits
    --     AND relevant_geo_country_code_count >= 2
    --     AND users_l7 >= 1000
    -- ORDER BY users_l7 DESC
    -- LIMIT 3
)
, users_above_threshold AS (
    -- adding this filter actually seems to take longer!
    SELECT
        user_id
    FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_20230502`
    WHERE pt = PT_DT
        AND user_clicks_pn_t7 >= 1
        OR (
            (num_post_consumes + screen_view_count_14d) >= 3
        )
)
, subscribes_base AS (
    SELECT
        -- We need distinct in case a user subscribes multiple times to the same sub
        DISTINCT
        s.user_id
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
    GROUP BY 1,2
)
, users_views_and_subscribes AS (
    SELECT
        COALESCE(su.user_id, v.user_id) AS user_id
        , COALESCE(su.subreddit_id, v.subreddit_id) AS subreddit_id
        , IF(su.subreddit_id IS NOT NULL, 1, 0) subscribed
        , v.* EXCEPT(user_id, subreddit_id)
    FROM users_with_views AS v
        FULL OUTER JOIN subscribes_base AS su
            ON v.user_id = su.user_id AND v.subreddit_id = su.subreddit_id
)
, users_with_geo AS (
    SELECT
        se.subreddit_name
        , uv.*
        , g.geo_country_code AS user_geo_country_code

    FROM users_views_and_subscribes AS uv
        LEFT JOIN selected_subreddits AS se
            ON uv.subreddit_id = se.subreddit_id
        LEFT JOIN (
            SELECT
                user_id
                , geo_country_code
            FROM `data-prod-165221.channels.user_geo_6mo_lookback`
            WHERE
                DATE(pt) = PT_DT
        ) AS g
            ON uv.user_id = g.user_id
)

SELECT
    PT_DT AS pt
    , PT_WINDOW_START AS pt_window_start
    , v.subreddit_id AS target_subreddit_id
    , v.subreddit_name AS target_subreddit
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

    , v.* EXCEPT(subreddit_id, subreddit_name, user_id, user_geo_country_code, subscribed)

FROM users_with_geo AS v
    -- Get pct & z-score for user<>subredit GEO
    LEFT JOIN (
        SELECT
            subreddit_id
            , geo_country_code
            , sub_dau_perc_l28
            , perc_by_country_sd
        FROM `data-prod-165221.i18n.community_local_scores`
        WHERE DATE(pt) = PT_DT
    ) AS cl
        ON v.subreddit_id = cl.subreddit_id
            AND v.user_geo_country_code = cl.geo_country_code

);  -- Close CREATE TABLE parens


-- ============
-- Test CTEs
-- ===

-- SELECT *
-- FROM selected_subreddits;

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
-- FROM users_with_geo
-- LIMIT 2000
-- ;
