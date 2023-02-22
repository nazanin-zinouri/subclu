-- Use this query to decide on active window to count a post as active
-- Hypothesis: 24hrs (current) is too short and
--  - gives us a lot of false positives (posts that aren't local show up as local)
--  - gives us a lot of false negatives (posts that ARE local don't show up as local)

-- NVM: it looks like most views happen within the first 2 days of pt (~48 hours)
--  But the query is super expensive. Not worth digging into it more unless I have
--  time to burn.

-- Pull from same source as ETL
--  NOTE: The ETL counts both: consumes OR views
SELECT
    ve.pt
    , ve.post_id
    -- , geo_country_code
    -- , user_id
    -- , noun
    -- , action

    , COUNT(DISTINCT user_id) AS total_users_count
    -- , SUM(total_users) AS total_users_sum
    -- , SUM(total_users_l28) AS total_users_l28_sum

FROM `data-prod-165221.fact_tables.post_consume_post_detail_view_events` AS ve
WHERE DATE(ve.pt) BETWEEN "2022-10-11" AND "2022-10-15"
    AND ve.post_id IN (
        't3_y1e4ui'
        , 't3_y13tjy'
        , 't3_y1l9io'
        , 't3_y1kly7'
    )
GROUP BY 1, 2
ORDER BY post_id, ve.pt
;


-- Check data from table that does aggregation
--   FINDING: it doesn't match the local-score table... is it double counting or doing cumulative counts?
-- SELECT *
-- FROM `data-prod-165221.i18n.post_stats`
-- WHERE DATE(pt) = "2022-10-12"
--     AND post_id IN (
--         't3_y1e4ui'
--         , 't3_y13tjy'
--     )
-- LIMIT 1000
-- ;


-- Check sum of views, consumes, & users
--  This table is an aggregation from the source table for ETL, it might double count users if they login from multiple geos in one day (e.g., using VPN or traveling)
--  Using it for testing because it's MUCH faster than pulling form raw views
-- SELECT
--     pt
--     , post_id

--     , SUM(users_post_consumes) AS users_consumes_sum
--     , SUM(total_users) AS total_users_sum
--     -- `total_users_l28` -> closes to post_local_score, when we have the same `pt` (1 day after create dt)
--     , SUM(total_users_l28) AS total_users_l28_sum

-- FROM `data-prod-165221.i18n.post_stats`
-- WHERE DATE(pt) = "2022-10-12"
--     AND post_id IN (
--         't3_y1e4ui'
--         , 't3_y13tjy'
--         , 't3_y1l9io'
--         , 't3_y1kly7'
--     )
-- GROUP BY 1, 2
-- ;

--- Try pulling data from a new source to compare
-- Use this query to decide on active window to count a post as active
-- Hypothesis: 24hrs (current) is too short and
--  - gives us a lot of false positives (posts that aren't local show up as local)
--  - gives us a lot of false negatives (posts that ARE local don't show up as local)

DECLARE PT_DATE DATE DEFAULT '2022-11-14';
DECLARE POST_CREATED_DT DATE DEFAULT '2022-10-11';
DECLARE TEST_SUBS DEFAULT [
    'mexico'
    -- , 'askreddit'
    -- , 'mapporn'
    -- , 'fragreddit'
    -- , 'de'
    -- , 'france'
    -- , 'bundesliga'
    -- , 'ligamx'
    -- , 'casualuk'
    -- , 'personalfinancecanada'
    -- , 'dataisbeautiful'
];

WITH
total_post_views AS (
    SELECT
        pl.subreddit_id
        , subreddit_name
        , post_create_dt
        , pl.post_id

        , SUM(post_dau_24hr) AS total_dau
        , MIN(DATE(pl.pt)) AS first_dt_with_views
        , MAX(DATE(pl.pt)) AS last_dt_with_views
    FROM `data-prod-165221.i18n.post_local_scores` AS pl
    WHERE DATE(pl.pt) BETWEEN POST_CREATED_DT AND PT_DATE
        AND pl.post_create_dt = POST_CREATED_DT
        AND subreddit_name IN UNNEST(TEST_SUBS)

    GROUP BY 1,2,3,4
)

-- , last_day_with_views AS (
--     SELECT


--     FROM `data-prod-165221.i18n.post_local_scores` AS pl
--         INNER JOIN total_post_views AS tp
--             ON pl.post_id = tp.post_id
-- )

SELECT *
FROM total_post_views
ORDER BY last_dt_with_views DESC # total_dau DESC
