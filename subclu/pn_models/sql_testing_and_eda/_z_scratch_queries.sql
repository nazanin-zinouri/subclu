-- The table that calculates recent PN activity is not partitioned
DECLARE PT_FEATURES DATE DEFAULT "2022-12-01";
DECLARE PT_WINDOW_START DATE DEFAULT PT_FEATURES - 7;

WITH user_actions_t7 AS (
    SELECT
      user_id,
      COUNT(click_endpoint_timestamp) user_clicks_pn_t7,
      COUNT(receive_endpoint_timestamp) user_receives_pn_t7,
      COUNT(
        CASE
          WHEN notification_type='lifecycle_post_suggestions'
            THEN click_endpoint_timestamp
          ELSE NULL
        END
    ) user_clicks_trnd_t7
    FROM `data-prod-165221.channels.push_notification_events`
    WHERE
        DATE(pt) BETWEEN PT_WINDOW_START AND PT_FEATURES
        AND NOT REGEXP_CONTAINS(notification_type, "email")
        AND receive_endpoint_timestamp IS NOT NULL
  GROUP BY user_id
)

SELECT
    *
FROM user_actions_t7 AS pn
WHERE
    user_id IN (
        't2_9zijedm0', 't2_is69c8yu'
    )
;



-- test getting subscribes using subscribe date
DECLARE PT_FEATURES DATE DEFAULT "2022-12-01";

WITH
subscribes AS (
    -- TODO(djb): FIX: use date_subscribed (nested) to get actual subscription
    SELECT
        s.user_id,
        subscriptions.subreddit_id subreddit_id
    from data-prod-165221.ds_v2_postgres_tables.account_subscriptions AS s,
        UNNEST(subscriptions) as subscriptions
        -- INNER JOIN selected_users AS u
        --     ON s.user_id = u.user_id
    WHERE DATE(_PARTITIONTIME) = (CURRENT_DATE() - 2)
        AND DATE(subscribe_date) <= PT_FEATURES
        AND user_id IN (
            't2_5lt5oiqv', 't2_6yxrzyy'
        )
)

SELECT *
FROM subscribes
;


-- Sample query to get top users for PN campaign
WITH ranked_users AS (
    SELECT
        pn.* EXCEPT(target_subreddit_id, top_users)
        , t.*
        -- Create rank per USER so we don't send more than one PN per user
        , ROW_NUMBER() OVER (
                PARTITION BY t.user_id
                ORDER BY t.click_proba DESC
        ) rank_unique_user
    FROM `reddit-employee-datasets.david_bermejo.pn_model_test` AS pn
        LEFT JOIN UNNEST(top_users) AS t
    WHERE pt = "2022-12-01"

        AND geo_country_code_top IN (
            'DE', "MISSING", "US", "ROW", "AT"
        )
        AND target_subreddit IN (
            'fragreddit', 'ich_iel'
        )
        AND t.user_rank_by_sub_and_geo <= 15

    QUALIFY rank_unique_user = 2

    ORDER BY t.click_proba DESC
    LIMIT 100
)

SELECT * EXCEPT(rank_unique_user)
FROM ranked_users
;


-- Check # of potential targets for KSI
SELECT
    m.pt
    , m.target_subreddit
    , m.user_geo_country_code
    , COUNT(DISTINCT t.user_id) AS user_count
FROM `reddit-growth-prod.pn_targeting.pn_model_subreddit_user_click_v1` AS m
    LEFT JOIN UNNEST(top_users) AS t
WHERE
    pt = (
        SELECT DATE(PARSE_TIMESTAMP("%Y%m%d", MAX(partition_id)))
        FROM `reddit-growth-prod.pn_targeting`.INFORMATION_SCHEMA.PARTITIONS
        WHERE table_name = "pn_model_subreddit_user_click_v1"
    )
    AND target_subreddit IN ('ksi')
GROUP BY 1,2,3
ORDER BY user_count DESC
;


-- Create prod table for PN subreddit<>user model
DECLARE PT_TARGET DATE DEFAULT "2023-05-07";

-- ==================
-- Only need to create the first time we run the script
-- === OR REPLACE
CREATE TABLE `reddit-growth-prod.pn_targeting.pn_model_subreddit_user_click_v1`
PARTITION BY pt
AS (

-- ==================
-- After table is created, we can delete a partition & update it
-- ===
-- DELETE
--     `reddit-growth-prod.pn_targeting.pn_model_subreddit_user_click_v1`
-- WHERE
--     pt = PT_TARGET
-- ;

-- -- Insert latest data
-- INSERT INTO `reddit-growth-prod.pn_targeting.pn_model_subreddit_user_click_v1`
-- (



SELECT *
FROM `reddit-employee-datasets.david_bermejo.pn_model_output_20230510`
WHERE pt = PT_TARGET
);  -- Close CREATE/INSERT parens


-- Check zeldan PN (Sahil's table)
SELECT
    -- COUNT(DISTINCT device_id) AS device_count
    -- , COUNT(DISTINCT user_id) AS user_id_count
    *
FROM `reddit-employee-datasets.sahil_verma.totk_pn_ml_targeting_20230512`
LIMIT 1000
;

-- Check overal stats for training data
SELECT
    -- pt_send
    pn_id
    , SUM(send) AS send_total
    , SUM(receive) AS receive_total
    , SUM(receive_not_suppressed) AS receive_not_suppressed_total
    , SUM(click) AS click_total
    , SAFE_DIVIDE(SUM(click), SUM(receive)) AS ctr_receive
    , SAFE_DIVIDE(SUM(click), SUM(receive_not_suppressed)) AS ctr_receive_no_suppressed
FROM `reddit-employee-datasets.david_bermejo.pn_training_data_20230515`
GROUP BY 1
ORDER BY send_total DESC
;


-- ============
-- Test counts for different thresholds
-- ===
-- The main table should include more users
-- ~102 million users
-- SELECT
--     pt
--     , COUNT(*) AS row_count
--     , COUNT(DISTINCT target_subreddit_id) AS subreddit_count
--     , COUNT(DISTINCT user_geo_country_code) AS country_code_count
--     , COUNT(DISTINCT t.user_id) AS user_count
-- FROM `reddit-employee-datasets.david_bermejo.pn_model_output_20230510`
--     LEFT JOIN UNNEST(top_users) AS t
-- GROUP BY 1
-- ORDER BY user_count DESC
-- ;


-- This `test` table should include fewer users (b/c of a higher threshold and lower rank)
--  Only ~4 million users
-- SELECT
--     pt
--     , COUNT(*) AS row_count
--     , COUNT(DISTINCT target_subreddit_id) AS subreddit_count
--     , COUNT(DISTINCT user_geo_country_code) AS country_code_count
--     , COUNT(DISTINCT t.user_id) AS user_count
-- FROM `reddit-employee-datasets.david_bermejo.pn_model_output_test_20230510`
--     LEFT JOIN UNNEST(top_users) AS t
-- GROUP BY 1
-- ;

-- check duplicates
-- SELECT
--     pt
--     , target_subreddit
--     , user_geo_country_code
--     , subscribed

--     , COUNT(*) AS dupe_count
-- FROM `reddit-employee-datasets.david_bermejo.pn_model_output_test_20230510`
-- GROUP BY 1,2,3,4
-- ORDER BY dupe_count DESC, target_subreddit
-- ;

-- Select latest partition
SELECT
    MAX(partition_id)
    -- , DATE(MAX(partition_id))
    , DATE(PARSE_TIMESTAMP("%Y%m%d", MAX(partition_id)))
    -- , CAST(MAX(partition_id) AS DATE FORMAT "%Y%m%d") AS latest_pt
FROM
  `reddit-growth-prod.pn_targeting`.INFORMATION_SCHEMA.PARTITIONS
WHERE
  table_name = "pn_model_subreddit_user_click_v1"
;


-- get consumes & views for specific user & compare with other tables
DECLARE PT_DT DATE DEFAULT '2023-05-23';
DECLARE PT_WINDOW_START DATE DEFAULT PT_DT - 3;

DECLARE TEST_USERS DEFAULT [
    -- 't2_fej9k5ak'
    -- , 't2_a2bjdmoi'
    -- , 't2_1fmw9l'
    -- -- Users with comments & upvotes, but no (or few) sessions or screenviews(!)
    -- , 't2_6inyx6xe'
    -- , 't2_2b8kwynv'

    -- recent users
    't2_ubebl'
    , 't2_7zbz1kgj'
    , 't2_adicsqpk'

    , 't2_kd9eyebq'
    , 't2_8can8w5p'
];

WITH pd_view_events AS (
    SELECT
        subreddit_id
        , subreddit_name
        , user_id

        -- , COALESCE(
        --     COUNT(DISTINCT v.post_id), 0
        -- ) AS us_view_and_consume_unique_count
        -- , COALESCE(
        --     COUNT(DISTINCT(IF(v.action='view', post_id, NULL))), 0
        -- ) AS us_view_unique_count  -- same as: ag.features.distinct_posts_viewed
        -- , SUM(IF(v.action='view', 1, 0)) AS us_view_count -- same as: ag.features.post_screenviews

        -- Remove extra checks now that this CTE only focuses on consumes
        , COUNT(DISTINCT post_id) AS us_consume_unique_count
        , COUNT(post_id) AS us_consume_count
        -- , COALESCE(
        --     COUNT(DISTINCT(IF(v.action='consume', post_id, NULL))), 0
        -- ) AS us_consume_unique_count
        -- , SUM(IF(v.action='consume', 1, 0)) AS us_consume_count
    FROM `data-prod-165221.fact_tables.post_consume_post_detail_view_events` AS v
    WHERE v.pt BETWEEN TIMESTAMP(PT_WINDOW_START) AND TIMESTAMP(PT_DT)
        AND v.user_id IS NOT NULL
        -- Limit to CONSUMES b/c we're already getting view info from the AGG table
        AND action IN ('consume')
        AND subreddit_name IN ("ich_iel")
        AND user_id IN UNNEST(TEST_USERS)
    GROUP BY 1,2,3
)
, user_sub_agg AS (
    SELECT
        ag.subreddit_id
        , slo.name AS subreddit_name
        , user_id

        -- TODO(djb): add linear decay
        -- Use 0.018518518519 to make it linear decay from a 0% reduction to today's data to 50% reduction to 30 days ago data
        -- , 1 - TIMESTAMP_DIFF(seeds.pt, activity.pt, DAY) * 0.018518518519 AS linear_decay_multiplier
        , SUM(features.upvotes) AS us_upvotes_l30
        , SUM(features.comments) AS us_comments_l30
        , SUM(features.posts) AS us_posts_l30

        , SUM(features.email_digest_clicked) AS us_email_digest_click_l30
        , SUM(features.trending_pn_received) AS us_trend_pn_receive_l30
        , SUM(features.trending_pn_clicked) AS us_trend_pn_click_l30

        , SUM(features.sessions) AS us_sessions_l30
        , SUM(features.screenviews) AS us_screenviews_l30
        , SUM(features.post_screenviews) AS us_post_screenviews_l30
        , SUM(features.distinct_posts_viewed) AS us_distinct_posts_viewed_l30
    FROM `reddit-growth-prod.growth_team_tables.data_aggregation_user_subreddit_activity` AS ag
        INNER JOIN (
            SELECT subreddit_id, name, dt
            FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
            WHERE dt = GREATEST(PT_DT, CURRENT_DATE - 21)
                AND LOWER(name) = 'ich_iel'
        ) AS slo
            ON ag.subreddit_id = slo.subreddit_id
    WHERE ag.pt  BETWEEN TIMESTAMP(PT_WINDOW_START) AND TIMESTAMP(PT_DT)
        AND ag.user_id IN UNNEST(TEST_USERS)
    GROUP BY 1,2,3
)
, us_daily AS (
    SELECT
        slo.subreddit_id
        , subreddit_name
        , user_id

        , SUM(ios_l1) AS us_ios_days_active_l30
        , SUM(android_l1) AS us_android_days_active_l30
        , SUM(l1) AS us_days_active_l30
        -- , SUM(votes_l1) AS us_votes_l30  -- repeat from ag.features.upvotes
        -- , SUM(comments_l1) AS us_comments_l30  -- repeat from ag.features.comments
        -- , SUM(posts_l1) AS us_posts_l30  -- repeat from ag.features.posts

    FROM `data-prod-165221.cnc.user_subreddit_daily` AS usd
        INNER JOIN (
            SELECT subreddit_id, name, dt
            FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
            WHERE dt = GREATEST(PT_DT, CURRENT_DATE - 21)
                AND LOWER(name) = 'ich_iel'
        ) AS slo
            ON usd.subreddit_name = LOWER(slo.name)
    WHERE pt BETWEEN TIMESTAMP(PT_WINDOW_START) AND TIMESTAMP(PT_DT)
        AND subreddit_name IN ('ich_iel')
        AND user_id IN UNNEST(TEST_USERS)
    GROUP BY 1,2,3
)

SELECT
    PT_DT AS pt
    , COALESCE(ve.subreddit_id, ag.subreddit_id, usd.subreddit_id) AS subreddit_id
    , COALESCE(ve.subreddit_name, ag.subreddit_name, usd.subreddit_name) AS subreddit_name
    , COALESCE(ve.user_id, ag.user_id, usd.user_id) AS user_id
    , ve.* EXCEPT(subreddit_name, subreddit_id, user_id)
    , ag.* EXCEPT(subreddit_name, subreddit_id, user_id)
    , usd.* EXCEPT(subreddit_name, subreddit_id, user_id)
FROM pd_view_events AS ve
    FULL OUTER JOIN user_sub_agg AS ag
        USING(subreddit_id, user_id)
    FULL OUTER JOIN us_daily AS usd
        ON ag.subreddit_name = usd.subreddit_name AND ag.user_id = usd.user_id
;
