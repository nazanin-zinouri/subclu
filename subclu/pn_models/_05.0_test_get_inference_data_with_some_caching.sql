-- Select subreddit<>user data for INFERENCE. v2023-05-01
--   For this version pick data already processes for training
--   The main use case is to get the data outputs in the right shape/format so we can
--   hand something off to Engineering

DECLARE PT_FEATURES DATE DEFAULT "2022-12-01";
DECLARE PT_WINDOW_START DATE DEFAULT PT_FEATURES - 7;
DECLARE PT_VIEWS_START DATE DEFAULT PT_FEATURES - 29;

-- TODO(djb): Steps to create inference data:
--  Create CANDIDATES table users with subreddit<>users views in L30 days
--  Create TARGET table with selected subs<>users
--  Create exploded ToS table for users in user<>subreddit table


WITH subreddit_per_user_count AS (
    SELECT
        tos.user_id
        , COUNT(DISTINCT subreddit_id) AS tos_sub_count
    FROM `reddit-employee-datasets.david_bermejo.pn_test_users_de_campaign_tos_30_pct_20230418` AS tos
    GROUP BY 1
)
, post_consumes_agg AS (
    SELECT
        user_id
        , SUM(num_post_consumes) AS num_post_consumes
        , SUM(num_post_consumes_home) AS num_post_consumes_home
        , SUM(num_post_consumes_community) AS num_post_consumes_community
        , SUM(num_post_consumes_post_detail) AS num_post_consumes_post_detail
        , SUM(IF(app_name = 'ios', num_post_consumes, 0)) AS num_post_consumes_ios
        , SUM(IF(app_name = 'android', num_post_consumes, 0)) AS num_post_consumes_android
        , SUM(num_post_consumes_nsfw) AS num_post_consumes_nsfw
        , SAFE_DIVIDE(SUM(num_post_consumes_nsfw), SUM(num_post_consumes)) AS pct_post_consumes_nsfw
        -- , SUM(num_post_consumes_sfw) AS num_post_consumes_sfw
    FROM `data-prod-165221.video.post_consumes_30d_agg`
    WHERE DATE(pt) = PT_FEATURES
    GROUP BY 1
)
, candidate_sub_users AS (
    SELECT
        -- Need to fill user_id where user_id is missing from new selection criteria
        COALESCE(f.user_id, act.user_id) AS user_id
        -- But for other ids, only keep the raw data (don't fill from training b/c that data has some dupes)
        , f.subreddit_name AS target_subreddit
        , f.subreddit_id AS target_subreddit_id

        , COALESCE(act.send, 0) AS send
        , COALESCE(act.receive, 0) AS receive
        , COALESCE(act.click, 0) AS click

        -- The subscribed column in the old test table was wrong (all 0)
        , f.* EXCEPT(pt, pt_window_start, user_id, subreddit_name, subreddit_id, subscribed)

    FROM (
        SELECT *
        FROM `reddit-employee-datasets.david_bermejo.pn_test_users_de_campaign_20230418`
    ) AS f
        -- TODO(djb): remove this join. For normal inference, we won't need to join on actual sends/clicks
        --  Only doing it for this query because we want to explore the users that the model ranks high, but didn't receive it
        LEFT JOIN `reddit-employee-datasets.david_bermejo.pn_training_data_test_20230428` AS act
            ON f.user_id = act.user_id
                AND f.subreddit_name = act.target_subreddit

    WHERE f.subreddit_id IS NOT NULL
        -- For inference, we don't need this clause.
        --   Only need to keep sends|receives for TRAINING
        -- AND act.target_subreddit IS NOT NULL
)
, user_actions_t7 AS (
    SELECT
      pne.user_id,
      COUNT(receive_endpoint_timestamp) user_receives_pn_t7,
      COUNT(click_endpoint_timestamp) user_clicks_pn_t7,
      COUNT(
        CASE
          WHEN notification_type='lifecycle_post_suggestions'
            THEN click_endpoint_timestamp
          ELSE NULL
        END
    ) user_clicks_trnd_t7
    FROM `data-prod-165221.channels.push_notification_events` AS pne
    INNER JOIN candidate_sub_users AS c
        ON pne.user_id = c.user_id
    WHERE
        DATE(pt) BETWEEN PT_WINDOW_START AND PT_FEATURES
        AND NOT REGEXP_CONTAINS(notification_type, "email")
        AND receive_endpoint_timestamp IS NOT NULL
  GROUP BY user_id
)
, subscribes AS (
    SELECT
        -- We need distinct in case a user subscribes multiple times to the same sub
        DISTINCT
        u.user_id,
        su.subreddit_id AS subreddit_id
    from data-prod-165221.ds_v2_postgres_tables.account_subscriptions AS s
        LEFT JOIN UNNEST(subscriptions) AS su

        INNER JOIN candidate_sub_users AS u
            ON s.user_id = u.user_id

    WHERE DATE(_PARTITIONTIME) = (CURRENT_DATE() - 2)
        AND DATE(subscribe_date) <= PT_FEATURES
)

-- Select final data
SELECT
    ct.user_id
    , ct.target_subreddit
    , ct.target_subreddit_id
    , ct.send
    , ct.receive
    , ct.click
    , IF(s.subreddit_id IS NOT NULL, 1, 0) subscribed
    , COALESCE(tsc.tos_sub_count, 0) AS tos_30_sub_count
    , COALESCE(tos.tos_30_pct, 0) AS tos_30_pct
    , COALESCE(sv.feature_value, 0) AS screen_view_count_14d
    , CASE
        WHEN cl.legacy_user_cohort = 'new' THEN 1
        WHEN cl.legacy_user_cohort = 'resurrected' THEN 2
        WHEN cl.legacy_user_cohort = 'casual' THEN 3
        WHEN cl.legacy_user_cohort IS NULL THEN 4  -- '_missing_' or 'dead'
        WHEN cl.legacy_user_cohort = 'core' THEN 5
        ELSE 0
    END AS legacy_user_cohort_ord
    , pna.* EXCEPT(user_id)
    , co.* EXCEPT(user_id)
    , ct.* EXCEPT(user_id, target_subreddit, target_subreddit_id, send, receive, click)

FROM candidate_sub_users AS ct
    -- Get count of subs in ToS
    LEFT JOIN subreddit_per_user_count AS tsc
        ON ct.user_id = tsc.user_id

    -- Recent PN activity
    LEFT JOIN user_actions_t7 AS pna
        ON ct.user_id = pna.user_id

    -- Get view counts (all subreddits)
    LEFT JOIN (
        SELECT entity_id, feature_value
        FROM `data-prod-165221.user_feature_platform.screen_views_count_over_14_days_v1`
        WHERE DATE(pt) = PT_FEATURES
    ) AS sv
        ON ct.user_id = sv.entity_id

    -- USER cohort, Legacy
    LEFT JOIN (
        SELECT user_id, legacy_user_cohort
        FROM `data-prod-165221.attributes_platform.user_rolling_legacy_user_cohorts`
        WHERE DATE(pt) = PT_FEATURES
    ) AS cl
        ON ct.user_id = cl.user_id

    -- USER consumes
    LEFT JOIN post_consumes_agg AS co
        ON ct.user_id = co.user_id

    -- Add ToS_pct for target subreddit
    LEFT JOIN `reddit-employee-datasets.david_bermejo.pn_test_users_de_campaign_tos_30_pct_20230418` AS tos
        ON ct.user_id = tos.user_id
            AND ct.target_subreddit_id = tos.subreddit_id

    -- Get flag for user subscribed/not subscribed to sub
    LEFT JOIN subscribes AS s
        ON ct.user_id = s.user_id
        AND ct.target_subreddit_id = s.subreddit_id

-- For Inference, there's no need for WHERE clause because we want to score ALL users, even those that didn't receive the PN
-- WHERE ct.receive = 1

-- Only order to check data, no need to spend time ordering for training or inference
-- ORDER BY tos_30_pct DESC, click DESC, tos_sub_count DESC
;
