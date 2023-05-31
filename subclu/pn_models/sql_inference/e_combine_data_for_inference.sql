-- This query was run on a notebook for training
-- %%time
-- %%bigquery df_train_raw --project data-prod-165221

-- E. Select lables and input data to train models
--   v1 Start with training labels to skip inference table

DECLARE PT_FEATURES DEFAULT [
    DATE('2022-12-01')
    , DATE('2023-02-19')
    , DATE('2023-02-28')
    , DATE('2023-04-17')
    , DATE('2023-04-24')
    , DATE('2023-05-04')
    , DATE('2023-05-07')
    , DATE('2023-05-08')
    , DATE('2023-05-09')
    , DATE('2023-05-11')
];
-- Need explicit max & min for legacy user cohorts
DECLARE PT_FEATURES_MAX DEFAULT TIMESTAMP((SELECT MAX(dt) FROM UNNEST(PT_FEATURES) AS dt));
DECLARE PT_FEATURES_MIN DEFAULT TIMESTAMP((SELECT MIN(dt) FROM UNNEST(PT_FEATURES) AS dt));

WITH
train_labels AS (
    SELECT
        pt_send
        , (pt_send - 1) AS pt
        , user_id
        , target_subreddit
        , t.send
        , t.receive
        , t.click
        , t.pn_id
        , t.correlation_id
    FROM `reddit-employee-datasets.david_bermejo.pn_training_data_20230515` AS t
    WHERE (COALESCE(t.receive, 0) + COALESCE(t.click, 0)) >= 1
        AND (pt_send - 1) IN UNNEST(PT_FEATURES)

        -- TODO(djb): go back and fix dupes later: EXCLUDE campaigns that have duplicate data??
        -- AND pt_send NOT IN ('2023-03-01', '2023-05-10')
)

, subreddit_ft AS (
    -- Query A
    SELECT
        s.* EXCEPT(
            relevant_geo_country_codes, relevant_geo_country_code_count
            -- TODO(djb): try encoding rating & topics later or let model ecode them
            , over_18, curator_rating, curator_topic_v2
        )
    FROM `reddit-employee-datasets.david_bermejo.pn_ft_subreddits_20230525` AS s
    WHERE s.pt IN UNNEST(PT_FEATURES)

    -- For testing, we can limit to only the top subreddits
    -- ORDER BY users_l7 DESC
    -- LIMIT 1000
)
, user_subreddit_ft AS (
    -- Query C
    SELECT
        us.* EXCEPT(pt_window_start)
        -- For inference, keep raw country

    FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230529` AS us

    WHERE us.pt IN UNNEST(PT_FEATURES)
        -- TODO(djb): for Inference Only keep users from target geos
        -- AND COALESCE(us.user_geo_country_code, "") IN UNNEST(TARGET_COUNTRY_CODES)
)
, user_ft AS (
    -- Query B
    -- TODO(djb): append cohorts to user-level table from the start!
    SELECT
        CASE
            WHEN cl.legacy_user_cohort = 'new' THEN 1
            WHEN cl.legacy_user_cohort = 'resurrected' THEN 2
            WHEN cl.legacy_user_cohort = 'casual' THEN 3
            WHEN cl.legacy_user_cohort IS NULL THEN 4  -- '_missing_' or 'dead'
            WHEN cl.legacy_user_cohort = 'core' THEN 5
            ELSE 0
        END AS legacy_user_cohort_ord
        , u.*
    FROM (
            SELECT *
            FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_20230529`
            WHERE pt IN UNNEST(PT_FEATURES)
        ) AS u
        LEFT JOIN (
            -- get USER cohort, Legacy
            SELECT user_id, legacy_user_cohort, pt
            FROM `data-prod-165221.attributes_platform.user_rolling_legacy_user_cohorts`
            WHERE pt BETWEEN PT_FEATURES_MIN AND PT_FEATURES_MIN
        ) AS cl
            ON u.user_id = cl.user_id AND cl.pt = TIMESTAMP(u.pt)
)
# , final_table AS (
#     SELECT
#         -- These index columns are needed for the final outputs (not inference)
#         COALESCE(u.pt, us.pt) AS pt
#         , us.target_subreddit_id
#         , us.target_subreddit
#         , COALESCE(u.user_id, us.user_id) AS user_id
#         , COALESCE(u.user_geo_country_code, us.user_geo_country_code) AS user_geo_country_code

#         -- The rest of the columns should be used for modeling inference
#         , u.* EXCEPT(pt, user_id, user_geo_country_code)
#         , us.* EXCEPT(pt, user_id, target_subreddit, target_subreddit_id, user_geo_country_code)

#         , s.* EXCEPT(subreddit_id, subreddit_name, pt)

#     -- TODO(djb): training: Start with User-level features, b/c we might need to fill nulls for MISSING target_subreddits
#     --   For inference, we might want to start with user<>subreddit features and only do inner join with subreddit features
#     FROM user_ft AS u
#         LEFT JOIN user_subreddit_ft AS us
#             ON us.user_id = u.user_id  AND us.pt = u.pt
#         -- Subreddit features. limit subreddits to score by using inner join
#         LEFT JOIN subreddit_ft AS s
#             ON us.target_subreddit_id = s.subreddit_id AND us.pt = s.pt
# )
, final_table_with_labels AS (
    SELECT
        -- Training labels
        t.pt_send
        , t.send
        , t.receive
        , t.click
        , t.pn_id
        , t.correlation_id

        -- These index columns are needed for the final outputs (not inference)
        , COALESCE(u.pt, us.pt) AS pt
        , us.target_subreddit_id
        , COALESCE(LOWER(t.target_subreddit), us.target_subreddit) AS target_subreddit
        , COALESCE(t.user_id, u.user_id, us.user_id) AS user_id
        , COALESCE(u.user_geo_country_code, us.user_geo_country_code) AS user_geo_country_code

        -- The rest of the columns should be used for modeling inference
        , u.* EXCEPT(pt, user_id, user_geo_country_code)
        , us.* EXCEPT(pt, user_id, target_subreddit, target_subreddit_id, user_geo_country_code)

        -- Exclude subreddit features that are raw counts (log features should be better)
        , s.users_log_l28
        , s.seo_users_pct_l28
        , s.loggedin_users_pct_l28
        , s.ios_users_pct_l28
        , s.android_users_pct_l28
#         , s.* EXCEPT(
#             subreddit_id, subreddit_name, pt
#             , votes_l7, votes_l28
#             , ios_users_l28, android_users_l28
#             , loggedin_users_l28, seo_users_l28
#             , comments_l7, comments_l28
#             , users_l7, users_l14, users_l28
#             , posts_l7, posts_l28
#         )
    FROM train_labels AS t
        LEFT JOIN user_ft AS u
            ON t.pt = u.pt AND t.user_id = u.user_id
        LEFT JOIN user_subreddit_ft AS us
            ON t.user_id = us.user_id
                AND t.pt = us.pt
                AND LOWER(t.target_subreddit) = us.target_subreddit
        -- Subreddit features. limit subreddits to score by using inner join
        LEFT JOIN subreddit_ft AS s
            ON LOWER(t.target_subreddit) = s.subreddit_name
                AND t.pt = s.pt
)


SELECT
    COALESCE(click, 0) AS click
    , COALESCE(subscribed, 0) AS subscribed
    , * EXCEPT(
        click, subscribed
        -- Other columns that have log transform OR percent
        , num_post_consumes_30, num_post_consumes_home_30
        , num_post_consumes_community_30, num_post_consumes_post_detail_30
        , num_post_consumes_nsfw_30
        , user_clicks_pn_t7
        , user_clicks_trnd_t7
        , screen_view_count_14d
        , us_trend_pn_receive_l14
        , us_screenviews_l14
        , us_post_screenviews_l14
        , us_distinct_posts_viewed_l14
    )
FROM final_table_with_labels
WHERE 1=1
    -- Drop users without recent activity because there's no data for model to learn from
    AND (
        click = 1
        AND user_receives_pn_t30 IS NOT NULL
        OR log_post_consumes_30 IS NOT NULL
    )
ORDER BY receive DESC, click DESC, num_post_consumes_30 DESC
;
