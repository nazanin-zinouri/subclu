-- C. Create table with Consumes in L30 + PN activity in L7 days
DECLARE PT_FEATURES DATE DEFAULT CURRENT_DATE() - 2;
DECLARE PT_PN_WINDOW_START DATE DEFAULT PT_FEATURES - 7;
-- DECLARE PT_CONSUMES_START DATE DEFAULT PT_FEATURES - 2;  -- 29


-- ==================
-- Only need to create the first time we run it
-- === OR REPLACE
-- CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.pn_ft_user_20230502`
-- PARTITION BY pt
-- AS (

-- ==================
-- After table is created, we can delete a partition & update it
-- ===
DELETE
    `reddit-employee-datasets.david_bermejo.pn_ft_user_20230502`
WHERE
    pt = PT_FEATURES
;

-- Insert latest data
INSERT INTO `reddit-employee-datasets.david_bermejo.pn_ft_user_20230502`
(

WITH post_consumes_agg AS (
    SELECT
        COALESCE(sv.entity_id, pc.user_id) AS user_id
        , COALESCE(sv.feature_value, 0) AS screen_view_count_14d
        , SUM(num_post_consumes) AS num_post_consumes
        , SUM(num_post_consumes_home) AS num_post_consumes_home
        , SUM(num_post_consumes_community) AS num_post_consumes_community
        , SUM(num_post_consumes_post_detail) AS num_post_consumes_post_detail
        , SUM(IF(app_name = 'ios', num_post_consumes, 0)) AS num_post_consumes_ios
        , SUM(IF(app_name = 'android', num_post_consumes, 0)) AS num_post_consumes_android
        , SUM(num_post_consumes_nsfw) AS num_post_consumes_nsfw

    FROM (
        -- Get view counts (all subreddits)
        SELECT entity_id, feature_value
        FROM `data-prod-165221.user_feature_platform.screen_views_count_over_14_days_v1`
        WHERE DATE(pt) = PT_FEATURES
    ) AS sv
        FULL OUTER JOIN `data-prod-165221.video.post_consumes_30d_agg` AS pc
            ON pc.user_id = sv.entity_id
    WHERE DATE(pc.pt) = PT_FEATURES
    GROUP BY 1,2
)
, user_actions_t7 AS (
    SELECT
        pne.user_id
        , COALESCE(COUNT(receive_endpoint_timestamp), 0) user_receives_pn_t7
        , COALESCE(COUNT(click_endpoint_timestamp), 0) user_clicks_pn_t7
        , COALESCE(COUNT(
            CASE
              WHEN notification_type='lifecycle_post_suggestions' THEN click_endpoint_timestamp
              ELSE NULL
            END
        ), 0) AS user_clicks_trnd_t7
    FROM post_consumes_agg  AS c
      LEFT JOIN `data-prod-165221.channels.push_notification_events` AS pne
          ON pne.user_id = c.user_id
    WHERE
        DATE(pne.pt) BETWEEN PT_PN_WINDOW_START AND PT_FEATURES
        AND NOT REGEXP_CONTAINS(notification_type, "email")
        AND receive_endpoint_timestamp IS NOT NULL
  GROUP BY user_id
)

SELECT
    PT_FEATURES AS pt
    , pc.user_id
    , ua.* EXCEPT(user_id)
    , pc.num_post_consumes
    , SAFE_DIVIDE(num_post_consumes_home, num_post_consumes) AS pct_post_consumes_home
    , SAFE_DIVIDE(num_post_consumes_community, num_post_consumes) AS pct_post_consumes_community
    , SAFE_DIVIDE(num_post_consumes_post_detail, num_post_consumes) AS pct_post_consumes_post_detail
    , SAFE_DIVIDE(num_post_consumes_ios, num_post_consumes) AS pct_post_consumes_ios
    , SAFE_DIVIDE(num_post_consumes_android, num_post_consumes) AS pct_post_consumes_android
    , SAFE_DIVIDE(num_post_consumes_nsfw, num_post_consumes) AS pct_post_consumes_nsfw
    , pc.* EXCEPT(user_id, num_post_consumes)
FROM post_consumes_agg AS pc
    LEFT JOIN user_actions_t7 AS ua
        ON pc.user_id = ua.user_id
); -- Close CREATE parens
