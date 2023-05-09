-- C. Loop through different dates to backfill user-level data
DECLARE DATES_LST array<date> default [
    -- Pick dates when we sent PNs we want to use for model training
    DATE("2023-05-02"),
    DATE("2022-11-30"),
    DATE("2022-12-01"),
    DATE("2022-12-13"),
    DATE("2022-12-14"),
    DATE("2022-12-16"),
    DATE("2022-12-17"),
    DATE("2022-12-19"),
    DATE("2023-01-15"),
    DATE("2023-02-19"),
    DATE("2023-02-24"),
    DATE("2023-02-28"),
    DATE("2023-03-02"),
    DATE("2023-03-04"),
    DATE("2023-03-26"),
    DATE("2023-04-03"),
    DATE("2023-04-17"),
    DATE("2023-04-20"),
    DATE("2023-04-21"),
    DATE("2023-04-23"),
    DATE("2023-04-24")
];
DECLARE table_name STRING default "`reddit-employee-datasets.david_bermejo.pn_ft_user_20230502`";

DECLARE parametrized_delete STRING default """
WHERE pt = @cur_date
""";


DECLARE parametrized_insert STRING default """
-- Create table with Consumes in L30 + PN activity in L7 days
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
        WHERE DATE(pt) = @cur_date
    ) AS sv
        FULL OUTER JOIN `data-prod-165221.video.post_consumes_30d_agg` AS pc
            ON pc.user_id = sv.entity_id
    WHERE DATE(pc.pt) = @cur_date
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
        DATE(pne.pt) BETWEEN (@cur_date - 7) AND @cur_date
        AND NOT REGEXP_CONTAINS(notification_type, "email")
        AND receive_endpoint_timestamp IS NOT NULL
  GROUP BY user_id
)

SELECT
    @cur_date AS pt
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
)
""";

-- This assumes we've already created the table and we're only inserting
FOR cur_date IN (select distinct cur_date from unnest(dates_lst) as cur_date order by cur_date) DO
    -- TODO(djb): add try/except logic to create table if it doesn't already exist
    execute immediate "DELETE " || table_name || " " || parametrized_delete
    USING cur_date.cur_date as cur_date;

    execute immediate "INSERT INTO " || table_name || " " || parametrized_insert
    USING cur_date.cur_date as cur_date;
END FOR;
