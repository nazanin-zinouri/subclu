-- Get test users for end-to-end embeddings
-- TODO(djb): Filters
--   - remove users who only viewed one subreddit
--   - keep users with an iOS or Android event
DECLARE PT_DT DATE DEFAULT "2022-12-01";
DECLARE PT_WINDOW_START DATE DEFAULT PT_DT - 13;

DECLARE TARGET_COUNTRIES DEFAULT [
    "DE", "AT", "CH"
];


CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.pn_test_users_de_campaign_20230417` AS (
SELECT
    PT_DT AS pt
    , PT_WINDOW_START AS pt_window_start
    , v.subreddit_id
    , v.subreddit_name
    , v.user_id
    , g.geo_country_code

    , COUNT(DISTINCT post_id) AS view_and_consume_unique_count
    , COUNT(DISTINCT(IF(v.action='consume', post_id, NULL))) AS consume_unique_count
    , SUM(IF(v.action='view', 1, 0)) AS view_count
    , SUM(IF(v.action='consume', 1, 0)) AS consume_count
    , SUM(IF(v.action='consume' AND app_name='ios', 1, 0)) AS consume_ios_count
    , SUM(IF(v.action='consume' AND app_name='android', 1, 0)) AS consume_android_count

FROM (
    SELECT
        user_id
        , geo_country_code
    FROM `data-prod-165221.channels.user_geo_6mo_lookback`
    WHERE
        DATE(pt) = PT_DT
        AND geo_country_code IN UNNEST(TARGET_COUNTRIES)
) AS g
    INNER JOIN `data-prod-165221.fact_tables.post_consume_post_detail_view_events` AS v
        on g.user_id = v.user_id

    -- TODO(djb): join to get only users who have at least 1 ios or android event in ANY sub
WHERE DATE(v.pt) BETWEEN PT_WINDOW_START AND PT_DT
    AND v.user_id IS NOT NULL
    AND v.subreddit_name IN (
        -- 1st sub is a target from a campaign & following ones are most similar based on ft2 embeddings:
        , 'de', 'fragreddit', 'ich_iel'
    )
    AND action IN ('consume', 'view')
GROUP BY 1,2,3,4,5,6
);


SELECT
    COUNT(*) AS row_count
    , COUNT(DISTINCT user_id) AS user_id_count
FROM `reddit-employee-datasets.david_bermejo.pn_test_users_for_embedding_20230412`
;
