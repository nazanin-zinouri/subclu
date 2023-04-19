-- Get test users for end-to-end embeddings
-- TODO(djb): Filters
--   - remove users who only viewed one subreddit
--   - keep users with an iOS or Android event
DECLARE PT_DT DATE DEFAULT "2022-12-01";
-- Expand to 30 days total to get at least 1 month's worth of data given that in the prev model 1 month was the minimum
DECLARE PT_WINDOW_START DATE DEFAULT PT_DT - 29;

-- DECLARE TARGET_COUNTRIES DEFAULT [
--     "DE", "AT", "CH"
-- ];


-- OR REPLACE
CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.pn_test_users_de_campaign_20230418` AS (

WITH
selected_users AS (
SELECT
    COALESCE(act.user_id, v.user_id) AS user_id
    , v.subreddit_id
    , v.subreddit_name
    -- Testing column to debug missing users
    , IF(
        (act.user_id IS NOT NULL) AND (v.user_id IS NULL) ,
        1,
        0
    ) AS user_in_actual_but_missing_from_new

    , COUNT(DISTINCT post_id) AS view_and_consume_unique_count
    , COUNT(DISTINCT(IF(v.action='consume', post_id, NULL))) AS consume_unique_count
    , SUM(IF(v.action='view', 1, 0)) AS view_count
    , SUM(IF(v.action='consume', 1, 0)) AS consume_count
    , SUM(IF(v.action='consume' AND app_name='ios', 1, 0)) AS consume_ios_count
    , SUM(IF(v.action='consume' AND app_name='android', 1, 0)) AS consume_android_count
FROM (
    SELECT
        subreddit_id
        , subreddit_name
        , user_id
        , post_id
        , app_name
        , action
    FROM `data-prod-165221.fact_tables.post_consume_post_detail_view_events`
    WHERE DATE(pt) BETWEEN PT_WINDOW_START AND PT_DT
        AND user_id IS NOT NULL
        AND subreddit_name IN (
            -- 1st sub is a target from a campaign & following ones are most similar based on ft2 embeddings:
            'de', 'fragreddit', 'ich_iel'
        )
        AND action IN ('consume', 'view')
) AS v
    FULL OUTER JOIN (
        -- Add Actual SENT users in case they don't match my selecting criteria
        SELECT user_id
        FROM `reddit-growth-prod.generated_one_offs.20221201191705_elizabethpollard_de_de_der_topbeitrag_diese_woche_62384`
        UNION ALL
        SELECT user_id
        FROM `reddit-growth-prod.generated_one_offs.20221201193812_elizabethpollard_de_de_der_topbeitrag_diese_woche_32358`
    ) act
        ON v.user_id = act.user_id
GROUP BY 1,2,3,4
)
, subscribes AS (
-- Subscription tables expire after 90 days... (sigh)
--  So we'll have imperfect data for training models before 2023-02
-- Maybe we should just exclude it for historical campaigns and
--  Only add it going forward?
SELECT
    u.user_id,
    subscriptions.subreddit_id subreddit_id
from data-prod-165221.ds_v2_postgres_tables.account_subscriptions AS s,
    UNNEST(subscriptions) as subscriptions
    INNER JOIN selected_users AS u
        ON s.user_id = u.user_id
WHERE DATE(_PARTITIONTIME) = PT_DT
)

SELECT
    PT_DT AS pt
    , PT_WINDOW_START AS pt_window_start
    , v.subreddit_id
    , v.subreddit_name
    , v.user_id
    , g.geo_country_code
    , IF(s.subreddit_id IS NOT NULL, 1, 0) subscribed
    , v.* EXCEPT(subreddit_id, subreddit_name, user_id)

FROM selected_users AS v
    LEFT JOIN (
        SELECT
            user_id
            , geo_country_code
        FROM `data-prod-165221.channels.user_geo_6mo_lookback`
        WHERE
            DATE(pt) = PT_DT
            -- AND geo_country_code IN UNNEST(TARGET_COUNTRIES)
    ) AS g
        on v.user_id = g.user_id

    -- Get flag for user subscribed/not subscribed to sub
    LEFT JOIN subscribes AS s
        ON v.user_id = s.user_id
        AND v.subreddit_id = s.subreddit_id

    -- TODO(djb): join to get only users who have at least 1 ios or android event in ANY sub

WHERE 1=1
);  -- Close CREATE TABLE parens


SELECT
    COUNT(*) AS row_count
    , COUNT(DISTINCT user_id) AS user_id_count
FROM `reddit-employee-datasets.david_bermejo.pn_test_users_de_campaign_20230418`
;
