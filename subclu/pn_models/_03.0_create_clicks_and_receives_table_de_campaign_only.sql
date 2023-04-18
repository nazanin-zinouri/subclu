-- Get labels for receives & clicks

-- Only look at click events 5 days afert send
DECLARE PT_WINDOW_START DATE DEFAULT '2022-12-02';
DECLARE PT_WINDOW_END DATE DEFAULT PT_WINDOW_START + 5;


-- ==================
-- Only need to create the first time we run it
-- ===
CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.pn_training_data_test`
-- CREATE TABLE IF NOT EXISTS `reddit-employee-datasets.david_bermejo.pn_training_data_test`
AS (

-- ==================
-- After table is created, we can delete a partition & update it
-- ===
-- DELETE
--     `reddit-employee-datasets.david_bermejo.top_bot_author_posts`
-- WHERE
--     pt = DT_END
-- ;

-- Insert latest data
-- INSERT INTO `reddit-employee-datasets.david_bermejo.pn_training_data_test`
-- (

WITH
send_long AS (
    SELECT
        a.correlation_id
        , notification_title
        , notification_type
        , a.user_id
        , app_name

    FROM `data-prod-165221.channels.pn_sends` a
        INNER JOIN  `reddit-growth-prod.generated_one_offs.20221201193812_elizabethpollard_de_de_der_topbeitrag_diese_woche_32358` b
        ON a.user_id = b.user_id
            AND a.notification_title = b.title
            AND a.notification_type = b.campaign_type
    WHERE 1=1
        AND DATE(a.pt) = PT_WINDOW_START
    GROUP BY 1,2,3,4,5
)
, send_wide AS (
SELECT
    correlation_id
    , user_id
    , notification_title
    , notification_type

    , 1 AS send
    , COUNT(correlation_id) AS send_count
    , COUNT(DISTINCT (CASE WHEN app_name = 'ios' THEN correlation_id
        ELSE NULL
    END
    )) AS send_ios
    , COUNT(DISTINCT (CASE WHEN app_name = 'android' THEN correlation_id
        ELSE NULL
    END
    )) AS send_android
    , COUNT(CASE WHEN app_name NOT IN ('android', 'ios') THEN correlation_id
        ELSE NULL
    END
    ) AS send_other

FROM send_long
GROUP BY 1,2,3,4
)
, receive_long as (
    SELECT
        a.correlation_id,
        a.user_id,
        a.app_name
    FROM `data-prod-165221.channels.pn_receives` AS a
        INNER JOIN send_wide AS b
        ON a.user_id = b.user_id
            AND a.correlation_id = b.correlation_id
    WHERE 1=1
        AND DATE(pt) between PT_WINDOW_START and PT_WINDOW_END

    GROUP BY 1,2,3
)
, receive_wide AS (
SELECT
    user_id
    , correlation_id

    , 1 AS receive
    , COUNT(correlation_id) AS receive_count

    , COUNT(DISTINCT (CASE WHEN app_name = 'ios' THEN correlation_id
        ELSE NULL
    END)) AS receive_ios
    , COUNT(DISTINCT (CASE WHEN app_name = 'android' THEN correlation_id
        ELSE NULL
    END)) AS receive_android
    , COUNT(CASE WHEN app_name NOT IN ('android', 'ios') THEN correlation_id
        ELSE NULL
    END
    ) AS receive_other

FROM receive_long
GROUP By 1,2
)
, click_long as (
    SELECT
        a.correlation_id,
        a.user_id,
        a.app_name
    FROM `data-prod-165221.channels.pn_clicks` AS a
        INNER JOIN send_wide AS b
        ON a.user_id = b.user_id
            AND a.correlation_id = b.correlation_id
    WHERE 1=1
        AND DATE(pt) between PT_WINDOW_START and PT_WINDOW_END

    GROUP BY 1,2,3
)
, click_wide AS (
SELECT
    user_id
    , correlation_id

    , 1 AS click
    , COUNT(correlation_id) AS click_count

    , COUNT(DISTINCT (CASE WHEN app_name = 'ios' THEN correlation_id
        ELSE NULL
    END)) AS click_ios
    , COUNT(DISTINCT (CASE WHEN app_name = 'android' THEN correlation_id
        ELSE NULL
    END)) AS click_android
    , COUNT(CASE WHEN app_name NOT IN ('android', 'ios') THEN correlation_id
        ELSE NULL
    END
    ) AS click_other
FROM click_long
GROUP BY 1,2
)
, all_data_wide AS (
SELECT
    s.correlation_id
    , s.user_id
    , s.notification_title
    , s.notification_type
    , s.send
    , r.receive
    , c.click

    , c.* EXCEPT(correlation_id, user_id, click)
    , r.* EXCEPT(correlation_id, user_id, receive)
    , s.* EXCEPT(correlation_id, user_id, notification_title, notification_type, send)

FROM send_wide AS s
    LEFT JOIN receive_wide AS r
        ON s.user_id = r.user_id AND s.correlation_id = r.correlation_id
    LEFT JOIN click_wide AS c
        ON s.user_id = c.user_id AND s.correlation_id = c.correlation_id
)


SELECT
    -- TODO(djb): need a dictionary/table to map each notification campaign to a target subreddit for model training
    'de' AS target_subreddit
    , *
FROM all_data_wide
);  -- Close create table parens


-- SELECT *
-- FROM send_wide
-- ORDER BY send_count DESC, user_id
-- ;

-- SELECT *
-- FROM receive_wide
-- ORDER BY receive_count DESC
-- ;

-- SELECT *
-- FROM click_wide
-- ORDER BY click_count DESC
-- ;

-- SELECT
--     app_name
--     , COUNT(DISTINCT correlation_id) click_count
-- FROM click_long
-- GROUP BY 1
-- ORDER BY click_count DESC
-- ;
