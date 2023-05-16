-- Get labels for receives & clicks

-- Only look at click events 5 days afert send
DECLARE PT_WINDOW_START DATE DEFAULT '2022-12-02';
DECLARE PT_WINDOW_END DATE DEFAULT PT_WINDOW_START + 5;

DECLARE RX_GET_SUBREDDIT_NAME STRING DEFAULT r"(?i)\br\/([a-zA-Z0-9]\w{2,30}\b)";

-- ==================
-- Only need to create the first time we run it
-- === OR REPLACE
CREATE TABLE `reddit-employee-datasets.david_bermejo.pn_training_data_test_20230428`
AS (

-- ==================
-- After table is created, we can delete a partition & update it
-- ===
-- DELETE
--     `reddit-employee-datasets.david_bermejo.pn_training_data_test_20230428`
-- WHERE
--     pt = DT_END
-- ;

-- Insert latest data
-- INSERT INTO `reddit-employee-datasets.david_bermejo.pn_training_data_test_20230428`
-- (

WITH
send_long AS (
    SELECT
        a.correlation_id
        , notification_title
        , notification_type
        , deeplink_uri
        , REGEXP_EXTRACT(deeplink_uri, RX_GET_SUBREDDIT_NAME, 1) AS target_subreddit
        , a.user_id
        , app_name

    FROM `data-prod-165221.channels.pn_sends` a
        INNER JOIN  `reddit-growth-prod.generated_one_offs.20221201193812_elizabethpollard_de_de_der_topbeitrag_diese_woche_32358` b
        ON a.user_id = b.user_id
            AND a.notification_title = b.title
            AND a.notification_type = b.campaign_type
    WHERE 1=1
        AND DATE(a.pt) = PT_WINDOW_START
    GROUP BY 1,2,3,4,5,6,7
)
, send_wide AS (
    SELECT
        correlation_id
        , user_id
        , notification_title
        , notification_type
        , target_subreddit
        , deeplink_uri

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
    GROUP BY 1,2,3,4,5,6
)
, receive_long as (
    SELECT
        a.correlation_id
        , a.user_id
        , a.app_name
        -- If ANY receive was supressed, count ALL as supressed
        , SUM(IF(a.is_suppressed = True, 1, 0)) AS supressed_count
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
        , SUM(supressed_count) AS suppressed_count

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

        , COUNT(
            DISTINCT(
                CASE WHEN app_name = 'ios' AND supressed_count >= 1 THEN correlation_id
                ELSE NULL
                END
            )
        ) AS suppressed_ios
        , COUNT(
            DISTINCT(
                CASE WHEN app_name = 'android' AND supressed_count >= 1 THEN correlation_id
                ELSE NULL
                END
            )
        ) AS suppressed_android

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
        -- Note that we're getting the target_subreddit from the deeplink URI
        s.correlation_id
        , s.user_id
        , s.target_subreddit
        , s.notification_title
        , s.notification_type
        , s.send
        , r.receive
        , c.click
        , CASE
            WHEN (
                r.receive = 1
                -- Keep receives when we have more receives in (android & iOS) than suppressed_receives
                AND (
                    (COALESCE(receive_ios, 0) + COALESCE(receive_android, 0)) >
                    (COALESCE(suppressed_ios, 0) + COALESCE(suppressed_android, 0))
                )
            ) THEN 1
            WHEN r.receive IS NOT NULL THEN 0
            ELSE NULL
        END AS receive_not_suppressed


        , c.* EXCEPT(correlation_id, user_id, click)
        , r.* EXCEPT(correlation_id, user_id, receive)
        , s.* EXCEPT(correlation_id, user_id, notification_title, notification_type, send, target_subreddit, deeplink_uri)

    FROM send_wide AS s
        LEFT JOIN receive_wide AS r
            ON s.user_id = r.user_id AND s.correlation_id = r.correlation_id
        LEFT JOIN click_wide AS c
            ON s.user_id = c.user_id AND s.correlation_id = c.correlation_id
)

-- Final select for TABLE
SELECT *
FROM all_data_wide
);  -- Close CREATE TABLE parens


-- ORDER BY click_count DESC, receive_count DESC



-- ============
-- Test CTEs
-- ===

-- ~~~~~~~~~~~~
-- Sends
-- ~~~
-- SELECT
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT user_id) AS user_id_count
-- FROM send_long;

-- SELECT *
-- FROM send_wide
-- ORDER BY send_count DESC, user_id
-- ;

-- SELECT
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT user_id) AS user_id_count
-- FROM send_wide;


-- ~~~~~~~~~~~~
-- Receives
-- ~~~
-- SELECT *
-- FROM receive_long
-- ORDER BY is_supressed_num DESC
-- ;

-- Long = Not unique users per row
-- SELECT
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT user_id) AS user_id_count
-- FROM receive_long;


-- SELECT *
-- FROM receive_wide
-- ORDER BY receive_count DESC
-- ;

-- Wide = Unique users per row
-- SELECT
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT user_id) AS user_id_count
-- FROM receive_wide;


-- ~~~~~~~~~~~~
-- Clicks
-- ~~~
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
