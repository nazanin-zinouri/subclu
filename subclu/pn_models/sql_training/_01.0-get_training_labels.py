"""
To get training labels, we use a colab notebook.
These are example queries and this is a colab notebook where we actually execue them.

First run:
* https://colab.research.google.com/drive/1cOHiRJoFSFnoWEXEaLJy6b-uwxfA1l8P#scrollTo=BgusX1_JifL-

Update on 05/25
* https://colab.research.google.com/drive/1wNHrd1MYRHk0xoM0mqsvfEY-teQ4hUrh#scrollTo=IuIvaOhDhFUM
"""
from datetime import datetime
import string

from tqdm import tqdm


train_data_table = 'reddit-employee-datasets.david_bermejo.pn_training_data_20230515'

SQL_DEFINE_VARS = r"""
-- Get labels for receives & clicks

-- Only look at click events 5 days afert send
DECLARE PT_WINDOW_START DATE DEFAULT DATE("${date_sent_utc}");
DECLARE PT_WINDOW_END DATE DEFAULT PT_WINDOW_START + 5;

DECLARE RX_GET_SUBREDDIT_NAME STRING DEFAULT r"(?i)\br\/([a-zA-Z0-9]\w{2,30}\b)";
"""

SQL_CREATE_TABLE = r"""
CREATE TABLE `${train_data_table}`
CLUSTER BY pn_id
AS (
"""

SQL_INSERT_INTO_TABLE = r"""
-- Delete data we're trying to re-insert
-- Should be faster with new pn_id & clustering
--   Note that in the custom tables the column is `title`, but in `pn_sends` it is `notification_title`
DELETE
    `${train_data_table}`
WHERE
    pn_id = (
        SELECT DISTINCT
        CONCAT(
            CAST(PT_WINDOW_START AS STRING)
            , "-"
            , title
            , "-"
            , deeplink_uri
        ) AS pn_id
        FROM `${full_table_name}`
    )
;

-- Insert latest data
INSERT INTO `${train_data_table}`
(
"""

SQL_SELECT_DATA = r"""
WITH
send_long AS (
    SELECT DISTINCT
        a.correlation_id
        , notification_title
        , notification_type
        , deeplink_uri
        , REGEXP_EXTRACT(deeplink_uri, RX_GET_SUBREDDIT_NAME, 1) AS target_subreddit
        , a.user_id
        , a.app_name
        , b.device_id

    FROM `data-prod-165221.channels.pn_sends` a
        INNER JOIN  `${full_table_name}` b
        ON a.user_id = b.user_id
            AND a.notification_title = b.title
            AND a.notification_type = b.campaign_type
    WHERE 1=1
        AND DATE(a.pt) = PT_WINDOW_START
    -- GROUP BY 1,2,3,4,5,6,7
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
        # , ARRAY_CONCAT_AGG(DISTINCT device_id) AS device_ids
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
        , s.* EXCEPT(correlation_id, user_id, notification_title, notification_type, send, target_subreddit)

    FROM send_wide AS s
        LEFT JOIN receive_wide AS r
            ON s.user_id = r.user_id AND s.correlation_id = r.correlation_id
        LEFT JOIN click_wide AS c
            ON s.user_id = c.user_id AND s.correlation_id = c.correlation_id
)

-- Final select for TABLE
SELECT 
    PT_WINDOW_START AS pt_send
    -- Create new campaign-id column so it's easier to find & delete campaigns
    , CONCAT(
        CAST(PT_WINDOW_START AS STRING)
        , "-"
        , notification_title
        , "-"
        , deeplink_uri
    ) AS pn_id
    , *
FROM all_data_wide
);  -- Close CREATE TABLE parens
"""

#  replace escape character b/c we sometimes need to use it with
#   regex or in JSON_EXTRACT() function
SQL_FULL_CREATE = (
    SQL_DEFINE_VARS + SQL_CREATE_TABLE + SQL_SELECT_DATA
    .replace("$.", "$$.")
    .replace("$|", "$$|")
    .replace('$"', '$$"')
)

SQL_FULL_INSERT = (
    SQL_DEFINE_VARS + SQL_INSERT_INTO_TABLE + SQL_SELECT_DATA
    .replace("$.", "$$.")
    .replace("$|", "$$|")
    .replace('$"', '$$"')
)


# Now that we have a template, we can define & insert the data we need:
l_campaign_pt_and_table = [
    {
        'date_sent_utc': '2023-05-12',
        'full_table_name': 'reddit-employee-datasets.sahil_verma.totk_pn_ml_targeting_20230512'
    },
]

for d_table_ in tqdm(l_campaign_pt_and_table):
    try:
        # bigquery_client.get_table(train_data_table)
        print("Table {} already exists.".format(train_data_table))
        template = string.Template(SQL_FULL_INSERT)
    except Exception as e:
        print(f"Table {train_data_table} is NOT found.\n  {e}")
        template = string.Template(SQL_FULL_CREATE)

    sql = template.substitute(
        **d_table_,
        **{'train_data_table': train_data_table}
    )
    # if log_query:
    #     print(sql)

    print(f"Running query for params:...\n  {d_table_}")
    query_start_time = datetime.utcnow()
    print(f"  {query_start_time} | query START time")

    # query_job = bigquery_client.query(sql)
    # query_job.result()
    query_end_time = datetime.utcnow()
    print(f"  {query_end_time} | query END time")
    print(f"  {query_end_time - query_start_time} | query ELAPSED time")


EXAMPLE_QUERY_STR = r"""
-- Get labels for receives & clicks

-- Only look at click events 5 days afert send
DECLARE PT_WINDOW_START DATE DEFAULT DATE("2023-05-12");
DECLARE PT_WINDOW_END DATE DEFAULT PT_WINDOW_START + 5;

DECLARE RX_GET_SUBREDDIT_NAME STRING DEFAULT r"(?i)\br\/([a-zA-Z0-9]\w{2,30}\b)";

-- Delete data we're trying to re-insert
-- This delete takes too long! Even with new pn_id & clustering, delete is as long as inserting 
-- DELETE
--     `reddit-employee-datasets.david_bermejo.pn_training_data_20230515`
-- WHERE
--     pn_id = (
--         SELECT DISTINCT
--         CONCAT(
--             CAST(PT_WINDOW_START AS STRING)
--             , "-"
--             , notification_title
--             , "-"
--             , deeplink_uri
--         ) AS pn_id
--         FROM `reddit-employee-datasets.sahil_verma.totk_pn_ml_targeting_20230512`
--     )
-- ;

-- Insert latest data
INSERT INTO `reddit-employee-datasets.david_bermejo.pn_training_data_20230515`
(

WITH
send_long AS (
    SELECT DISTINCT
        a.correlation_id
        , notification_title
        , notification_type
        , deeplink_uri
        , REGEXP_EXTRACT(deeplink_uri, RX_GET_SUBREDDIT_NAME, 1) AS target_subreddit
        , a.user_id
        , a.app_name
        , b.device_id

    FROM `data-prod-165221.channels.pn_sends` a
        INNER JOIN  `reddit-employee-datasets.sahil_verma.totk_pn_ml_targeting_20230512` b
        ON a.user_id = b.user_id
            AND a.notification_title = b.title
            AND a.notification_type = b.campaign_type
    WHERE 1=1
        AND DATE(a.pt) = PT_WINDOW_START
    -- GROUP BY 1,2,3,4,5,6,7
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
        # , ARRAY_CONCAT_AGG(DISTINCT device_id) AS device_ids
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
        , s.* EXCEPT(correlation_id, user_id, notification_title, notification_type, send, target_subreddit)

    FROM send_wide AS s
        LEFT JOIN receive_wide AS r
            ON s.user_id = r.user_id AND s.correlation_id = r.correlation_id
        LEFT JOIN click_wide AS c
            ON s.user_id = c.user_id AND s.correlation_id = c.correlation_id
)

-- Final select for TABLE
SELECT 
    PT_WINDOW_START AS pt_send
    -- Create new campaign-id column so it's easier to find & delete campaigns
    , CONCAT(
        CAST(PT_WINDOW_START AS STRING)
        , "-"
        , notification_title
        , "-"
        , deeplink_uri
    ) AS pn_id
    , *
FROM all_data_wide
);  -- Close CREATE TABLE parens
"""
