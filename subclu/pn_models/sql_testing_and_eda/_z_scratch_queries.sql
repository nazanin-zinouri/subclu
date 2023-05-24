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


