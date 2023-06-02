-- Example query to get top users for PN campaign
-- This query can also be used to figure out which subreddit is the best fit for a user
--   that is subscribed to or visits multiple target-subreddits.
--   For example, if a user participates in r/place we can use this query to find
--     which subreddit a user is most likely to click on (and participate)

DECLARE TARGET_SUBREDDITS DEFAULT [
    'zelda', 'nintendo'
];

-- NOTE: the PN cache only includes users from 18 countries
DECLARE TARGET_COUNTRY_CODES DEFAULT [
    "DE"
    , "GB", "FR", "MX"
];

WITH ranked_users AS (
    -- Get the highest ranked subreddit for each user
    SELECT
        pn.* EXCEPT(top_users)
        , t.*
        -- Create rank per USER so we don't send more than one PN per user
        --   We pick the highest click probabiliy to get the best targeting
        , ROW_NUMBER() OVER (
                PARTITION BY t.user_id
                ORDER BY t.click_proba DESC
        ) rank_unique_user
    FROM `reddit-growth-prod.pn_targeting.pn_model_subreddit_user_click_v1` AS pn
        LEFT JOIN UNNEST(top_users) AS t

    WHERE pt = (
        -- This subquery always picks the latest partition for the model
        SELECT
            DATE(PARSE_TIMESTAMP("%Y%m%d", MAX(partition_id)))
        FROM `reddit-growth-prod.pn_targeting`.INFORMATION_SCHEMA.PARTITIONS
        WHERE table_name = "pn_model_subreddit_user_click_v1"
            AND COALESCE(partition_id, '__NULL__') NOT IN (
                '__NULL__', '__UNPARTITIONED__'
            )
    )

        AND user_geo_country_code IN UNNEST(TARGET_COUNTRY_CODES)
        AND target_subreddit IN UNNEST(TARGET_SUBREDDITS)

    QUALIFY rank_unique_user = 1
)


SELECT
    pt
    , target_subreddit_id
    , target_subreddit
    , user_id
    -- NOTE: country code is valis as of the `pt` date, but it can change over time
    , user_geo_country_code
    , click_proba
FROM ranked_users
ORDER BY click_proba DESC
;
