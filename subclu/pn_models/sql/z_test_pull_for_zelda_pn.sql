-- Example query to get top users for a PN campaign
--  In this query I pull the target subreddits from a pre-computed table instead of the subreddit array
-- DECLARE TARGET_SUBREDDITS DEFAULT [
--     "nintendoswitch", "breath_of_the_wild"
--     , "zelda", "tearsofthekingdom"
--     , "3ds", "nintendo", "botw", "switch"
-- ];

DECLARE TARGET_COUNTRY_CODES DEFAULT [
    "DE"
    , "GB", "FR", "MX", "IN"
];

WITH ranked_users AS (
    SELECT
        pn.* EXCEPT(target_subreddit_id, top_users)
        , t.*
        -- Create rank per USER so we don't send more than one PN per user
        , ROW_NUMBER() OVER (
                PARTITION BY t.user_id
                ORDER BY t.click_proba DESC
        ) rank_unique_user
    FROM `reddit-employee-datasets.david_bermejo.pn_model_output_20230510` AS pn
        LEFT JOIN UNNEST(top_users) AS t
    WHERE pt = "2023-05-07"

        AND user_geo_country_code IN UNNEST(TARGET_COUNTRY_CODES)
        -- AND target_subreddit IN UNNEST(TARGET_SUBREDDITS)
        AND target_subreddit IN (
            SELECT subreddit_name
            FROM `reddit-employee-datasets.david_bermejo.pn_zelda_target_subreddits`
            -- WHERE target_input = 1
        )
        -- AND t.user_rank_by_sub_and_geo <= 15

    QUALIFY rank_unique_user = 1

    ORDER BY t.click_proba DESC
    -- LIMIT 100
)


-- Get list of users
-- SELECT
--     user_id
--     , target_subreddit
--     , user_geo_country_code
--     , click_proba
--     , user_rank_by_sub_and_geo

-- FROM ranked_users
-- ;


-- Get agg count of users per country
-- SELECT
--     user_geo_country_code
--     , COUNT(DISTINCT user_id) AS user_count
--     -- , COUNT(*) AS row_count
-- FROM ranked_users
-- GROUP BY 1
-- ORDER BY user_count DESC
-- ;


-- Get agg count of users per subreddit
SELECT
    target_subreddit
    , COUNT(DISTINCT user_id) AS user_count
    -- , COUNT(*) AS row_count
FROM ranked_users
GROUP BY 1
ORDER BY user_count DESC
;

-- Get agg count of users per country+subreddit
-- SELECT
--     user_geo_country_code
--     , target_subreddit
--     , COUNT(DISTINCT user_id) AS user_count
--     -- , COUNT(*) AS row_count
-- FROM ranked_users
-- GROUP BY 1,2
-- ORDER BY user_geo_country_code, user_count DESC
-- ;
