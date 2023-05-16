-- Create table for candidate users for zelda PN
DECLARE PT_MODEL DATE DEFAULT "2023-05-07";

DECLARE TARGET_SUBREDDITS DEFAULT [
    "nintendoswitch", "breath_of_the_wild"
    , "zelda", "tearsofthekingdom"
    , "3ds", "nintendo", "botw", "switch"
];

DECLARE TARGET_COUNTRY_CODES DEFAULT [
    "DE"
    , "GB", "FR", "MX", "IN"
];


CREATE TABLE `reddit-employee-datasets.david_bermejo.pn_zelda_target_users_20230511`
AS (

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
        AND target_subreddit IN UNNEST(TARGET_SUBREDDITS)

    -- Keep only one row per user (with the highest probability)
    QUALIFY rank_unique_user = 1

    ORDER BY t.click_proba DESC
)


-- Get list of users
SELECT
    pt
    , user_id
    , target_subreddit
    , user_geo_country_code
    , click_proba
    , user_rank_by_sub_and_geo

FROM ranked_users
);
