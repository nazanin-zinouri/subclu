-- Sample query to get top users for PN campaign

DECLARE TARGET_SUBREDDITS DEFAULT [
    'fragreddit', 'ich_iel'
];

DECLARE TARGET_COUNTRY_CODES DEFAULT [
    "DE", "AT", "CH"
    , "MISSING", "ROW"
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
    FROM `reddit-employee-datasets.david_bermejo.pn_model_test` AS pn
        LEFT JOIN UNNEST(top_users) AS t
    WHERE pt = "2022-12-01"

        AND geo_country_code_top IN UNNEST(TARGET_COUNTRY_CODES)
        AND target_subreddit IN UNNEST(TARGET_SUBREDDITS)
        -- AND t.user_rank_by_sub_and_geo <= 15

    QUALIFY rank_unique_user = 1

    ORDER BY t.click_proba DESC
    LIMIT 100
)


SELECT
    user_id
    , target_subreddit
    , geo_country_code_top
    , click_proba
    -- , user_rank_by_sub_and_geo

FROM ranked_users
;


-- -- Check for dupes in this table
-- --   WE want to check & remove dupes for inference
-- SELECT
--     user_id
--     -- , subreddit_id
--     , subreddit_name

--     , COUNT(*) AS dupe_count
-- FROM `reddit-employee-datasets.david_bermejo.pn_test_users_de_campaign_20230418`

-- GROUP BY 1,2
-- HAVING dupe_count > 1

-- ORDER BY dupe_count DESC
-- ;
