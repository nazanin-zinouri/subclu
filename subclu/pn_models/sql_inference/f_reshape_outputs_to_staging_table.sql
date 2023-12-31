-- E. Rank and reshape data for PN model into staging or prod
--   For now, use staging table before pushing to prod
-- ETA: 5 mins (slot: 11 hours)
DECLARE PT_TARGET DATE DEFAULT '2023-05-29';
DECLARE MODEL_NAME_ DEFAULT 'PN click subreddit-user';
DECLARE MODEL_VERSION_ DEFAULT 'v1.0.0 2023-05-29';

-- ==================
-- After table is created, we can delete a partition & update it
-- ===
DELETE
    `reddit-employee-datasets.david_bermejo.pn_model_output_20230510`
WHERE
    pt = PT_TARGET
;

-- Insert latest data
INSERT INTO `reddit-employee-datasets.david_bermejo.pn_model_output_20230510`
(

WITH
new_rank AS (
    SELECT
        *
        , ROW_NUMBER() OVER(
            PARTITION BY pt, target_subreddit, subscribed, user_geo_country_code
            ORDER BY click_proba DESC
        ) AS user_rank_by_sub_and_geo
    FROM `reddit-relevance.tmp.i18n_pn_predictions_raw`
    WHERE pt = PT_TARGET
        # AND target_subreddit IN ('askreddit', 'de', 'streetfighter', 'zelda')
    QUALIFY user_rank_by_sub_and_geo <= 1200000
)
, nested_reshape AS (
    SELECT
        pt
        , MODEL_NAME_ AS model_name
        , MODEL_VERSION_ AS model_version
        , target_subreddit_id
        , target_subreddit
        , subscribed
        , user_geo_country_code
        , ARRAY_AGG(
            STRUCT(
                user_id
                , click_proba
                , user_rank_by_sub_and_geo
            )
            ORDER BY user_rank_by_sub_and_geo
        ) AS top_users
    FROM new_rank

    GROUP BY pt
        , model_name
        , model_version
        , target_subreddit_id
        , target_subreddit
        , subscribed
        , user_geo_country_code
)

SELECT *
FROM nested_reshape
-- ORDER BY target_subreddit, user_geo_country_code, subscribed, user_geo_country_code
);  -- close INSERT/CREATE parens


-- Test CTEs
-- SELECT * FROM new_rank
-- ORDER BY target_subreddit, user_geo_country_code, subscribed, user_rank_by_sub_and_geo
-- ;
