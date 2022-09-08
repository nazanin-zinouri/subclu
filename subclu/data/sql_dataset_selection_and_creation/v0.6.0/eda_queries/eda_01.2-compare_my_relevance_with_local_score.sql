-- DJB: compare relevance in prod v. my own score
--  Turns out the prod (ETL) doesn't exclude small countries and blows up the standardized scores
-- Check PR status with fix:
--   https://github.snooguts.net/reddit/dw-airflow/pull/1297

DECLARE SUBREDDITS_TO_CHECK DEFAULT [
    'indianfood'
    , 'cricket'
    , 'askreddit'
    , 'tinder'
];

WITH capped_score AS (
    SELECT
        subreddit_name
        , geo_country_code
        , ROUND(100 * users_percent_by_subreddit_l28, 1) AS sub_dau_perc_l28_djb
        , ROUND(users_percent_by_country_standardized, 2) AS perc_by_country_capped_sd
        , ROUND(100 * users_percent_by_country_l28, 3) AS perc_by_country_l28
    FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220901`
    WHERE 1=1
        -- AND users_percent_by_subreddit_l28 <= .2
        AND geo_country_code IN (
            'IN', 'GB', 'AU'
        )
        AND subreddit_name IN UNNEST(SUBREDDITS_TO_CHECK)
)
, uncapped_score AS (
    SELECT
        subreddit_name
        , geo_country_code
        , ROUND(100 * sub_dau_perc_l28, 1) AS sub_dau_perc_l28
        , ROUND(perc_by_country_sd, 2) AS perc_by_country_sd
    FROM `data-prod-165221.i18n.community_local_scores`
    WHERE DATE(pt) = "2022-09-06"
        AND geo_country_code IN (
            'IN', 'GB', 'AU'
        )
        AND subreddit_name IN UNNEST(SUBREDDITS_TO_CHECK)
)
, new_capped_score AS (
    SELECT
        subreddit_name
        , geo_country_code
        , ROUND(100 * sub_dau_perc_l28, 1) AS sub_dau_perc_l28
        , ROUND(perc_by_country_sd, 3) AS perc_by_country_sd
        , ROUND(100 * perc_by_country, 3) AS perc_by_country
    FROM `reddit-employee-datasets.rustem_saitgareev.community_local_scores_2`
    WHERE DATE(pt) = "2022-08-08"
        AND geo_country_code IN (
            'IN', 'GB', 'AU'
        )
        AND subreddit_name IN UNNEST(SUBREDDITS_TO_CHECK)
)

SELECT
    cs.subreddit_name
    , cs.geo_country_code

    , cs.perc_by_country_capped_sd AS perc_by_country_sd_djb
    , us.perc_by_country_sd AS perc_by_country_sd_prod
    , nc.perc_by_country_sd

    , cs.perc_by_country_l28
    , nc.perc_by_country

    , cs.sub_dau_perc_l28_djb
    , us.sub_dau_perc_l28 AS sub_dau_perc_l28_prod
    , nc.sub_dau_perc_l28

FROM capped_score AS cs
    LEFT JOIN uncapped_score AS us
        ON cs.subreddit_name = us.subreddit_name AND cs.geo_country_code = us.geo_country_code
    LEFT JOIN new_capped_score AS nc
        ON cs.subreddit_name = nc.subreddit_name AND cs.geo_country_code = nc.geo_country_code
ORDER BY cs.subreddit_name, perc_by_country_capped_sd DESC
;
