
DECLARE SUBREDDITS_TO_CHECK DEFAULT [
    'indianfood'
    , 'cricket'
    , 'askreddit'
    , 'tinder'
    , 'afl'
];

DECLARE PT_SCORE DATE DEFAULT "2022-10-24";

WITH capped_score AS (
    SELECT
        subreddit_name
        , geo_country_code
        , ROUND(100 * users_percent_by_subreddit_l28, 1) AS sub_dau_perc_l28_djb
        , ROUND(users_percent_by_country_standardized, 2) AS perc_by_country_capped_sd
        , ROUND(100 * users_percent_by_country_l28, 3) AS perc_by_country_l28
    FROM `reddit-relevance.tmp.subclu_subreddit_geo_relevance_standardized_20221101_1650`
    WHERE 1=1
        -- AND users_percent_by_subreddit_l28 <= .2
        AND geo_country_code IN (
            'IN', 'GB', 'AU'
        )
        AND subreddit_name IN UNNEST(SUBREDDITS_TO_CHECK)
)
, etl_score AS (
    SELECT
        subreddit_name
        , geo_country_code
        , ROUND(100 * sub_dau_perc_l28, 1) AS sub_dau_perc_l28
        , ROUND(100 * perc_by_country, 1) AS perc_by_country
        , ROUND(perc_by_country_sd, 2) AS perc_by_country_sd
    FROM `data-prod-165221.i18n.community_local_scores`
    WHERE DATE(pt) = PT_SCORE
        AND geo_country_code IN (
            'IN', 'GB', 'AU'
        )
        AND subreddit_name IN UNNEST(SUBREDDITS_TO_CHECK)
)


SELECT
    cs.subreddit_name
    , cs.geo_country_code

    , cs.perc_by_country_capped_sd AS perc_by_country_sd_djb
    , us.perc_by_country_sd


    , cs.perc_by_country_l28 AS perc_by_country_l28_djb
    , us.perc_by_country

    , cs.sub_dau_perc_l28_djb
    , us.sub_dau_perc_l28

FROM capped_score AS cs
    LEFT JOIN etl_score AS us
        ON cs.subreddit_name = us.subreddit_name AND cs.geo_country_code = us.geo_country_code

ORDER BY cs.subreddit_name, perc_by_country_capped_sd DESC
;
