-- Use this query to pick top subs to explore for post-level score thresholds
SELECT
    subreddit_id
    , geo_country_code
    , subreddit_name
    , ROW_NUMBER() OVER(PARTITION BY geo_country_code ORDER BY sub_dau_l28 DESC) AS country_rank
    , * EXCEPT(
        pt, subreddit_id, geo_country_code, subreddit_name
        , sub_dau_l1, sub_dau_perc_l1
        , is_removed, is_spam
    )

FROM `data-prod-165221.i18n.community_local_scores`
WHERE DATE(pt) = '2022-11-10'
    AND geo_country_code IN (
        'DE', 'FR', 'MX', 'BR'
        , 'CA', 'GB', 'IN', 'IE'
    )
    AND COALESCE(nsfw, FALSE) = FALSE
QUALIFY country_rank <= 50
ORDER BY geo_country_code, country_rank
;
