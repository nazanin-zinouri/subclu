-- Get relevant subreddits for a country
SELECT *
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220502`
WHERE 1=1
    AND (
        geo_relevance_default = TRUE
        OR relevance_percent_by_subreddit = TRUE
        OR users_percent_by_country_standardized >= 3.0
    )
    AND geo_country_code IN ('DE')

ORDER BY subreddit_rank_in_country ASC
;


-- Find relevant countries for specific subreddits
subclu_subreddit_relevance_beta_20220502
SELECT *
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220502`
WHERE subreddit_name IN (
    'soccer', 'cricket', 'fussball'
)
    AND (
        geo_relevance_default = TRUE
        OR relevance_percent_by_subreddit = TRUE
        -- OR relevance_percent_by_country_standardized = TRUE
        OR users_percent_by_subreddit_l28 >= 0.08
        OR users_percent_by_country_standardized >= 3.0
    )
ORDER BY subreddit_name, users_percent_by_subreddit_l28 DESC
LIMIT 1000
;
