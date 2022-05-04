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
