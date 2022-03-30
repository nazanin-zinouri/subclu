-- Instead of replicating work, create a view in my personal dataset
--  so it's available to people who don't have access to reddit-relevance

CREATE VIEW `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220321` AS (
    SELECT *
    FROM `reddit-relevance.tmp.subclu_subreddit_geo_relevance_standardized_20220321`
)
