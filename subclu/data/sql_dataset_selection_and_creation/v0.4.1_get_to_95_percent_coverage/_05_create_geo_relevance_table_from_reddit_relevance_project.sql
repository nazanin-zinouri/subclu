-- Get new relevance score from reddit-relevance tables
--  these tables are a by-product of moving the model to kubeflow
CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220329`
AS (
    SELECT *
    FROM `reddit-relevance.tmp.subclu_subreddit_geo_relevance_standardized_20220329`
)
