-- Random queries
SELECT
  subreddit_seed_for_clusters
  , COUNT(DISTINCT subreddit_id) AS subreddit_count
  , ROUND(AVG(users_l7), 2) AS users_l7_avg
  , ROUND(AVG(posts_not_removed_l28), 2) AS posts_not_removed_l28_avg
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_for_modeling_20221107`
GROUP BY 1
