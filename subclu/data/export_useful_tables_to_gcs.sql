-- Export some columns to GCS to make it easier to join some things that are common but may not have been
-- a part of original queries (or they change over time)


-- Ambassador subs
EXPORT DATA OPTIONS(
  uri='gs://i18n-subreddit-clustering/data/ambassador_subreddits/2021-08-18/*.parquet',
  format='PARQUET',
  overwrite=true
  ) AS
SELECT
  LOWER(subreddit_name) AS subreddit_name
  , * EXCEPT(subreddit_name)
FROM `reddit-employee-datasets.wacy_su.ambassador_subreddits`
;


-- Geo-Relevant subreddits (all, not just German ones)
EXPORT DATA OPTIONS(
  uri='gs://i18n-subreddit-clustering/data/geo_relevant_subreddits/2021-08-18/*.parquet',
  format='PARQUET',
  overwrite=true
  ) AS
SELECT
  *
FROM `reddit-employee-datasets.wacy_su.geo_relevant_subreddits_2021`
;
