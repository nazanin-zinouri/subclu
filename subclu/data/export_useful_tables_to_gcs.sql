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


-- DEPRECATED | Geo-Relevant subreddits (all, not just German ones)
EXPORT DATA OPTIONS(
  uri='gs://i18n-subreddit-clustering/data/geo_relevant_subreddits/2021-08-18/*.parquet',
  format='PARQUET',
  overwrite=true
  ) AS
SELECT
  *
FROM `reddit-employee-datasets.wacy_su.geo_relevant_subreddits_2021`
;


-- This is the geo-score table that gets updated daily
--   Filters: active subreddits AND SFW
-- Note that a sub can change over time:
-- * a sub can be linked to 2 countries if they each get 40% of screen views
-- * if/when there are spikes and declines of activity from one country
-- * more often for small subreddits
EXPORT DATA OPTIONS(
  uri='gs://i18n-subreddit-clustering/data/geo_relevant_subreddits/2021-09-01/*.parquet',
  format='PARQUET',
  overwrite=true
  ) AS
SELECT
  *
FROM `data-prod-165221.i18n.geo_sfw_communities` AS geo
WHERE DATE(pt) BETWEEN (CURRENT_DATE() - 28) AND (CURRENT_DATE() - 2)
;
