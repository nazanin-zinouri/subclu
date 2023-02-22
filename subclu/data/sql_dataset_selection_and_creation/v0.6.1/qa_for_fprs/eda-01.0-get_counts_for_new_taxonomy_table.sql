-- This tale is a 1-time data dump
--  Last known refresh was on 10/21/22
SELECT
  COUNT(*) AS row_count
  , COUNT(DISTINCT subreddit_id) AS subreddit_count
  , COUNT(curator_rating) AS curator_rating_count
  , COUNT(curator_topic) AS curator_topic_count
  , COUNT(curator_topic_v2) AS curator_topic_v2_count
FROM `data-prod-165221.taxonomy.daily_export`
WHERE 1=1
  -- AND subreddit_id = 't5_31khza'
