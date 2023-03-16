-- Get subreddits where English is NOT the primary language by posts & comments
SELECT
  subreddit_id
  , subreddit_name
  , language_name
  , language_rank
  , language_percent
  , total_count
  , thing_type
FROM `reddit-employee-datasets.david_bermejo.subreddit_language_rank_20230306`
WHERE thing_type = 'posts_and_comments'
  AND language_rank = 1
  AND language_name NOT IN ('English', 'Unknown')
  AND total_count >= 7
ORDER BY total_count DESC
