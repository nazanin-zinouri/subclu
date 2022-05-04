-- For input subreddit, get top languages
--  Each subreddit+language is 1 row
--  Limit to rank=1 to get primary/top language
SELECT *
FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_language_rank`
WHERE 1=1
  AND language_rank <= 3
  AND subreddit_name IN ('greece')
ORDER BY subreddit_name, thing_type, language_rank
;


-- Get subreddits where top language is not English
--  Or a specific language
SELECT *
FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_language_rank`
WHERE 1=1
  AND language_rank = 1
  AND language_name != 'English'
  -- AND language_name = 'Japanese'
  AND thing_type = 'post'
ORDER BY total_count DESC, subreddit_name, thing_type, language_rank
;
