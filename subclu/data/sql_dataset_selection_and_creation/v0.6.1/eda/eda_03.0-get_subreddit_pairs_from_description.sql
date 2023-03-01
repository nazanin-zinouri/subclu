-- We could use these pairs to create a "gold" data set to find similar subreddits
--  This table pulls any subreddit mentioned in a subreddit's `description`
--  CAVEAT: sometimes a subreddit's own name gets mentioned and I didn't remove it

SELECT
  users_l7
  , subreddit_name
  , subreddits_in_descriptions
  , subreddit_title
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_for_modeling_20220811`
WHERE subreddits_in_descriptions IS NOT NULL
    AND subreddit_name != LOWER(subreddits_in_descriptions)
ORDER BY users_l7 DESC, subreddit_name
;
