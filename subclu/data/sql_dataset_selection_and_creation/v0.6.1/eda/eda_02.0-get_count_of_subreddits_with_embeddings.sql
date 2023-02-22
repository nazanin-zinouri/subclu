-- Get number of subreddits we've used for embeddings jobs

SELECT
  COUNT(DISTINCT COALESCE(o.subreddit_id, n.subreddit_id)) AS sub_full_count
  , COUNT(DISTINCT n.subreddit_id) AS subreddit_count_new
  , COUNT(DISTINCT o.subreddit_id) AS subreddit_count_old
  , SUM(
    CASE
      WHEN o.subreddit_id IS NULL THEN 1
      ELSE 0
    END
  ) AS sub_new_only_count
  , SUM(
    CASE
      WHEN n.subreddit_id IS NULL THEN 1
      ELSE 0
    END
  ) AS sub_old_only_count

FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_for_modeling_20221107` AS n
  FULL OUTER JOIN `reddit-employee-datasets.david_bermejo.subclu_subreddits_for_modeling_20220811` AS o
    ON n.subreddit_id = o.subreddit_id
;
