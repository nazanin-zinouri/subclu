-- This table was exported by the taxonomy team as a 1-time dump
SELECT
    override_rating_short
    , COUNT(*) AS row_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_count
FROM `reddit-employee-datasets.david_bermejo.subreddit_taxonomy_overrides`
GROUP BY 1
ORDER BY 2 DESC
;

SELECT
    override_topic
    , COUNT(*) AS row_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_count
FROM `reddit-employee-datasets.david_bermejo.subreddit_taxonomy_overrides`
GROUP BY 1
ORDER BY 2 DESC
;
