-- Use this query to make sure we have permission to a relevance table

CREATE OR REPLACE TABLE `reddit-relevance.tmp.subclu_test_${run_id}`
AS (
SELECT
    *
    , ${end_date}   AS end_date
FROM `reddit-relevance.ads_subreddit_semantic_emb.post_comment_info`

LIMIT 1000
)
;
