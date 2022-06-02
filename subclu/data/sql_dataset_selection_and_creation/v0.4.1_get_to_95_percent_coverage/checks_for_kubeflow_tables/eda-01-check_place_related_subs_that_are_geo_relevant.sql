-- Check subs that are about r/place AND geo-relevant
SELECT *
FROM `reddit-relevance.tmp.subclu_subreddits_for_modeling_20220406`
WHERE 1=1
    AND geo_relevant_countries IS NOT NULL
    AND subreddit_name LIKE "%place%"
LIMIT 1000
;
