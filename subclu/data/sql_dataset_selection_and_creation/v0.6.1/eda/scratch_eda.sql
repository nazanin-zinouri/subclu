-- Random queries
SELECT
  subreddit_seed_for_clusters
  , COUNT(DISTINCT subreddit_id) AS subreddit_count
  , ROUND(AVG(users_l7), 2) AS users_l7_avg
  , ROUND(AVG(posts_not_removed_l28), 2) AS posts_not_removed_l28_avg
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_for_modeling_20221107`
GROUP BY 1


-- Get size & latest status of all subreddits with 1+ post in L7 days
-- Use it to select subs to use for TSNE seed visualization in reddit maps
DECLARE partition_date DATE DEFAULT CURRENT_DATE() - 2;


SELECT
    slo.subreddit_id
    , asr.subreddit_name
    , DATE(slo.created_date) AS created_date
    , asr.users_l7
    , asr.posts_l7
    , slo.verdict
    , slo.is_spam
    , slo.is_deleted
    #, COALESCE(acs.subreddit_name, asr.subreddit_name) AS subreddit_name

FROM (
    SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
    WHERE DATE(pt) = partition_date
) AS asr
    -- LEFT JOIN (
    --     SELECT * FROM `data-prod-165221.ds_subreddit_whitelist_tables.active_subreddits`
    --     WHERE DATE(_PARTITIONTIME) = partition_date
    -- ) AS acs
    --     ON asr.subreddit_name = acs.subreddit_name

    LEFT JOIN (
        SELECT * FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
        WHERE dt = partition_date
    ) AS slo
        ON LOWER(asr.subreddit_name) = LOWER(slo.name)

WHERE 1=1
    -- Exclude subs that are quarantined, removed, deleted, or marked as spam
    AND COALESCE(slo.verdict, "") != 'admin-removed'
    AND COALESCE(slo.is_spam, FALSE) = FALSE
    AND COALESCE(slo.is_deleted, FALSE) = FALSE
    AND slo.deleted IS NULL

    -- Keep only subs above activity threshold
    AND users_l7 >= 100
    AND posts_l7 >= 1
ORDER BY users_l7 DESC, posts_l7 DESC
;
