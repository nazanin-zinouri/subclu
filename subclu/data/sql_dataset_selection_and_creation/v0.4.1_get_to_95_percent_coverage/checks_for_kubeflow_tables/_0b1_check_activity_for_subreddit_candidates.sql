-- Use this query to check activity thresholds for selecting globally active subreddits
--  i.e., subreddits to add to topic model that are not geo-relevant
SELECT
    COUNT(*) as row_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count
    , COUNT(DISTINCT subreddit_name) AS subreddit_name_count
    , MIN(activity_7_day) AS activity_7_day_min
    , AVG(activity_7_day) AS activity_7_day_avg
    , MIN(users_l7) AS users_l7_min
    , AVG(users_l7) AS users_l7_avg
    , MIN(posts_not_removed_l28) AS posts_not_removed_l28_min
    , AVG(posts_not_removed_l28) AS posts_not_removed_l28_avg

FROM `reddit-relevance.tmp.subclu_subreddit_candidates_20220323`

WHERE 1=1
    AND (
        (
            active=TRUE
            AND users_l7 >= 100
            AND posts_not_removed_l28 >= 10
        )
        OR (
            activity_7_day >= 9
            AND users_l7 >= 700
            AND posts_not_removed_l28 >= 16
            AND unique_posters_l7_submitted >= 2
        )
    )

-- ORDER BY activity_7_day DESC, users_l7 DESC, ssc.subreddit_id, ssc.subreddit_name
;
