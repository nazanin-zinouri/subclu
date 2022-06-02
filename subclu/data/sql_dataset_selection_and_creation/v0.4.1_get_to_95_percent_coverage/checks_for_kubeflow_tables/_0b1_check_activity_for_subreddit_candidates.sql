-- Use this query to check activity thresholds for selecting globally active subreddits
--  i.e., subreddits to add to topic model that are not geo-relevant
-- Expected: ~ 53k subreddits to meet threshold below

SELECT
    COUNT(*) as row_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_id_unique_count
    , COUNT(DISTINCT subreddit_name) AS subreddit_name_unique_count
    , SUM(
        CASE WHEN (active = TRUE) THEN 1
        ELSE 0
        END
    ) AS active_subreddit_count

    , MIN(activity_7_day) AS activity_7_day_min
    , APPROX_QUANTILES(activity_7_day, 100)[OFFSET(50)] AS activity_7_day_median
    , AVG(activity_7_day) AS activity_7_day_avg
    , APPROX_QUANTILES(activity_7_day, 100)[OFFSET(95)] AS activity_7_day_p95

    , MIN(users_l7) AS users_l7_min
    , APPROX_QUANTILES(users_l7, 100)[OFFSET(50)] AS users_l7_median
    , AVG(users_l7) AS users_l7_avg
    , APPROX_QUANTILES(users_l7, 100)[OFFSET(95)] AS users_l7_p95

    , MIN(posts_not_removed_l28) AS posts_not_removed_l28_min
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(50)] AS posts_not_removed_l28_median
    , AVG(posts_not_removed_l28) AS posts_not_removed_l28_avg
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(95)] AS posts_not_removed_l28_p95

FROM `reddit-relevance.tmp.subclu_subreddit_candidates_20220401`

WHERE 1=1
    AND (
        (
            active = TRUE
            AND users_l7 >= 100
            AND posts_not_removed_l28 >= 4
        )
        OR (
            activity_7_day >= 9
            AND users_l7 >= 500
            AND posts_not_removed_l28 >= 12
            AND unique_posters_l7_submitted >= 2
        )
    )

-- ORDER BY activity_7_day DESC, users_l7 DESC, ssc.subreddit_id, ssc.subreddit_name
;
