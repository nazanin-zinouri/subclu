-- Get distribution for # of users per country
-- Use these to get threshold of cut-offs for standard-deviation
--  in relevance score because Some small islands screw up standard-dev scores
-- Rough percentiles:
--  p25:        10k
--  p50:        67k
--  p75:       612k
--  p80:     1,369k (1.3 million)
--  AVG:     2,730k (2.7 million)
--  p90:     3,285k
SELECT
    -- geo_country_code
    COUNT(DISTINCT geo_country_code) AS country_count

    , MIN(total_users_in_country_l28) AS users_min
    , APPROX_QUANTILES(total_users_in_country_l28, 100)[OFFSET(10)] AS users_p10
    , APPROX_QUANTILES(total_users_in_country_l28, 100)[OFFSET(25)] AS users_p25
    , APPROX_QUANTILES(total_users_in_country_l28, 100)[OFFSET(50)] AS users_median
    , AVG(total_users_in_country_l28) AS users_avg
    , APPROX_QUANTILES(total_users_in_country_l28, 100)[OFFSET(75)] AS users_p75
    , APPROX_QUANTILES(total_users_in_country_l28, 100)[OFFSET(80)] AS users_p80
    , APPROX_QUANTILES(total_users_in_country_l28, 100)[OFFSET(90)] AS users_p90
    , APPROX_QUANTILES(total_users_in_country_l28, 100)[OFFSET(95)] AS users_p95
    , APPROX_QUANTILES(total_users_in_country_l28, 100)[OFFSET(99)] AS users_p99

FROM (
    SELECT DISTINCT
        geo_country_code
        , total_users_in_country_l28
    FROM `reddit-relevance.tmp.subclu_subreddit_geo_relevance_raw_20220526`
)


-- How many subreddits have 2 or 3+ posts?
-- IDEA: use subreddits with 4+ posts as clustering seeds
--  then use k-nn (or similar approach) to get labels for
--  the remaining ~80k subreddits that aren't part of the cluster core.
SELECT
    SUM(IF(posts_not_removed_l28 >= 2, 1, 0)) AS subreddits_2_plus_posts
    , SUM(IF(posts_not_removed_l28 >= 3, 1, 0)) AS subreddits_3_plus_posts
FROM `reddit-relevance.tmp.subclu_subreddit_candidates_20220526`

LIMIT 1000
