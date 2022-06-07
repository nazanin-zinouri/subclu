

-- ==================
-- Check CTE Counts (Before creating table)
-- ===
-- Selected subreddit counts & percentiles
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

FROM selected_subs
;


-- Check slo clean text
SELECT
    COUNT(*) as row_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_id_unique_count
FROM subreddit_lookup_clean_text_meta
;


-- Check clean text columns
SELECT
    subreddit_id

    -- -- NOTE: For some reason bigquery shows a space before any string, but it doesn't mean it's part of the output
    , name
    , title
    -- , subreddits_in_descriptions
    , public_description
    , description

    , subreddit_name_title_and_clean_descriptions

FROM subreddit_lookup_clean_text_meta

WHERE 1=1
    -- AND description LIKE "%|%"
    AND LOWER(name) IN (
        -- non-English
        'de', 'mexico', 'france', 'turkey', 'ukraina', 'ja'
        -- r/ mentions
        , 'gardern', 'burgers', 'hawktalk', 'multihub', 'edcexchange'
        , 'uwaterloo', 'yojamba', 'kindness', 'hypotheticalsituation', 'ecigclassifieds'
        , 'motorcitykitties', 'minnesotatwins', 'mlb'
        -- tables and other markdown
        , 'clg', 'braves', 'sportingkc', 'atlantaunited', 'mma', 'barca'
        , 'askeurope', 'asoiaf', 'dogemarket', 'writingprompts', 'brewgearfs'
    )

ORDER BY LOWER(name)

LIMIT 1000
;


-- Check how many posts each subreddit has.
--  Use to help how many subs have 3+ posts in L28 days
-- Selected subreddit counts & percentiles
SELECT
    COUNT(*) as row_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_id_unique_count
    , COUNT(DISTINCT subreddit_name) AS subreddit_name_unique_count
    , SUM(
        IF(posts_not_removed_l28 >= 1, 1, 0)
    ) AS subreddits_1_plus_posts
    , SUM(
        IF(posts_not_removed_l28 >= 2, 1, 0)
    ) AS subreddits_2_plus_posts
    , SUM(
        IF(posts_not_removed_l28 >= 3, 1, 0)
    ) AS subreddits_3_plus_posts
    , SUM(
        IF(posts_not_removed_l28 >= 4, 1, 0)
    ) AS subreddits_4_plus_posts
    , SUM(
        IF(posts_not_removed_l28 >= 5, 1, 0)
    ) AS subreddits_5_plus_posts

    , MIN(posts_not_removed_l28) AS posts_not_removed_l28_min
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(25)] AS posts_not_removed_l28_p25
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(30)] AS posts_not_removed_l28_p30
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(35)] AS posts_not_removed_l28_p35
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(40)] AS posts_not_removed_l28_p40
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(50)] AS posts_not_removed_l28_median
    , AVG(posts_not_removed_l28) AS posts_not_removed_l28_avg
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(95)] AS posts_not_removed_l28_p95
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(99)] AS posts_not_removed_l28_p99

FROM `reddit-relevance.tmp.subclu_subreddit_candidates_20220603`
;
