

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
