-- Check len of POST+COMMENT text for embedding
SELECT
    COUNT(*) as row_count
    , COUNT(DISTINCT comment_id) AS comment_unique_count
    , COUNT(DISTINCT post_id) AS post_unique_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_id_unique_count
    , COUNT(DISTINCT subreddit_name) AS subreddit_name_unique_count

    , MIN(comment_text_clean_len) AS comment_len_min
    , APPROX_QUANTILES(comment_text_clean_len, 100)[OFFSET(1)] AS comment_len_p01
    , APPROX_QUANTILES(comment_text_clean_len, 100)[OFFSET(5)] AS comment_len_p05
    , APPROX_QUANTILES(comment_text_clean_len, 100)[OFFSET(10)] AS comment_len_p10
    , APPROX_QUANTILES(comment_text_clean_len, 100)[OFFSET(25)] AS comment_len_p25
    , APPROX_QUANTILES(comment_text_clean_len, 100)[OFFSET(50)] AS comment_len_median
    , ROUND(AVG(comment_text_clean_len), 2)     AS comment_len_avg
    , APPROX_QUANTILES(comment_text_clean_len, 100)[OFFSET(75)] AS comment_len_p75
    , APPROX_QUANTILES(comment_text_clean_len, 100)[OFFSET(85)] AS comment_len_p85
    , APPROX_QUANTILES(comment_text_clean_len, 100)[OFFSET(90)] AS comment_len_p90
    , APPROX_QUANTILES(comment_text_clean_len, 100)[OFFSET(95)] AS comment_len_p95
    , APPROX_QUANTILES(comment_text_clean_len, 100)[OFFSET(99)] AS comment_len_p99
    , MAX(comment_text_clean_len)               AS comment_len_max

FROM `reddit-relevance.tmp.subclu_comments_for_modeling_20220628`
;


-- check specific posts + comments
-- check post+comments to excude b/c they're too short
SELECT
    *
FROM `reddit-relevance.tmp.subclu_post_and_comment_text_combined_20220628`

WHERE 1=1
    -- AND subreddit_name LIKE "%irl"
    -- AND post_and_comment_text_clean_len = 50
    -- AND post_and_comment_text_clean LIKE "%M4F%"
    AND subreddit_name IN (
        'bollywood', 'bollyarm'
    )
-- check similar posts by length
-- ORDER BY post_and_comment_text_clean_len, post_and_comment_text_clean, subreddit_name

-- Check most important posts per subreddit
ORDER BY subreddit_name, net_upvotes_lookup DESC
;


-- Get distribution for post length per subreddit
WITH
post_and_text_count AS (
    SELECT
        sm.subreddit_seed_for_clusters
        , pc.subreddit_name
        , COUNT(DISTINCT post_id) AS post_count
        , SUM(
            IF(post_and_comment_text_clean_len >= 5, 1, 0)
        ) AS posts_text_len_5_plus
        , SUM(
            IF(post_and_comment_text_clean_len >= 9, 1, 0)
        ) AS posts_text_len_9_plus
        -- , MIN(post_and_comment_text_clean_len) AS text_len_min
        , APPROX_QUANTILES(post_and_comment_text_clean_len, 100)[OFFSET(1)] AS text_len_p01
        , APPROX_QUANTILES(post_and_comment_text_clean_len, 100)[OFFSET(5)] AS text_len_p05
        , APPROX_QUANTILES(post_and_comment_text_clean_len, 100)[OFFSET(10)] AS text_len_p10
        , APPROX_QUANTILES(post_and_comment_text_clean_len, 100)[OFFSET(25)] AS text_len_p25
        , APPROX_QUANTILES(post_and_comment_text_clean_len, 100)[OFFSET(50)] AS text_len_median
        , ROUND(AVG(post_and_comment_text_clean_len), 1) AS text_len_avg
    FROM `reddit-relevance.tmp.subclu_post_and_comment_text_combined_20220629` AS pc
        LEFT JOIN `reddit-relevance.tmp.subclu_subreddits_for_modeling_20220629` AS sm
            ON pc.subreddit_id = sm.subreddit_id
    GROUP BY 1, 2
)

SELECT
    subreddit_seed_for_clusters
    , subreddit_name
    , post_count
    , ROUND(posts_text_len_5_plus / post_count, 2) AS pct_5_plus
    , ROUND(posts_text_len_9_plus / post_count, 2) AS pct_9_plus
    , * EXCEPT(subreddit_seed_for_clusters, subreddit_name, post_count)
FROM post_and_text_count
WHERE 1=1
    -- AND post_count >= 4
    -- AND subreddit_seed_for_clusters = TRUE
    -- AND posts_text_len_9_plus < 5
    -- AND (
    --     subreddit_name LIKE "hmm%"
    --     OR subreddit_name IN (
    --         'ich_iel', 'memexico'
    --     )
    -- )
    -- AND post_and_comment_text_clean_len = 9
    -- AND post_and_comment_text_clean IN (
    --     '', '.', '!', ';', ':'
    -- )
ORDER BY posts_text_len_5_plus DESC, pct_5_plus DESC, posts_text_len_9_plus DESC, text_len_p01 DESC
;
