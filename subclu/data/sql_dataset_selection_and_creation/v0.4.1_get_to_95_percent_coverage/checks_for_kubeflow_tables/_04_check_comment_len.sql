-- Check len of COMMENT text for embedding
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
