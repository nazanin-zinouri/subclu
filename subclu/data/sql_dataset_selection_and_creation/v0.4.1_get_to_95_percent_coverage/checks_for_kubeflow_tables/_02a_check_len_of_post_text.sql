-- Check len of post text for embedding (title, body, OCR, flair, URL)
SELECT
    COUNT(*) as row_count
    , COUNT(DISTINCT post_id) AS post_unique_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_id_unique_count
    , COUNT(DISTINCT subreddit_name) AS subreddit_name_unique_count

    -- , SUM(
    --     CASE WHEN (active = TRUE) THEN 1
    --     ELSE 0
    --     END
    -- ) AS active_subreddit_count

    , MIN(post_text_for_embeddings_len) AS post_len_min
    , APPROX_QUANTILES(post_text_for_embeddings_len, 100)[OFFSET(25)] AS post_len_p25
    , APPROX_QUANTILES(post_text_for_embeddings_len, 100)[OFFSET(50)] AS post_len_median
    , ROUND(AVG(post_text_for_embeddings_len), 2)     AS post_len_avg
    , APPROX_QUANTILES(post_text_for_embeddings_len, 100)[OFFSET(75)] AS post_len_p75
    , APPROX_QUANTILES(post_text_for_embeddings_len, 100)[OFFSET(85)] AS post_len_p85
    , APPROX_QUANTILES(post_text_for_embeddings_len, 100)[OFFSET(90)] AS post_len_p90
    , APPROX_QUANTILES(post_text_for_embeddings_len, 100)[OFFSET(95)] AS post_len_p95
    , APPROX_QUANTILES(post_text_for_embeddings_len, 100)[OFFSET(99)] AS post_len_p99
    , MAX(post_text_for_embeddings_len)               AS post_len_max

FROM `reddit-relevance.tmp.subclu_posts_for_modeling_20220622`



-- Check len of ONLY post title + body
SELECT
    COUNT(*) as row_count
    , COUNT(DISTINCT post_id) AS post_unique_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_id_unique_count
    , COUNT(DISTINCT subreddit_name) AS subreddit_name_unique_count

    -- , SUM(
    --     CASE WHEN (active = TRUE) THEN 1
    --     ELSE 0
    --     END
    -- ) AS active_subreddit_count

    , MIN(post_title_and_body_text_clean_len) AS post_len_min
    , APPROX_QUANTILES(post_title_and_body_text_clean_len, 100)[OFFSET(25)] AS post_len_p25
    , APPROX_QUANTILES(post_title_and_body_text_clean_len, 100)[OFFSET(50)] AS post_len_median
    , ROUND(AVG(post_title_and_body_text_clean_len), 2)     AS post_len_avg
    , APPROX_QUANTILES(post_title_and_body_text_clean_len, 100)[OFFSET(75)] AS post_len_p75
    , APPROX_QUANTILES(post_title_and_body_text_clean_len, 100)[OFFSET(85)] AS post_len_p85
    , APPROX_QUANTILES(post_title_and_body_text_clean_len, 100)[OFFSET(90)] AS post_len_p90
    , APPROX_QUANTILES(post_title_and_body_text_clean_len, 100)[OFFSET(95)] AS post_len_p95
    , APPROX_QUANTILES(post_title_and_body_text_clean_len, 100)[OFFSET(99)] AS post_len_p99
    , MAX(post_title_and_body_text_clean_len)               AS post_len_max

FROM `reddit-relevance.tmp.subclu_posts_for_modeling_20220622`
;

-- Sample results (7 days)
-- row_count	post_unique_count	subreddit_id_unique_count	subreddit_name_unique_count
-- 4,383,428	4,383,428	75,907	75,907


-- Sample results (7 days)
-- Distribution of len(post title + post body)
--  zero -> post title contained only periods, slashes or other punctuation
-- post_len_min	    0
-- post_len_p25	    30
-- post_len_median	59
-- sub_desc_len_avg	248.7
-- post_len_p75	    175
-- post_len_p85	    361
-- post_len_p90	    578
-- post_len_p95	    1,039
-- post_len_p99	    2,629


-- OCR text len
SELECT
    COUNT(*) as row_count
    , COUNT(DISTINCT post_id) AS post_unique_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_id_unique_count
    , COUNT(DISTINCT subreddit_name) AS subreddit_name_unique_count

    , SUM(
        CASE WHEN (ocr_text_len IS NOT NULL) THEN 1
            ELSE 0
        END
    ) AS posts_with_ocr_text

    , MIN(ocr_text_len) AS ocr_text_len_min
    , APPROX_QUANTILES(ocr_text_len, 100)[OFFSET(50)] AS ocr_text_len_median
    , AVG(ocr_text_len) AS ocr_text_len_avg
    , APPROX_QUANTILES(ocr_text_len, 100)[OFFSET(75)] AS ocr_text_len_p75
    , APPROX_QUANTILES(ocr_text_len, 100)[OFFSET(85)] AS ocr_text_len_p85
    , APPROX_QUANTILES(ocr_text_len, 100)[OFFSET(90)] AS ocr_text_len_p90
    , APPROX_QUANTILES(ocr_text_len, 100)[OFFSET(95)] AS ocr_text_len_p95
    , APPROX_QUANTILES(ocr_text_len, 100)[OFFSET(99)] AS ocr_text_len_p99

FROM `reddit-relevance.tmp.subclu_posts_for_modeling_20220501`
;

-- Sample results (7 days)
-- row_count	post_unique_count	subreddit_id_unique_count	subreddit_name_unique_count	posts_with_ocr_text
-- 4,383,428	4,383,428	75,907	75,907	457,507

-- ocr_text_len_min	    1.0
-- ocr_text_len_median	64.0
-- ocr_text_len_avg	    194.06
-- ocr_text_len_p75	    197.0
-- ocr_text_len_p85	    333.0
-- ocr_text_len_p90	    463.0
-- ocr_text_len_p95	    749.0
-- ocr_text_len_p99	    1,863.0
