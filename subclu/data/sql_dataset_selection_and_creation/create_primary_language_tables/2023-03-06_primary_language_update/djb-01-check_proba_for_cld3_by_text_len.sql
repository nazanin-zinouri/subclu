-- Use this query to figure out what might be a good threshold for comment
--  length to keep for primary language detection.
--  Looks like at 20-30 cld3 goes over 0.80 proba, so let's use Min Text Len >= 20
SELECT
    CASE
        WHEN (comment_text_length BETWEEN 0 and 10) THEN '00_and_10'
        WHEN (comment_text_length BETWEEN 10 and 20) THEN '10_and_20'
        WHEN (comment_text_length BETWEEN 20 and 30) THEN '20_and_30'
        WHEN (comment_text_length BETWEEN 30 and 40) THEN '30_and_40'
        WHEN (comment_text_length BETWEEN 40 and 50) THEN '40_and_50'
        WHEN (comment_text_length BETWEEN 50 and 70) THEN '50_and_70'
        WHEN (comment_text_length BETWEEN 70 and 85) THEN '70_and_85'
        WHEN (comment_text_length BETWEEN 85 and 99) THEN '85_and_99'
        ELSE 'over_100'
    END AS text_len_bin
    , AVG(top1_language_probability) AS top1_language_proba_avg
    , COUNT(*) AS row_count
    , COUNT(*) / (
        SELECT COUNT(*) FROM `reddit-employee-datasets.david_bermejo.comment_language_detection_cld3_clean`
    ) AS pct_comments_of_total
FROM `reddit-employee-datasets.david_bermejo.comment_language_detection_cld3_clean`
WHERE 1=1
    -- AND dt = "2022-08-08"
    -- AND comment_text_length >= 20

GROUP BY 1
ORDER BY 1
;


-- Get proba, comment count & % by text length
--  Use buckets that map to use case:
--    * I exclude shorter than 20 chars for subreddit-level aggregation
--    * baby peach only does posts over 100 chars (and zero-level)
SELECT
    CASE
        WHEN (comment_text_length BETWEEN 0 and 20) THEN '000_and_20'
        WHEN (comment_text_length BETWEEN 20 and 99) THEN '020_and_99'
        ELSE '100_or_more'
    END AS text_len_bin
    , AVG(top1_language_probability) AS top1_language_proba_avg
    , COUNT(*) AS row_count
    , ROUND(100 * COUNT(*) / (
        SELECT COUNT(*)
        FROM `reddit-employee-datasets.david_bermejo.comment_language_detection_cld3_clean`
        -- WHERE dt BETWEEN "2022-07-01" AND "2022-08-01"
    ), 2) AS pct_comments_of_total
FROM `reddit-employee-datasets.david_bermejo.comment_language_detection_cld3_clean`
WHERE 1=1
    -- AND dt BETWEEN "2022-07-01" AND "2022-08-01"
    -- AND comment_text_length >= 20

GROUP BY 1
ORDER BY 1
;
