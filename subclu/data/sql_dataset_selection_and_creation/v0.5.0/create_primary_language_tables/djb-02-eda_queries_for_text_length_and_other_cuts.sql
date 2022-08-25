-- Get count & % of comments per language
SELECT
    weighted_language_name
    , COUNT(*) as row_count
    , ROUND( 100.0 * COUNT(*) / (
        SELECT COUNT(*)
        FROM `reddit-employee-datasets.david_bermejo.comment_language_detection_cld3_clean`
        WHERE 1=1
            -- AND dt BETWEEN "2022-07-01" AND "2022-08-01"
            -- AND comment_text_length >= 100
            AND comment_text_length BETWEEN 20 and 100
    ), 2) AS pct_post_of_total
FROM `reddit-employee-datasets.david_bermejo.comment_language_detection_cld3_clean`
WHERE 1=1
    -- AND comment_text_length >= 100
    AND comment_text_length BETWEEN 20 and 100

GROUP BY 1
ORDER BY 2 DESC
;

-- Get count & % of posts by post type
SELECT
    post_type
    , COUNT(*) as row_count
    , COUNT(*) / (
        SELECT COUNT(*)
        FROM `reddit-employee-datasets.david_bermejo.post_language_detection_cld3_clean`
        -- WHERE dt BETWEEN "2022-07-01" AND "2022-08-01"
    ) AS pct_post_of_total
FROM `reddit-employee-datasets.david_bermejo.post_language_detection_cld3_clean`

GROUP BY 1



