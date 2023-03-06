-- CTE checks for comments post language table

-- Check counts of post language table
--  All should be the same value (except posts unique b/c a post can have multiple comments)
-- SELECT
--     COUNT(*) as row_count
--     , COUNT(DISTINCT comment_id) AS comment_unique_count
--     , SUM(IF(post_thing_user_row_num = 1, 1, 0)) as row_num1_comments
--     , COUNT(DISTINCT post_id) as posts_unique_count
-- FROM comment_language
-- ;



-- Check top languages (overall)
-- SELECT
--     weighted_language_name
--     -- , top1_language_name
--     -- , removed

--     , STRING_AGG(DISTINCT(weighted_language_code), ',') AS weighted_language_codes
--     , COUNT(DISTINCT post_id) as posts_unique_count
--     , ROUND(100.0 * COUNT(DISTINCT post_id) / (SELECT COUNT(*) FROM post_language), 3) AS posts_pct
--     -- , COUNT(*) AS row_count

-- FROM post_language AS p

-- WHERE 1=1
--     AND post_title_and_body_text_length >= 1
--     -- AND weighted_language_name IN (
--     --     'Chinese', 'Russian'
--     -- )
-- GROUP BY 1  -- , 2  -- 3=removed
-- ORDER BY 3 DESC, 1 ASC
-- ;


-- Get Length aggregates per post type
--  Check how much longer are "text" posts than other posts?
-- SELECT
--     post_type
--     , AVG(COALESCE(post_title_length, 0)) AS post_title_len_avg
--     , AVG(post_body_length) AS post_body_len_avg_if_not_null
--     , AVG(post_title_and_body_text_length) AS post_title_and_body_len_avg
--     , COUNT(DISTINCT post_id) AS post_id_count
--     , ROUND(100.0 * COUNT(DISTINCT post_id) / (SELECT COUNT(*) FROM post_language), 3) AS posts_pct
-- FROM post_language
-- GROUP BY 1
-- ORDER BY 5 DESC, 2 DESC
-- ;


-- Check codes (overall)
--  if we filter by code.id flag languages that look wrong
-- SELECT
--     lc.language_name
--     , p.weighted_language_code
--     , p.top1_language_code

--     -- , COUNT(DISTINCT p.top1_language_code) as language_code_count
--     -- , STRING_AGG(DISTINCT(weighted_language_code), ',') AS weighted_language_codes
--     -- , STRING_AGG(DISTINCT(p.top1_language_code), ',') AS top1_language_codes
--     , COUNT(DISTINCT post_id) as posts_unique_count
--     , ROUND(100.0 * COUNT(DISTINCT post_id) / (SELECT COUNT(*) FROM post_language), 2) AS posts_pct

-- FROM post_language AS p
--     LEFT JOIN `reddit-employee-datasets.david_bermejo.language_detection_code_to_name_lookup_cld3` AS lc
--         ON p.weighted_language_code = lc.language_code

-- WHERE 1=1
--     -- AND post_title_and_body_text_length >= 9

-- GROUP BY 1, 2, 3

-- HAVING (
--     posts_unique_count >= 10
--     AND weighted_language_code != top1_language_code
-- )
-- ORDER BY 1 ASC, 4 DESC
-- ;


-- Check completed table
DECLARE PT_END DATE DEFAULT "2023-03-04";
DECLARE POST_PT_START DATE DEFAULT PT_END - 1;

SELECT *
FROM `reddit-employee-datasets.david_bermejo.post_language_detection_cld3_clean`
WHERE dt BETWEEN POST_PT_START AND PT_END
;
