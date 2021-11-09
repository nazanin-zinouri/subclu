-- ==========================================
-- NOTE: WIP. Won't be using for first iteration because of QA issues with duplicates
--      in language detection tables (hard to get a good count number of posts in a language)
-- ===

-- SELECT detected language for all posts in past ~l28
-- For now, We'll create a language-relevant score for each subreddit based on post-level detected
-- Later we can try or add comment-language detection.

DECLARE lookup_pt_date DATE DEFAULT '2021-09-07';
DECLARE post_created_start DATE DEFAULT '2021-08-25';
DECLARE post_created_end DATE DEFAULT '2021-09-07';

WITH posts_selected AS (
SELECT
    subreddit_id
    , author_id
    , post_id
    , upvotes
    , downvotes
    , (upvotes - downvotes) AS net_votes
    , flair_text
    , created_timestamp

    , verdict
    , deleted
    , neutered
    , over_18
    -- , selftext_is_richtext
    , language_preference
    , promoted

FROM `data-prod-165221.ds_v2_postgres_tables.post_lookup` AS plo

WHERE DATE(plo._PARTITIONTIME) = lookup_pt_date
    AND DATE(created_timestamp) BETWEEN post_created_start AND post_created_end
    AND deleted = false
    AND (
        plo.verdict IS NULL
        OR plo.verdict NOT IN ('admin-removed', 'mod-removed')
    )
),

-- Get unique/latest language detected for each post
post_detected_language_ranked AS(
SELECT
    DATE(_PARTITIONTIME) as pt
    -- thing_type DESC = sort `POST` before `COMMENT`
    , ROW_NUMBER() OVER(PARTITION BY post_id ORDER BY thing_type DESC, created_timestamp ASC) AS thing_order_in_post
    , created_timestamp
    , subreddit_id
    , post_id
    , user_id
    -- , id AS post_or_comment_id

    , thing_type
    , app_name
    , geolocation_country_code
    , weighted_language AS language_detected_weighted
    , language AS language_detected
    , probability
    , weighted_language_probability

    , text

FROM `reddit-protected-data.language_detection.comment_language_v2`
WHERE DATE(_PARTITIONTIME) BETWEEN post_created_start AND post_created_end
    AND weighted_language IN (
        'en',  -- English
        'de',  -- German *
        'pt',  -- Portuguese *
        'es',  -- Spanish *
        'fr',  -- French *
        -- 'no',
        -- 'af',
        -- 'nl',
        'it'  -- Italian *
    )
ORDER BY subreddit_id, post_id, thing_type DESC, created_timestamp
),

-- Get unique counts: for each subreddit counts per language
posts_with_language_detected AS (
SELECT
    COALESCE(plo.subreddit_id, lan.subreddit_id) AS subreddit_id
    , COALESCE(plo.post_id, lan.post_id)         AS post_id
    , COALESCE(lan.language_detected, "other") AS language_detected
    , COALESCE(lan.language_detected_weighted, "other") AS language_detected_weighted

FROM posts_selected AS plo
LEFT JOIN (
    SELECT *
    FROM post_detected_language_ranked
    WHERE thing_order_in_post = 1
)AS lan
    ON plo.subreddit_id = lan.subreddit_id AND plo.post_id = lan.post_id
),

-- Count language posts per subreddit
post_language_count_per_sub AS (
SELECT
    subreddit_id
    , language_detected_weighted
    , COUNT(DISTINCT post_id) AS posts_unique_count
FROM posts_with_language_detected

GROUP BY 1, 2
ORDER BY 1, 3 DESC
)

-- Check long count of language per post
SELECT *
FROM post_language_count_per_sub

ORDER BY subreddit_id, posts_unique_count DESC
LIMIT 100
;




-- check raw CTE
-- SELECT
--     *
-- FROM posts_with_language_detected
-- LIMIT 500
-- ;




-- ========================
-- Test/Check queries
-- ===
-- Check counts per subquery
-- SELECT
--     (SELECT COUNT(*) FROM posts_selected) AS posts_selected_count
--     , (SELECT COUNT(*) FROM post_detected_language_ranked)  AS posts_detected_language_ranked_count
--     , (SELECT COUNT(*) FROM posts_with_language_detected)   AS posts_with_unique_language_count
-- ;


-- Check totals post count per language:
-- SELECT
--     language_detected_weighted
--     , COUNT(*) AS row_count
-- FROM posts_with_language_detected

-- GROUP BY 1
-- ORDER BY row_count DESC
-- LIMIT 300
-- ;



-- Check that merged table has all unique posts/rows
-- SELECT
--     COUNT(*)        AS total_rows
--     , COUNT(DISTINCT post_id)   AS post_ids_unique
--     , COUNT(*) = COUNT(DISTINCT post_id)    AS total_equal_unique
-- FROM posts_with_language_detected
-- ;


-- Count to check to make sure we've filtered out removed posts
-- SELECT
--     verdict
--     , promoted
--     , COUNT(DISTINCT post_id) AS post_id_unique_count

-- FROM posts_selected AS plo

-- WHERE 1=1

-- -- 2 categories
-- GROUP BY 1, 2
-- ORDER BY 1, 2, 3 DESC
-- ;
