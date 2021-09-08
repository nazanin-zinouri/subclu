-- See wiki:
-- https://reddit.atlassian.net/wiki/spaces/DataScience/pages/2168848484/Language+Detection+Tables
-- Old language detection table (has both posts & comments in single place)
-- - `reddit-protected-data.language_detection.comment_language_v2`
--
-- New language detection tables: (one for posts another one for comments)
-- - `data-prod-165221.language_detection.post_language_detection_cld3`
-- - data-prod-165221.language_detection.comment_language_detection_cld3


-- ========================
-- Does new table still have duplicated posts?
--  Yes, it looks like it does...
-- =========
-- QA on new post-language table. Do we still see duplicated posts?
--  Yes, we still see duplicates...
DECLARE pt_start_date DATE DEFAULT '2021-08-29';
DECLARE pt_end_date DATE DEFAULT '2021-09-05';

SELECT
    DATE(created_timestamp)     AS  created_date
    , COUNT(*)                  AS  row_count
    , COUNT(DISTINCT thing_id)  AS  post_unique_count
    , COUNT(DISTINCT subreddit_id)  AS subreddit_unique_count

FROM `data-prod-165221.language_detection.post_language_detection_cld3`
WHERE DATE(created_timestamp) BETWEEN pt_start_date AND pt_end_date

GROUP BY 1
ORDER BY 1 DESC
;



-- ========================
-- Example in wiki for old posts
-- Looks like comments are fine, but posts are duplicated: each post creates a new post-level row...
-- =========
-- Debug language-v2 table to drop duplicate posts/comments
--  Use it to make accurate & faster JOINS when counting languge posts & selecting posts
--  for semantic clusters
DECLARE partition_date DATE DEFAULT '2021-09-05';

-- Dates for edited post:
DECLARE pt_start_date DATE DEFAULT '2021-09-03';
DECLARE pt_end_date DATE DEFAULT '2021-09-08';


WITH language_data AS (
SELECT
    DATE(_PARTITIONTIME) as pt
    , ROW_NUMBER() OVER(PARTITION BY post_id ORDER BY thing_type DESC, created_timestamp ASC) AS thing_order_in_post
    -- , *
    , created_timestamp
    , subreddit_id
    , post_id
    , user_id
    , id AS post_or_comment_id

    , thing_type
    , app_name
    , geolocation_country_code
    , weighted_language
    , language
    , probability
    , weighted_language_probability

    , text

FROM `reddit-protected-data.language_detection.comment_language_v2`
WHERE DATE(_PARTITIONTIME) BETWEEN pt_start_date AND pt_end_date

ORDER BY subreddit_id, post_id, thing_type DESC, created_timestamp
)

-- for a post known to have duplicates:
-- GROUP BY thing-type (post v comment)
--  count rows
SELECT
    post_id
    , thing_type
    , COUNT(*) AS row_count
    , COUNT(DISTINCT post_or_comment_id ) AS post_or_comment_id_unique
    , COUNT(DISTINCT user_id ) AS user_id_unique

FROM language_data
WHERE 1=1

    -- maybe better from eli5
    AND post_id = 't3_pi2imr'

    -- from r/soccer: Op comments in post
    AND post_id = 't3_pk8dc7'

GROUP BY 1, 2
;



-- ========================
-- Scratch work / WIP
-- ===
-- Debug language-v2 table to drop duplicate posts/comments
--  Use it to make accurate & faster JOINS when counting languge posts & selecting posts
--  for semantic clusters
DECLARE partition_date DATE DEFAULT '2021-09-05';

-- Dates for old posts I found
-- DECLARE pt_start_date DATE DEFAULT '2021-08-29';
-- DECLARE pt_end_date DATE DEFAULT '2021-09-02';

-- Dates for edited post:
DECLARE pt_start_date DATE DEFAULT '2021-09-03';
DECLARE pt_end_date DATE DEFAULT '2021-09-08';


WITH language_data_uniques AS (
SELECT
    created_timestamp
    , DATE(_PARTITIONTIME) as pt
    , subreddit_id
    , post_id
    , user_id
    , id AS post_or_comment_id
    , ROW_NUMBER() OVER(PARTITION BY post_id ORDER BY thing_type DESC, created_timestamp ASC) AS thing_order_in_post
    , thing_type
    , app_name
    , geolocation_country_code
    , weighted_language
    , language
    , probability
    , weighted_language_probability

    , text

FROM `reddit-protected-data.language_detection.comment_language_v2`
WHERE DATE(_PARTITIONTIME) BETWEEN pt_start_date AND pt_end_date

ORDER BY subreddit_id, post_id, thing_type DESC, created_timestamp
),

language_data AS (
SELECT
    DATE(_PARTITIONTIME) as pt
    , ROW_NUMBER() OVER(PARTITION BY post_id ORDER BY thing_type DESC, created_timestamp ASC) AS thing_order_in_post
    -- , *
    , created_timestamp
    , subreddit_id
    , post_id
    , user_id
    , id AS post_or_comment_id

    , thing_type
    , app_name
    , geolocation_country_code
    , weighted_language
    , language
    , probability
    , weighted_language_probability

    , text

FROM `reddit-protected-data.language_detection.comment_language_v2`
WHERE DATE(_PARTITIONTIME) BETWEEN pt_start_date AND pt_end_date

ORDER BY subreddit_id, post_id, thing_type DESC, created_timestamp
)

-- See posts & comments
-- SELECT
--     *
-- FROM language_data
-- WHERE 1=1
--     -- AND thing_type = 'post'

--     AND post_id IN (
--         't3_pgjf1l', 't3_pe6tmt', 't3_pgfyb2',

--         -- post with an edit:
--         't3_pjfl6z'
--     )
-- ;



-- for a post known to have duplicates:
-- GROUP BY thing-type (post v comment)
--  count rows
--  count distinct post_or_comment_IDs
--  Count user-ids
--  count of DISTINCT user-id, created_timestamp
--  count of DISTINCT user-id, post_or_comment_IDs
--  count of DISTINCT user-id, created_timestamp, app_name
SELECT
    post_id
    , thing_type
    , COUNT(*) AS row_count
    , COUNT(DISTINCT post_or_comment_id ) AS post_or_comment_id_unique
    , COUNT(DISTINCT user_id ) AS user_id_unique
    -- , COUNT(DISTINCT user_id, created_timestamp) AS user_id_by_created_dt_unique

FROM language_data
WHERE 1=1
    -- post with an edit & multiple comments from switzerland
    -- AND post_id = 't3_pjfl6z'

    -- maybe better from eli5
    AND post_id = 't3_pi2imr'

GROUP BY 1, 2
;

