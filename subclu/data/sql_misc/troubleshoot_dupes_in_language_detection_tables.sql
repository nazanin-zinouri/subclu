-- See wiki:
-- https://reddit.atlassian.net/wiki/spaces/DataScience/pages/2168848484/Language+Detection+Tables
-- Old language detection table (has both posts & comments in single place)
-- - `reddit-protected-data.language_detection.comment_language_v2`
--
-- New language detection tables: (one for posts another one for comments)
-- - `data-prod-165221.language_detection.post_language_detection_cld3`
-- - data-prod-165221.language_detection.comment_language_detection_cld3

-- ========================
-- 2021-09-27
-- WIP here's how to remove duplicates
--
-- Clean language-v2 table to drop duplicate posts
-- NEW:
--   * use `post_thing_user_row_num` = 1 to keep only 1 row in language table
--   * use `row_num` = 1 to keep only 1 row in successful_posts table
--  Use it to make accurate & faster JOINS when counting languge posts & selecting posts
--  for semantic clusters
DECLARE partition_date DATE DEFAULT '2021-09-05';

-- Dates for edited post:
DECLARE pt_start_date DATE DEFAULT '2021-09-03';
DECLARE pt_end_date DATE DEFAULT '2021-09-08';


WITH posts_with_language AS (
    SELECT
        DATE(_PARTITIONTIME) as pt
        -- `thing_order_in_post` doesn't work:
        --  There's no good way to sort at POST-ID only because the `created_timestamp` is the same for all duplicates
        --  the only differences are: user_id, app_name (of commenters), geo_location_country_code (of commenters)
        --    earliest=(created_timestamp ASC) or latest=(created_timestamp DESC)

        -- Instead, rank by post-ID + user_id + thing_type (one user can post AND comment)
        --   If/when we want comment-level we need to add ID (comment ID) INTO THE partition
        , ROW_NUMBER() OVER(
            PARTITION BY post_id, id, user_id
            ORDER BY created_timestamp ASC, probability DESC
        ) AS post_thing_user_row_num
        , ROW_NUMBER() OVER(
            PARTITION BY post_id
            ORDER BY thing_type DESC, created_timestamp ASC
        ) AS thing_order_in_post_asc
        , ROW_NUMBER() OVER(
            PARTITION BY post_id
            ORDER BY thing_type DESC, created_timestamp DESC
        ) AS thing_order_in_post_desc
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
        AND thing_type = 'post'
        AND id = post_id
    ),

    successful_posts AS(
        SELECT
            -- Use row_number to get the latest edit as row=1
            ROW_NUMBER() OVER (
                PARTITION BY post_id
                ORDER BY endpoint_timestamp DESC, removal_timestamp DESC
            ) AS row_num
            , removal_timestamp
            , removed
            , * EXCEPT(removal_timestamp, removed)

        FROM `data-prod-165221.cnc.successful_posts` AS sp

        WHERE sp.dt BETWEEN pt_start_date AND pt_end_date

        ORDER BY post_id, row_num
    )


-- for a post known to have duplicates:
-- GROUP BY thing-type (post v comment)
--  count rows
SELECT
    -- get counts only
    COALESCE(sp.post_id, pl.post_id) AS post_id
    , pl.thing_type
    , COUNT(*) AS row_count
    , COUNT(DISTINCT pl.post_or_comment_id ) AS post_or_comment_id_unique
    , COUNT(DISTINCT sp.user_id ) AS user_id_unique

    -- check individual rows
    -- sp.subreddit_name
    -- , COALESCE(sp.post_id, pl.post_id) AS post_id
    -- , COALESCE(sp.user_id, pl.user_id) AS user_id
    -- , pl.thing_order_in_post_asc
    -- , pl.thing_order_in_post_desc
    -- , pl.created_timestamp
    -- , pl.pt
    -- , pl.post_thing_user_row_num

    -- , sp.row_num
    -- , sp.removal_timestamp
    -- , sp.endpoint_timestamp
    -- , sp.dt
    -- , sp.submit_date

    -- , sp.post_title
    -- , LEFT(pl.text, 50) AS text_first_50
    -- , pl.* EXCEPT(thing_order_in_post_desc, thing_order_in_post_asc, created_timestamp, text)
    -- , sp.* EXCEPT(row_num, removal_timestamp, endpoint_timestamp, dt, submit_date, post_body_text)

FROM posts_with_language AS pl
INNER JOIN successful_posts AS sp
    ON pl.post_id = sp.post_id AND pl.user_id = sp.user_id
        AND pl.app_name = sp.app_name
        AND pl.thing_type = sp.noun
WHERE 1=1
    -- Remove duplicates in successful_post table (multiple rows when a post is removed multiple times)
    AND sp.row_num = 1

    -- Remove dupes in language detection table at POST-LEVEL
    -- Note: might NEED A SEPARATE COL FOR COMMENT-LEVEL DUPES
    AND pl.post_thing_user_row_num = 1

    AND (
        -- posts with some duplicates expected
        COALESCE(sp.post_id, pl.post_id) IN (
            't3_pi4jz7', 't3_pijlpp', 't3_phnk2k'
        )
        -- post IDs that aren't duplicated
        OR COALESCE(sp.post_id, pl.post_id) IN (
            't3_piqzou', 't3_pj61di', 't3_pj9xfo',
            't3_pj3wld', 't3_pisbia', 't3_pj58h3',
            't3_pj8nzu', 't3_piuu4j'
        )
        -- Too many duplicates 't3_pits1o' (63), 't3_pih5t4' (90 repeats), 't3_phn3iu', 't3_pi2imr'
        --   use to test FINAL join b/c otherwise it's too noisy to get the uniques
        -- OR COALESCE(sp.post_id, pl.post_id) IN (
        --     't3_pits1o', 't3_pih5t4', 't3_phn3iu'
        -- )
    )

-- Only apply groupby when counting
GROUP BY 1, 2

-- Order by when inspecting individual rows
-- ORDER BY sp.subreddit_id, sp.post_id, thing_type DESC, created_timestamp
-- ORDER BY sp.post_id, thing_type DESC, created_timestamp
;




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
    -- thing_type DESC = sort `POST` before `COMMENT`
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


-- After chatting with Anna, she recommended using post_lookup table to get
--  the latest status of a post
-- Check post & comment counts for the past ~5 years
DECLARE pt_date DATE DEFAULT '2021-09-07';

-- USE CTE because we want to calculate differences between uniques & deleted
WITH post_count_per_year AS (
SELECT
    EXTRACT(YEAR from created_timestamp) AS YEAR
    , COUNT(*) total_rows
    , COUNT(DISTINCT post_id) AS post_id_unique
    , SUM(
        CASE WHEN (deleted = true) THEN 1
            ELSE 0
        END
    ) AS post_ids_deleted
FROM `data-prod-165221.ds_v2_postgres_tables.post_lookup` AS plo

WHERE DATE(_PARTITIONTIME) = pt_date
    AND created_timestamp >= '2015-01-01'

GROUP BY 1

ORDER BY 1
)

SELECT
    *
    , (post_id_unique - post_ids_deleted) AS post_ids_not_deleted
    , (total_rows = post_id_unique) AS total_rows_equal_unique_ids
FROM post_count_per_year
;


-- for successful posts we can get something close to it:
DECLARE pt_start_date DATE DEFAULT '2021-09-04';
DECLARE pt_end_date DATE DEFAULT '2021-09-07';


-- Select duplicated posts to see why they're duplicated
WITH duplicated_check AS (
SELECT
    -- Use row_number to get the latest edit as row=1
    ROW_NUMBER() OVER (
        PARTITION BY post_id
        ORDER BY endpoint_timestamp DESC, removal_timestamp desc
    ) AS row_num
    , removal_timestamp
    , removed
    , * EXCEPT(removal_timestamp, removed)

FROM `data-prod-165221.cnc.successful_posts` AS sp

WHERE sp.dt BETWEEN pt_start_date AND pt_end_date
    -- Post IDs that have duplicates in `successful_posts`
    --  Too many duplicates 't3_pits1o', 't3_phn3iu'
    AND post_id IN ('t3_pits1o', 't3_pih5t4', 't3_pi4jz7', 't3_pijlpp', 't3_phnk2k')
    -- post IDs that aren't duplicated
    OR post_id IN (
        't3_piqzou', 't3_pj61di', 't3_pj9xfo',
        't3_pj3wld', 't3_pisbia', 't3_pj58h3', 't3_pj8nzu', 't3_piuu4j'
    )

ORDER BY post_id, row_num
)

-- Find posts that are duplicated or NOT duplicated
-- SELECT
--     post_id
--     , COUNT(*)  AS row_count

-- FROM `data-prod-165221.cnc.successful_posts` AS sp

-- WHERE sp.dt BETWEEN pt_start_date AND pt_end_date
--     AND removed = 0

-- GROUP BY 1

-- ORDER BY 2 ASC
-- LIMIT 1500
-- ;


-- Count posts
-- SELECT
--     COUNT(*) total_rows
--     , COUNT(DISTINCT post_id) AS post_id_unique
--     , SUM(
--         CASE WHEN (removed = 0) THEN 1
--             ELSE 0
--         END
--     ) AS post_ids_active
--     , SUM(
--         CASE WHEN (removed = 1) THEN 1
--             ELSE 0
--         END
--     ) AS post_ids_removed

-- FROM `data-prod-165221.cnc.successful_posts` AS sp

-- WHERE sp.dt BETWEEN pt_start_date AND pt_end_date
-- ;

-- ===============
-- CTE calls
-- ===
-- If we sort by removal date, what happens if removal date is blank?
SELECT
    *
FROM duplicated_check
;

-- HOWEVER, it looks like we can find duplicates if we exclude: removal_timestamp
--  it appears that this is the column that can change over time :mind-blown: sigh
--  so we should probably row_num() over removed time stamp and keep the latest action
-- SELECT
--     DISTINCT * EXCEPT(removal_timestamp, row_num)
-- FROM duplicated_check
-- ;

-- We can also get uniques by picking only row_num = 1
-- SELECT
--     *
-- FROM duplicated_check
-- WHERE row_num = 1
-- ;

-- Try DISTINCT: it doesn't remove any duplicates on all columns
-- SELECT
--     DISTINCT * EXCEPT (row_num)
-- FROM duplicated_check
-- ;




-- COMMENT level doesn't have an equivalent table to `post_lookup`
-- Using `successful_comment` returns a very small number of comments not sure why.
--   is it because they're filtering out spam or something else?
DECLARE pt_start_date DATE DEFAULT '2015-01-01';
DECLARE pt_end_date DATE DEFAULT '2021-09-07';

WITH comments_ranked AS (
SELECT
    -- Use row_number to get the latest edit as row=1
    ROW_NUMBER() OVER (
        PARTITION BY post_id
        ORDER BY endpoint_timestamp DESC, removal_timestamp desc
    ) AS row_num
    , removal_timestamp
    , removed
    , * EXCEPT(removal_timestamp, removed)

FROM `data-prod-165221.cnc.successful_comments` AS sc

WHERE sc.dt BETWEEN pt_start_date AND pt_end_date
    -- AND comment_id IN (
    --     -- w/o dupes
    --     't1_hbq0t4q', 't1_hbmzuzk', 't1_hbo17hj', 't1_hbmwjyb'

    --     -- with dupes
    --     , 't1_hbma5hj', 't1_hbl8kh0', 't1_hboyjd1'

    --     -- with dupes AND removed=0
    --     , 't1_hbq752z', 't1_hbqgi1k'
    -- )

),

comment_count_per_year AS (
SELECT
    EXTRACT(YEAR from endpoint_timestamp) AS YEAR
    , COUNT(*) total_rows
    , COUNT(DISTINCT comment_id) AS comment_id_unique
    , SUM(
        CASE WHEN (removed = 1) THEN 1
            ELSE 0
        END
    ) AS comment_ids_removed
    , SUM(
        CASE WHEN (removed = 0) THEN 1
            ELSE 0
        END
    ) AS comment_ids_active

FROM comments_ranked

WHERE 1=1
    AND row_num = 1

GROUP BY 1
ORDER BY 1
)

-- Counts PER YEAR
SELECT
    *
FROM comment_count_per_year
;

-- Try DISTINCT: it doesn't remove any duplicates on all columns
-- SELECT
--     DISTINCT * EXCEPT (row_num)
-- FROM comments_unique

-- ORDER BY comment_id, endpoint_timestamp DESC
-- ;

-- If we sort by removal date, what happens if removal date is blank?
-- SELECT
--     *
-- FROM comments_unique

-- ORDER BY post_id, row_num

-- LIMIT 200
-- ;

-- HOWEVER, it looks like we can find duplicates if we exclude: removal_timestamp & endpoint_timestamp
--  it appears that these cols that can change over time :mind-blown: sigh
--  so we should probably row_num() over removed time stamp and keep the latest action
-- SELECT
--     DISTINCT * EXCEPT(removal_timestamp, row_num)
-- FROM comments_unique

-- ORDER BY comment_id, endpoint_timestamp DESC
-- ;

-- We can also get uniques by picking only row_num = 1
-- SELECT
--     *
-- FROM comments_unique
-- WHERE row_num = 1
-- ;

-- ALL Counts using new filter
-- SELECT
--     COUNT(*) total_rows
--     , COUNT(DISTINCT comment_id) AS comment_id_unique
--     , SUM(
--         CASE WHEN (removed = 0) THEN 1
--             ELSE 0
--         END
--     ) AS comment_ids_active
--     , SUM(
--         CASE WHEN (removed = 1) THEN 1
--             ELSE 0
--         END
--     ) AS comment_ids_removed
-- FROM comments_unique
-- WHERE row_num = 1
-- ;

-- =====================
-- Other calls w/o CTE
-- ===
-- Find COMMENTS that are duplicated or NOT duplicated
-- SELECT
--     comment_id
--     , COUNT(*)  AS row_count

-- FROM `data-prod-165221.cnc.successful_comments` AS sc

-- WHERE sc.dt BETWEEN pt_start_date AND pt_end_date
--     AND removed = 0

-- GROUP BY 1

-- ORDER BY 2 DESC
-- LIMIT 1500
-- ;


-- Check whether successful comments table also has duplicates
--  Answer: yes
-- SELECT
--     COUNT(*) total_rows
--     , COUNT(DISTINCT comment_id) AS comment_id_unique
--     , SUM(
--         CASE WHEN (removed = 0) THEN 1
--             ELSE 0
--         END
--     ) AS comment_ids_active
--     , SUM(
--         CASE WHEN (removed = 1) THEN 1
--             ELSE 0
--         END
--     ) AS comment_ids_removed

-- FROM `data-prod-165221.cnc.successful_comments` AS sc

-- WHERE sc.dt BETWEEN pt_start_date AND pt_end_date
-- ;
