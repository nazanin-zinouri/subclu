-- Remove comment dupes to improve query pereformance
--  when selecting comments for cluster


-- It looks like the duplicates in successful_post come in 2 flavors:
--   * comments removed multiple times (still don't know why)
--   * comments that get 2 (or more) submit events (maybe some weird async thing?)
-- So using row_number() solves the comment dupe issue
DECLARE start_date DATE DEFAULT '2021-08-01';
DECLARE end_date DATE DEFAULT '2021-09-21';
DECLARE MIN_COMMENT_LEN NUMERIC DEFAULT 11;
DECLARE MAX_COMMENTS_PER_POST NUMERIC DEFAULT 8;


WITH
    geo AS (
        SELECT
            # Keys & IDS
            gs.subreddit_name
            , ROW_NUMBER() OVER (
                PARTITION BY sp.comment_id
                ORDER BY endpoint_timestamp DESC, removal_timestamp DESC
            ) AS row_num_comment_dupes
            , sp.subreddit_id
            , sp.post_id
            , sp.comment_id
            , sp.user_id

            , sp.* EXCEPT(comment_id, comment_body_text, subreddit_id, post_id, user_id)

            # Meta content
            -- , sp.submit_date
            -- , sp.endpoint_timestamp
            -- , sp.noun
            -- , sp.removed
            -- , sp.upvotes
            -- , sp.successful
            -- , sp.app_name
            -- , sp.post_type
            -- -- , sp.post_url
            -- , sp.post_nsfw

            -- Meta about subreddit
            # , gs.combined_topic
            -- , gs.combined_topic_and_rating
            # , gs.rating
            # , gs.rating_version

            -- Text
            , sp.comment_body_text


        -- Start with selected posts to reduce orphan comments
        FROM `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20210927` AS gs
        -- FROM (
        --     SELECT *
        --     FROM `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20210716`
        --     LIMIT 20
        -- ) AS gs

        LEFT JOIN (
            SELECT * FROM `data-prod-165221.cnc.successful_comments`
            WHERE dt BETWEEN start_date AND end_date
        ) AS sp
            ON gs.subreddit_name = sp.subreddit_name
                AND gs.post_id = sp.post_id

        WHERE 1=1
            AND sp.removed = 0
    )

-- ==================
-- Checks/ tests

-- Check uniques in geo
SELECT
    *
    , (post_unique_count / subreddit_unique_count) AS posts_per_subreddit_mean
    , (comment_unique_count / subreddit_unique_count) AS comments_per_subreddit_mean
    , (comment_unique_count / post_unique_count) AS comments_per_post_mean

FROM (
    SELECT
        COUNT(*)       AS row_count
        , COUNT(DISTINCT post_id) AS post_unique_count
        , COUNT(DISTINCT comment_id) AS comment_unique_count
        -- , COUNTIF(removed=0)        AS comment_not_removed_count  -- only use as check if including all comments
        , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count
        , COUNT(DISTINCT user_id)   AS user_id_unique

    FROM geo
    WHERE 1=1
        AND row_num_comment_dupes = 1
);
-- === '2021-09-18' to '2021-09-21' ===
-- RESULT, only filtering sp.removed=0
--  row_count 	 post_unique_count 	 comment_unique_count 	 subreddit_unique_count 	 user_id_unique
--                                                                  posts_per_subreddit_mean 	 comments_per_subreddit_mean
--  14,865,721 	 796,800 	 14,849,738 	 18,209 	 3,014,466 	 43.76 	 815.52

-- RESULT, With successful comments rownum=1
--  row_count 	 post_unique_count 	 comment_unique_count 	 subreddit_unique_count 	 user_id_unique 	 posts_per_subreddit_mean 	 comments_per_subreddit_mean
--  14,849,738 	 796,800 	 14,849,738 	 18,209 	 3,014,466 	 43.76 	 815.52

-- RESULT, after looking at full date range
--  row_count 	 post_unique_count 	 comment_unique_count 	 subreddit_unique_count 	 user_id_unique 	 posts_per_subreddit_mean 	 comments_per_subreddit_mean 	 comments_per_post_mean
--  192,454,697  7,042,396 	 192,454,697 	 19,021 	 10,524,266 	 370.24 	 10,118.01 	 27.33


-- ===
-- Inspect dupes
-- Select suspected dupes to get IDS to investigate how to dedupe
-- SELECT
--     LEFT(comment_body_text, 80) AS comment_body_text_left_
--     , * EXCEPT(comment_body_text, post_body_text)

-- FROM geo
-- WHERE 1=1
--     -- AND row_num_comment_dupes >= 2
--     AND comment_id IN (
--         't1_hdc5mvq', 't1_hdgrwwt', 't1_hdgwp3h',
--         't1_hdiilf6', 't1_hdm3xh8'
--     )

-- ORDER BY subreddit_id, post_id
-- LIMIT 1000
-- ;





-- ==================
-- `data-prod-165221.fact_tables.comment_events` has interesting info like geo-location of person posting - maybe we could also integrate comments post geo-location into geo-relevant score (e.g., give more weight if posts & comments come from a country, not just views)
--  It can also help track a conversation (it has comment_parent_id)
--  HOWEVER this one is not good for ranking comments because it doesn't contain latest stats for upvotes/scores/points (they mostly seem to be 1)
WITH
    geo AS (
        SELECT
            LEFT(comment_body_text, 80) AS comment_body_text_left_
            , ROW_NUMBER() OVER (
                PARTITION BY comment_id
                ORDER BY endpoint_timestamp DESC
            ) AS row_num_comment_dupes
            , ce.* EXCEPT(post_body_text, post_title, post_url, comment_body_text)

        -- Start with selected posts to reduce orphan comments
        FROM `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20210927` AS gs
        LEFT JOIN (
            SELECT * FROM `data-prod-165221.fact_tables.comment_events`
            WHERE DATE(pt) BETWEEN start_date AND end_date
        ) AS ce
            ON gs.post_id = ce.post_id AND gs.subreddit_id = ce.subreddit_id
        WHERE 1=1
        -- LIMIT 1000
    )

-- ==================
-- Checks/ tests

-- Check uniques in geo
-- SELECT
--     *
--     , (post_unique_count / subreddit_unique_count) AS posts_per_subreddit_mean
--     , (comment_unique_count / subreddit_unique_count) AS comments_per_subreddit_mean
-- FROM (
--     SELECT
--         COUNT(*)       AS row_count
--         , COUNT(DISTINCT post_id) AS post_unique_count
--         , COUNT(DISTINCT comment_id) AS comment_unique_count
--         , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count
--         , COUNT(DISTINCT user_id)   AS user_id_unique

--     FROM geo
-- );
-- Result, no filters
--  row_count 	 post_unique_count 	 comment_unique_count 	 subreddit_unique_count user_id_unique
--                       	                                        posts_per_subreddit_mean 	 comments_per_subreddit_mean
--  25,538,261 	 905,148 	 15,635,600 	 18,256 	 3,102,838 	 49.58 	 856.46


-- ===
-- Inspect dupes
-- Select suspected dupes to get IDS to investigate how to dedupe
SELECT
    *

FROM geo
WHERE 1=1
    -- AND row_num_comment_dupes >= 2
    AND comment_id IN (
        't1_hdc5mvq', 't1_hdgrwwt', 't1_hdgwp3h',
        't1_hdiilf6', 't1_hdm3xh8'
    )
ORDER BY subreddit_id, post_id
LIMIT 1000
;
