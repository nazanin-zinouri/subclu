-- Goal: pick comments for the subreddits we've already selected so that we can use comments
--  AND posts to create topic models (instead of only posts)

-- Turns out that the language_detect_v2 table doesn't have unique posts/comments
--    so we have to create an intermediary table to remove duplicates
-- Select COMMENTS + detected language for topic modeling

-- Update checklist:
-- * start date
-- * end date
-- * min comment len
-- * max comments per sub
-- * name of new created table (update date)
-- * table with latest selected posts (e.g., subclu_posts_top_no_geo_20211214)
-- * name of newly created table for exporting
-- * new GCS folder for new table

-- Select COMMENTS for v0.4.1 topic clustering
-- Query complete (3 min 41 sec elapsed, 252.4 GB processed)
DECLARE start_date DATE DEFAULT '2021-10-01';
DECLARE end_date DATE DEFAULT '2021-12-15';
DECLARE MIN_COMMENT_LEN NUMERIC DEFAULT 8;
DECLARE MAX_COMMENTS_PER_POST NUMERIC DEFAULT 9;


CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_comments_top_no_geo_20211214`
PARTITION BY submit_date
AS (

WITH
    selected_posts AS (
        -- Start with selected posts to reduce orphan comments
        SELECT * FROM `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20211214`
        -- Use where-clause for testing
        -- WHERE 1=1
        --     AND (
        --         geo_relevant_subreddit_all = TRUE
        --     )
    ),
    geo AS (
        SELECT
            # Keys & IDS
            gs.subreddit_name
            , sp.subreddit_id
            , sp.post_id
            , sp.comment_id
            , sp.user_id
            , sp.uuid

            # Meta content
            , sp.submit_date
            , sp.endpoint_timestamp
            , sp.noun
            , sp.removed
            , sp.upvotes
            , sp.successful
            , sp.app_name
            , sp.post_type
            , sp.post_url
            , sp.post_nsfw

            -- Meta about subreddit
            # , gs.combined_topic
            # , gs.rating
            # , gs.rating_version

            -- Text
            , sp.comment_body_text

            , ROW_NUMBER() OVER (
                PARTITION BY sp.comment_id
                ORDER BY endpoint_timestamp DESC, removal_timestamp DESC
            ) AS row_num_comment_dupes

        -- Start with selected posts to reduce orphan comments
        FROM selected_posts AS gs
        LEFT JOIN `data-prod-165221.cnc.successful_comments` AS sp
            ON gs.subreddit_name = sp.subreddit_name
                AND gs.post_id = sp.post_id

        WHERE sp.dt BETWEEN start_date AND end_date
            AND sp.removed = 0
    ),

    -- TL = thing_language. In this case thing=comment
    tl_with_meta AS (
        SELECT
            -- # counts check
            -- COUNT(DISTINCT(tl.id)) AS unique_post_ids
            -- , COUNT(DISTINCT(tl.subreddit_id)) AS unique_subreddits

            # Mostly Keys/IDs to join
            geo.subreddit_name
            , tl.subreddit_id
            , tl.post_id
            , geo.comment_id
            , tl.user_id
            , tl.thing_type

            # Metadata
            , tl.created_timestamp
            # , geo.endpoint_timestamp
            , geo.submit_date
            , geo.removed
            , geo.upvotes
            , geo.successful
            , geo.app_name
            , geo.post_type
            , geo.post_nsfw
            -- , geo.post_url
            , tl.geolocation_country_code

            -- Meta about subreddit

            -- Language predictions
            , tl.language
            , tl.probability
            , tl.weighted_language
            , tl.weighted_language_probability

            # Text
            #  Wait to do relatively expensive string manipulation until AFTER removing duplicates
            # , CHAR_LENGTH(tl.text) AS text_len
            # , array_length(regexp_extract_all(tl.text, r"\b\w+\b")) text_word_count_estimate
            -- , tl.text AS comment_body_text_for_lang_detection
            , geo.comment_body_text

            # Metadata to add separately?
            # , tl.possible_alternatives  # unwieldy field, analyze later
            # , tl.toxicity

        FROM (
            SELECT *
            FROM `reddit-protected-data.language_detection.comment_language_v2`
            WHERE _PARTITIONTIME BETWEEN TIMESTAMP(start_date) AND TIMESTAMP(end_date)
                AND thing_type = 'comment'
                -- AND id !=
        ) AS tl
        INNER JOIN (
            SELECT * FROM geo
            WHERE row_num_comment_dupes = 1
        ) AS geo
            ON tl.subreddit_id = geo.subreddit_id
                AND tl.post_id = geo.post_id
                AND tl.thing_type = geo.noun
                AND tl.user_id = geo.user_id
                AND tl.id = geo.comment_id

        -- Exclude some known bots
        WHERE geo.user_id NOT IN ("t2_4kh8rj3k")
    ),

    tl_unique_with_meta AS
        (
        SELECT
            * EXCEPT (row_num_tl_dupes)
            , CHAR_LENGTH(comment_body_text) AS comment_text_len
        FROM (
            SELECT
                *
                , ROW_NUMBER() OVER (
                    PARTITION BY post_id, subreddit_id, user_id
                    ORDER BY created_timestamp desc
                ) row_num_tl_dupes
            FROM tl_with_meta
        )
        WHERE row_num_tl_dupes = 1
    ),

    -- We can't rely on comments_count table to be right because it may not be synced at the same date,
    --  so better to always rank and limit comments
    comments_ranked AS (
        SELECT
            tl.subreddit_id
            , tl.post_id
            , tl.comment_id

            , ROW_NUMBER() OVER (
                PARTITION BY tl.post_id
                ORDER BY tl.upvotes DESC, tl.comment_text_len DESC
            ) comment_rank_by_post_id

        FROM (
            SELECT * FROM tl_unique_with_meta
            -- Filter to keep only comments that are long enough
            WHERE comment_text_len >= MIN_COMMENT_LEN
        ) AS tl
    ),

    selected_comments AS (
        -- Pick only comments that meet ranking/filtering criteria
        SELECT
            subreddit_id
            , post_id
            , comment_id
        FROM comments_ranked AS cr
        -- filter out comments above threshold
        WHERE comment_rank_by_post_id <= MAX_COMMENTS_PER_POST
    ),

    tl_unique_with_meta_top_comments AS (
        SELECT
            tl.* EXCEPT (comment_body_text)
            , array_length(regexp_extract_all(comment_body_text, r"\b\w+\b")) comment_text_word_count
            , comment_body_text

        FROM selected_comments AS sc
        INNER JOIN tl_unique_with_meta AS tl
            ON sc.subreddit_id = tl.subreddit_id
                AND sc.post_id = tl.post_id
                AND sc.comment_id = tl.comment_id
    )

    -- This is the final table used for modeling
    --   Comment this section out if you want to preview with queries below
    SELECT * FROM  tl_unique_with_meta_top_comments
); -- close create table parens


-- ==============================
-- check counts AFTER creating COMMENTS table
-- ===
-- check counts AFTER creating COMMENTS table
-- SELECT
--     *
--     , (post_unique_count / subreddit_unique_count) AS posts_per_subreddit_mean
--     , (comment_unique_count / subreddit_unique_count) AS comments_per_subreddit_mean
--     , (comment_unique_count / post_unique_count) AS comments_per_post_mean

-- FROM (
--     SELECT
--         COUNT(*)       AS row_count
--         , COUNT(DISTINCT post_id) AS post_unique_count
--         , COUNT(DISTINCT comment_id) AS comment_unique_count
--         , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count

--     FROM `reddit-employee-datasets.david_bermejo.subclu_comments_top_no_geo_20211214`
-- );
-- RESULT, numbers match the numbers before materializing table
--   Query complete (8.9 sec elapsed, 1.2 GB processed)
--   New: Query complete (8.9 sec elapsed, 1.7 GB processed)
--  row_count 	 post_unique_count 	 comment_unique_count 	 subreddit_unique_count 	 posts_per_subreddit_mean 	 comments_per_subreddit_mean 	 comments_per_post_mean
--  39,901,968 	 7,038,219 	 39,901,968 	 19,020 	 370.0 	 2,097.9 	 5.7
-- NEW row_count	post_unique_count	comment_unique_count	subreddit_unique_count
--                                                       posts_per_subreddit_mean	comments_per_subreddit_mean	comments_per_post_mean
--  54,407,324 	 11,155,334  54,407,324 	 48,835 	 228.4	 1,114.1	4.9


-- ==============================
-- Export data to google cloud storage (GCS)
-- ===
-- Update checklist:
--   - URI folder location
--   - source table name
-- Export new comments for topic modeling
-- EXPORT DATA OPTIONS (
--     uri='gs://i18n-subreddit-clustering/comments/top/2021-12-14/*.parquet',
--     format='PARQUET',
--     overwrite=true
-- ) AS
-- SELECT * EXCEPT (created_timestamp)
-- FROM `reddit-employee-datasets.david_bermejo.subclu_comments_top_no_geo_20211214`
-- ORDER BY subreddit_name ASC
-- ;


-- ==============================
-- Count check, tables side by side
-- ===
-- SELECT
--     (SELECT COUNT(*) FROM geo_subs_custom_raw) AS geo_subs_custom_raw_count
--     , (SELECT COUNT(*) FROM subs_selected_by_geo) AS subs_selected_by_geo_count
--     , (SELECT COUNT(*) FROM ambassador_subs) AS ambassador_subs_count  -- Expect 173+
    -- , (SELECT COUNT(*) FROM subs_selected_by_activity) AS subs_selected_by_activity_count_rows
    -- , (SELECT COUNT(*) FROM selected_subs_base) AS selected_subs_BASE_count_rows
    -- , (SELECT COUNT(DISTINCT subreddit_id) FROM selected_subs_base) AS selected_subs_BASE_unique_sub_ids_count
    -- , (SELECT COUNT(*) FROM selected_subs) AS selected_subs_count_rows
    -- , (SELECT COUNT(DISTINCT subreddit_id) FROM selected_subs) AS selected_subs_unique_sub_ids_count
--     , (SELECT COUNT(*) FROM final_table) AS final_table_rows_count
--     , (SELECT COUNT(DISTINCT subreddit_id) FROM final_table) AS final_table_sub_id_unique_count
-- ;

-- ====================================
-- Count checks specific tables
-- ===
-- Check uniques in tl_unique_with_meta
-- SELECT
--     *
--     , (post_unique_count / subreddit_unique_count) AS posts_per_subreddit_mean
--     , (comment_unique_count / subreddit_unique_count) AS comments_per_subreddit_mean
--     , (comment_unique_count / post_unique_count) AS comments_per_post_mean

-- FROM (
--     SELECT
--         COUNT(*)       AS row_count
--         , COUNT(DISTINCT post_id) AS post_unique_count
--         , COUNT(DISTINCT comment_id) AS comment_unique_count
--         , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count
--         -- , COUNT(DISTINCT user_id)   AS user_id_unique

--     FROM tl_unique_with_meta
-- );
-- RESULT, only for geo-relevant (including ambassador) subreddits
--  row_count 	 post_unique_count 	 comment_unique_count 	 subreddit_unique_count 	 posts_per_subreddit_mean 	 comments_per_subreddit_mean 	 comments_per_post_mean
--  9,831,770 	 652,835 	 9,831,770 	 4,074 	 160.2 	 2,413.3 	 15.1



-- Counts for final table (before materializing it)
-- SELECT
--     *
--     , (post_unique_count / subreddit_unique_count) AS posts_per_subreddit_mean
--     , (comment_unique_count / subreddit_unique_count) AS comments_per_subreddit_mean
--     , (comment_unique_count / post_unique_count) AS comments_per_post_mean

-- FROM (
--     SELECT
--         COUNT(*)       AS row_count
--         , COUNT(DISTINCT post_id) AS post_unique_count
--         , COUNT(DISTINCT comment_id) AS comment_unique_count
--         , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count

--     FROM tl_unique_with_meta_top_comments
-- );
-- RESULTS, only geo-relevant/ambassador subs:
--    MIN_COMMENT_LEN NUMERIC = 8
--    MAX_COMMENTS_PER_POST = 9
--  row_count 	 post_unique_count 	 comment_unique_count 	 subreddit_unique_count 	 posts_per_subreddit_mean 	 comments_per_subreddit_mean 	 comments_per_post_mean
--  3,566,450 	 652,695 	 3,566,450 	 4,073 	 160.2 	 875.6 	 5.5
-- RESULTS, all v0.4.0 subreddits. ~ 1:12 elapsed time
--  row_count 	 post_unique_count 	 comment_unique_count 	 subreddit_unique_count 	 posts_per_subreddit_mean 	 comments_per_subreddit_mean 	 comments_per_post_mean
--  39,901,968 	 7,038,219 	 39,901,968 	 19,020 	 370.0 	 2,097.9 	 5.7


-- Counts for selected comments (this table should have already filtered and ranked the comments!)
-- SELECT
--     *
--     , (post_unique_count / subreddit_unique_count) AS posts_per_subreddit_mean
--     , (comment_unique_count / subreddit_unique_count) AS comments_per_subreddit_mean
--     , (comment_unique_count / post_unique_count) AS comments_per_post_mean

-- FROM (
--     SELECT
--         COUNT(*)       AS row_count
--         , COUNT(DISTINCT post_id) AS post_unique_count
--         , COUNT(DISTINCT comment_id) AS comment_unique_count
--         , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count

--     FROM selected_comments
-- );
-- RESULT with:
--    MIN_COMMENT_LEN NUMERIC = 10
--    MAX_COMMENTS_PER_POST = 8
--  row_count 	 post_unique_count 	 comment_unique_count 	 subreddit_unique_count 	 posts_per_subreddit_mean 	 comments_per_subreddit_mean 	 comments_per_post_mean
--  3,337,142 	 652,663 	 3,337,142 	 4,073 	 160.2 	 819.3 	 5.1



-- Check only comments for posts below comment-count threshold
-- SELECT
--     *
--     , (post_unique_count / subreddit_unique_count) AS posts_per_subreddit_mean
--     , (comment_unique_count / subreddit_unique_count) AS comments_per_subreddit_mean
--     , (comment_unique_count / post_unique_count) AS comments_per_post_mean

-- FROM (
--     SELECT
--         COUNT(*)       AS row_count
--         , COUNT(DISTINCT post_id) AS post_unique_count
--         , COUNT(DISTINCT comment_id) AS comment_unique_count
--         , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count

--     FROM comments_from_posts_below_threshold
-- );
-- RESULT - coments from posts below threshold (8 comments per post)
--  row_count 	 post_unique_count 	 comment_unique_count 	 subreddit_unique_count 	 posts_per_subreddit_mean 	 comments_per_subreddit_mean 	 comments_per_post_mean
--  1,448,229 	 406,595 	 1,448,229 	 4,068 	 99.9 	 356.0 	 3.6


-- Check comments for posts ABOVE comment-count threshold
-- SELECT
--     *
--     , (post_unique_count / subreddit_unique_count) AS posts_per_subreddit_mean
--     , (comment_unique_count / subreddit_unique_count) AS comments_per_subreddit_mean
--     , (comment_unique_count / post_unique_count) AS comments_per_post_mean

-- FROM (
--     SELECT
--         COUNT(*)       AS row_count
--         , COUNT(DISTINCT post_id) AS post_unique_count
--         , COUNT(DISTINCT comment_id) AS comment_unique_count
--         , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count

--     FROM comments_from_posts_above_threshold
--     WHERE 1=1
--         AND comment_rank_by_post_id <= MAX_COMMENTS_PER_POST
-- );
-- RESULTS w/o filtering max comments per post
--  row_count 	 post_unique_count 	 comment_unique_count 	 subreddit_unique_count 	 posts_per_subreddit_mean 	 comments_per_subreddit_mean 	 comments_per_post_mean
--  7,900,663 	 246,068 	 7,900,663 	 2,373 	 103.7 	 3,329.4 	 32.1
--  looks like we're only losing ~400k comments because they are too short (9.8 million - 1.4 - 7.9)

-- RESULTS after limit to max comments per post. (mean per post should be max/limit, but we'll drop a few so maybe a bit lower than it)
--  row_count 	 post_unique_count 	 comment_unique_count 	 subreddit_unique_count 	 posts_per_subreddit_mean 	 comments_per_subreddit_mean 	 comments_per_post_mean
--  1,888,913 	 246,068 	 1,888,913 	 2,373 	 103.7 	 796.0 	 7.7



-- Check counts in cnc post table
--  Use it to compare against content-language posts.
--    Expect number here to be higher than in content-language (b/c of inner join)
-- SELECT
--     COUNT(*)                AS total_rows
--     , COUNT(DISTINCT uuid)  AS uuid_unique
--     , COUNT(DISTINCT comment_id)  AS comment_id_unique
--     , COUNT(DISTINCT post_id)  AS post_id_unique
--     , COUNT(DISTINCT subreddit_id)  AS subreddit_id_unique
--     , COUNT(DISTINCT user_id)  AS user_id_unique
-- FROM geo
-- ;
