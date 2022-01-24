-- Select posts for subreddits that may qualify as relevant b/c of primary language

-- Turns out that the language_detect_v2 table doesn't have unique posts/comments
--    so we have to create an intermediary table to remove duplicates
-- Select COMMENTS + detected language for topic modeling

-- Update checklist:
-- * start date
-- * end date
-- * name of table with subreddits pre-computed post count (not removed) & users L7
-- * name of new created table (posts)
--   * e.g., subclu_posts_top_no_geo_202XXXXX

-- For now, don't include OCR text b/c we don't run that through language detection anyway.

-- Create new POSTS table for clean language detection
DECLARE END_DATE DATE DEFAULT '2022-01-22';
DECLARE START_DATE DATE DEFAULT END_DATE - 29;

-- Minimums for each subreddit to check
--  if we go too far below 45 users and 4 posts, we risk getting dead subreddits
DECLARE MIN_USERS_L7 NUMERIC DEFAULT 45;
DECLARE MIN_POSTS_L28_NOT_REMOVED NUMERIC DEFAULT 4;


CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_posts_primary_language_check_20220122`
PARTITION BY submit_date
AS (
    WITH
    subreddits_above_threshold AS (
        SELECT
        FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_candidates_posts_no_removed_20220122`
        WHERE

    ),
    posts_with_language AS (
        SELECT
            -- Rank by post-ID + user_id + thing_type (one user can post AND comment)
            ROW_NUMBER() OVER(
                PARTITION BY post_id, user_id
                ORDER BY created_timestamp ASC, weighted_probability DESC
            ) AS post_thing_user_row_num
            , *

        FROM `data-prod-165221.language_detection.post_language_detection_cld3`
        WHERE DATE(_PARTITIONTIME) BETWEEN start_date AND end_date

    ),
    posts_not_removed AS(
        SELECT
            -- Use row_number to get the latest edit as row=1
            ROW_NUMBER() OVER (
                PARTITION BY post_id
                ORDER BY endpoint_timestamp DESC, removal_timestamp DESC
            ) AS row_num
            , *

        FROM `data-prod-165221.cnc.successful_posts` AS sp

        WHERE sp.dt BETWEEN start_date AND end_date
            AND sp.removed = 0
            -- Remove duplicates in successful_post table (multiple rows when a post is removed multiple times)
            -- AND sp.row_num = 1
    ),
