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

DECLARE regex_cleanup_country_name_str STRING DEFAULT r" of Great Britain and Northern Ireland| of America|";


CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_posts_primary_language_check_20220122`
AS (
    WITH
    subreddits_above_threshold AS (
        SELECT *
        FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_candidates_posts_no_removed_20220122`
        WHERE 1=1
            AND users_l7 >= MIN_USERS_L7
            AND posts_not_removed_l28 >= MIN_POSTS_L28_NOT_REMOVED
    ),
    posts_with_language AS (
        -- We need row_num to remove duplicate posts
        SELECT * EXCEPT(post_thing_user_row_num)
        FROM (
            SELECT
                -- Rank by post-ID + user_id + thing_type (one user can post AND comment)
                ROW_NUMBER() OVER(
                    PARTITION BY post_id, user_id
                    ORDER BY created_timestamp ASC, weighted_probability DESC
                ) AS post_thing_user_row_num
                , lang.*

            FROM `data-prod-165221.language_detection.post_language_detection_cld3` AS lang
                -- Inner join so that we only count subreddits above threshold requirements
                INNER JOIN subreddits_above_threshold AS t
                    ON lang.subreddit_id = t.subreddit_id
            WHERE DATE(_PARTITIONTIME) BETWEEN start_date AND end_date
        )
        WHERE post_thing_user_row_num = 1
    ),
    posts_not_removed AS(
        SELECT * EXCEPT (row_num)
        FROM (
            SELECT
                -- Use row_number to get the latest edit as row=1 & drop duplicates
                ROW_NUMBER() OVER (
                    PARTITION BY post_id
                    ORDER BY endpoint_timestamp DESC, removal_timestamp DESC
                ) AS row_num
                , sp.*

            FROM `data-prod-165221.cnc.successful_posts` AS sp
                -- Inner join so that we only count subreddits above threshold requirements
                INNER JOIN subreddits_above_threshold AS t
                    ON sp.subreddit_id = t.subreddit_id

            WHERE sp.dt BETWEEN start_date AND end_date
                AND sp.removed = 0
        )
        -- Remove duplicates in successful_post table (multiple rows when a post is removed multiple times)
        WHERE 1=1
            AND row_num = 1
    ),

    -- For posts not removed, get language + text metadata (e.g., text len)
    pl_with_meta AS (
        SELECT
            -- Keys/IDs to join
            tl.subreddit_id
            , geo.subreddit_name
            , tl.post_id
            , tl.user_id


            # Language predictions + text meta
            -- Focus on weighted language b/c that's what I end up using anyway
            , tl.weighted_language
            , ll.language_name
            , ll.language_name_top_only
            , ll.language_in_use_multilingual
            , tl.weighted_probability AS weighted_language_probability
            , plo.language_preference AS post_language_preference
            , tl.geolocation_country_code
            -- Regex replace long names w/o a comma
            , REGEXP_REPLACE(
                SPLIT(cm.country_name, ', ')[OFFSET(0)],
                regex_cleanup_country_name_str, ""
            ) AS geolocation_country_name
            , cm.region AS geolocation_geo_region
            , CHAR_LENGTH(tl.text) AS text_len
            , array_length(regexp_extract_all(tl.text, r"\b\w+\b")) text_word_count

            -- Other Metadata
            , tl.created_timestamp
            # , geo.endpoint_timestamp
            , geo.submit_date
            , geo.removed
            , geo.upvotes AS upvotes
            , plo.upvotes    AS upvotes_lookup
            , plo.downvotes  AS downvotes_lookup
            , (plo.upvotes - plo.downvotes) AS net_upvotes_lookup
            , plo.neutered
            , plo.verdict
            , plo.content_category

            , geo.comments
            , geo.successful
            , geo.app_name
            , geo.post_type
            , geo.post_nsfw
            -- , geo.post_url

            -- Text -- for now, only keep text meta, not text itself b/c that can make the table huge
            -- , plo.flair_text
            -- , tl.text

        FROM posts_with_language AS tl
        INNER JOIN posts_not_removed AS geo
            ON tl.subreddit_id = geo.subreddit_id
                AND tl.post_id = geo.post_id
                AND tl.user_id = geo.user_id
        INNER JOIN (
            SELECT *
            FROM `data-prod-165221.ds_v2_postgres_tables.post_lookup`
            WHERE DATE(_PARTITIONTIME) = end_date
        ) AS plo
            ON tl.subreddit_id = plo.subreddit_id AND tl.post_id = plo.post_id
                AND tl.user_id = plo.author_id
        LEFT JOIN `reddit-employee-datasets.david_bermejo.language_detection_code_to_name_lookup_cld3` AS ll
            ON tl.weighted_language = ll.language_code

        LEFT JOIN `data-prod-165221.ds_utility_tables.countrycode_region_mapping` AS cm
            ON tl.geolocation_country_code = LOWER(cm.country_code)

    )

SELECT *
FROM pl_with_meta
ORDER BY subreddit_id ASC, upvotes DESC

);  -- Close CREATE TABLE parens


-- Count check - final table
--  Each row should be a unique POST-ID
-- SELECT
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT subreddit_id) AS subreddit_id_count_unique
--     , COUNT(DISTINCT post_id) AS post_id_count_unique
-- FROM pl_with_meta
-- ;

-- Count checks posts not removed
-- SELECT
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT subreddit_id) AS subreddit_id_count_unique
--     , COUNT(DISTINCT post_id) AS post_id_count_unique
-- FROM posts_not_removed
-- ;
