-- Goal: pick comments for the subreddits we've already selected so that we can use comments
--  AND posts to create topic models (instead of only posts)

-- Turns out that the language_detect_v2 table doesn't have unique posts/comments
--    so we have to create an intermediary table to remove duplicates
-- Select COMMENTS + detected language for topic modeling

-- Update checklist:
-- * start date
-- * end date
-- * max posts per sub
-- * name of new created table (update date)
-- * table with latest selected subreddits (e.g., subclu_subreddits_top_no_geo_20210709)
-- * name of newly created table for exporting
-- * new GCS folder for new table
DECLARE start_date DATE DEFAULT '2021-06-01';
DECLARE end_date DATE DEFAULT '2021-07-13';
DECLARE MIN_COMMENT_LEN NUMERIC DEFAULT 11;
DECLARE MAX_COMMENTS_PER_POST NUMERIC DEFAULT 8;


CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_comments_top_no_geo_20210716`
PARTITION BY submit_date
AS (

WITH geo AS (
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
    , gs.combined_topic_and_rating
    # , gs.rating
    # , gs.rating_version

    -- Text
    , sp.comment_body_text

-- Start with selected posts to reduce orphan comments
FROM `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20210716` AS gs
-- FROM (
--     SELECT *
--     FROM `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20210716`
--     LIMIT 20
-- ) AS gs

LEFT JOIN `data-prod-165221.cnc.successful_comments` AS sp
    ON gs.subreddit_name = sp.subreddit_name
        AND gs.post_id = sp.post_id

WHERE sp.dt BETWEEN start_date AND end_date
    AND sp.removed = 0
),

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
    , geo.post_url
    , tl.geolocation_country_code

    -- Meta about subreddit
    , geo.combined_topic_and_rating

    # Language predictions
    , tl.language
    , tl.probability
    , tl.weighted_language
    , tl.weighted_language_probability

    # Text
    #  Wait to do relatively expensive string manipulation until AFTER removing duplicates
    # , CHAR_LENGTH(tl.text) AS text_len
    # , array_length(regexp_extract_all(tl.text, r"\b\w+\b")) text_word_count_estimate
    , tl.text AS comment_body_text_for_translation
    , geo.comment_body_text

    # Metadata to add separately?
    # , tl.possible_alternatives  # unwieldy field, analyze later
    # , tl.toxicity

FROM (
    SELECT *
    FROM `reddit-protected-data.language_detection.comment_language_v2`
    WHERE _PARTITIONTIME BETWEEN TIMESTAMP(start_date) AND TIMESTAMP(end_date)
        AND thing_type = 'comment'
        AND id !=
) AS tl
INNER JOIN geo
    ON tl.subreddit_id = geo.subreddit_id
        AND tl.post_id = geo.post_id
        AND tl.thing_type = geo.noun
        AND tl.user_id = geo.user_id
        -- TODO(djb): add to future queries, it might help remove dupes?
        -- AND tl.id = geo.comment_id

-- Exclude some known bots
WHERE geo.user_id NOT IN ("t2_4kh8rj3k")
),

tl_unique_with_meta AS
(
SELECT
    * EXCEPT (row_num)
    , CHAR_LENGTH(comment_body_text) AS comment_text_len
FROM (
    SELECT
        *
        , ROW_NUMBER() OVER (
            PARTITION BY post_id, subreddit_id, user_id
            ORDER BY created_timestamp desc
        ) row_num
    FROM tl_with_meta
)
WHERE row_num = 1
),

tl_unique_with_meta_top_comments AS (
-- Instead of picking all comments, limit to top N comments per post
SELECT
    tl.*

FROM (
    SELECT
        ROW_NUMBER() OVER(PARTITION BY post_id ORDER BY upvotes DESC) AS rank_comment_in_post
        , *
    FROM tl_unique_with_meta
    WHERE comment_text_len >= MIN_COMMENT_LEN
) AS tl

WHERE tl.rank_comment_in_post <= MAX_COMMENTS_PER_POST

)


-- This is the de-duped table used for modeling
--   Comment this section out if you want to preview with queries below
SELECT
    * EXCEPT (post_url, comment_body_text, comment_body_text_for_translation)
    , comment_body_text

FROM (
    SELECT *
        , array_length(regexp_extract_all(comment_body_text, r"\b\w+\b")) comment_text_word_count

    FROM tl_unique_with_meta
)
) -- close create table parens
;

-- Counts for AFTER dropping dupes from language detection table
# SELECT
#     COUNT(DISTINCT subreddit_id) AS subreddit_id_unique_count
#     , COUNT(DISTINCT post_id) AS post_id_unique_count
#     , COUNT(DISTINCT comment_id) AS comment_id_unique_count
#     , COUNT(*)        AS total_rows
# FROM tl_unique_with_meta
# ;

-- Counts AFTER selecting only top comments
# SELECT
#     COUNT(DISTINCT subreddit_id) AS subreddit_id_unique_count
#     , COUNT(DISTINCT post_id) AS post_id_unique_count
#     , COUNT(DISTINCT comment_id) AS comment_id_unique_count
#     , COUNT(*)        AS total_rows
# FROM tl_unique_with_meta_top_comments
# ;
-- when 20 posts, 10 char min, 10 comments / post:
-- subreddit_id_unique_count	post_id_unique_count	comment_id_unique_count total_rows
-- 12 	                        14 	                    70 	                    70

-- when 20 posts, 11 char min, 9 comments / post:
-- subreddit_id_unique_count	post_id_unique_count	comment_id_unique_count total_rows
-- 12 	                        14 	                    41 	                    41


-- Check counts in cnc post table
--  Use it to compare against content-language posts.
--    Expect number here to be higher than in content-language (b/c of inner join)
# SELECT
#     COUNT(*)                AS total_rows
#     , COUNT(DISTINCT uuid)  AS uuid_unique
#     , COUNT(DISTINCT comment_id)  AS comment_id_unique
#     , COUNT(DISTINCT post_id)  AS post_id_unique
#     , COUNT(DISTINCT subreddit_id)  AS subreddit_id_unique
#     , COUNT(DISTINCT user_id)  AS user_id_unique
# FROM geo
# ;


-- check counts AFTER creating the table
# SELECT
#     COUNT(*)                AS total_rows
#     , COUNT(DISTINCT post_id)  AS post_id_unique
#     , COUNT(DISTINCT subreddit_id)  AS subreddit_id_unique
#     , COUNT(DISTINCT user_id)  AS user_id_unique
# FROM `reddit-employee-datasets.david_bermejo.subclu_comments_top_no_geo_20210716`
# ;


-- Export data to google cloud storage (GCS)
# EXPORT DATA OPTIONS(
#   uri='gs://i18n-subreddit-clustering/comments/top/2021-07-16/*.parquet',
#   format='PARQUET',
#   overwrite=true
#   ) AS
# SELECT * EXCEPT (created_timestamp)
# FROM `reddit-employee-datasets.david_bermejo.subclu_comments_top_no_geo_20210716`
# ;



-- Find users with lots of posts, as proxy to investigate bots
# SELECT
#     user_id
#     , COUNT(DISTINCT comment_id) AS comment_id_unique_count
#     , COUNT(DISTINCT subreddit_id) AS subreddit_id_unique_count
#     , COUNT(DISTINCT post_id) AS post_id_unique_count

# FROM tl_unique_with_meta
# GROUP BY user_id
# ORDER BY comment_id_unique_count DESC

# LIMIT 100
# ;


