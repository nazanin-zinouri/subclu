-- Goal: pick comments for the subreddits we've already selected so that we can use comments
--  AND posts to create topic models (instead of only posts)

-- Turns out that the language_detect_v2 table doesn't have unique posts/comments
--    so we have to create an intermediary table to remove duplicates
-- Select POSTS + detected language for topic modeling
-- Ambassador program only started around 05-01 so try to get data that includes posts after that date
DECLARE start_date DATE DEFAULT '2021-04-01';
DECLARE end_date DATE DEFAULT '2021-05-19';

CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.comments_for_germany_topic_clustering_20210519`
PARTITION BY submit_date
AS (

WITH geo AS
(
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
    , gs.geo_country_code AS subreddit_geo_country_code
    , gs.combined_topic
    , gs.combined_topic_and_rating
    , gs.rating
    , gs.rating_version

    -- Text
    , sp.comment_body_text

FROM `reddit-employee-datasets.david_bermejo.subclu_selected_subs_20210519` AS gs
LEFT JOIN `data-prod-165221.cnc.successful_comments` AS sp
    ON gs.subreddit_name = sp.subreddit_name

WHERE sp.dt BETWEEN start_date AND end_date
    AND sp.removed = 0
),

tl_with_meta AS (
SELECT
    -- # counts check
    -- COUNT(DISTINCT(tl.id)) AS unique_post_ids
    -- , COUNT(DISTINCT(tl.subreddit_id)) AS unique_subreddits

    # Mostly Keys/IDs to join
    geo.uuid
    , geo.subreddit_name
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
    , geo.subreddit_geo_country_code
    , geo.combined_topic
    , geo.combined_topic_and_rating
    , geo.rating
    , geo.rating_version

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
) AS tl
INNER JOIN geo
    ON tl.subreddit_id = geo.subreddit_id
        AND tl.post_id = geo.post_id
        AND tl.thing_type = geo.noun
        AND tl.user_id = geo.user_id

-- Exclude some known bots
WHERE geo.user_id NOT IN ("t2_4kh8rj3k")
),

tl_unique_with_meta AS
(
SELECT * EXCEPT (row_num)
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

ORDER BY post_id
)


-- This is the de-duped table used for modeling
--   Comment this section out if you want to preview with queries below
SELECT
    * EXCEPT (uuid, post_url, comment_body_text, comment_body_text_for_translation)
    , comment_body_text

FROM (
    SELECT *
        , CHAR_LENGTH(comment_body_text) AS comment_text_len
        , array_length(regexp_extract_all(comment_body_text, r"\b\w+\b")) comment_text_word_count

    FROM tl_unique_with_meta
)
-- Preview table
# LIMIT 500

) -- close create table parens
;


-- Export data to google cloud storage (GCS)
# EXPORT DATA OPTIONS(
#   uri='gs://i18n-subreddit-clustering/comments/2021-05-19/*.parquet',
#   format='PARQUET',
#   overwrite=true
#   ) AS
# SELECT * EXCEPT (created_timestamp)
# FROM `reddit-employee-datasets.david_bermejo.comments_for_germany_topic_clustering_20210519`
# ;


-- Check counts in cnc post table
--  Use it to compare against content-language posts.
--    Expect number here to be higher than in content-language (b/c of inner join)
# SELECT
#     COUNT(*)                AS total_rows
#     , COUNT(DISTINCT uuid)  AS uuid_unique
#     , COUNT(DISTINCT post_id)  AS post_id_unique
#     , COUNT(DISTINCT subreddit_id)  AS subreddit_id_unique
#     , COUNT(DISTINCT user_id)  AS user_id_unique
# FROM geo
# ;


-- Count checks
# SELECT
#     COUNT(DISTINCT subreddit_id) AS subreddit_id_unique_count
#     , COUNT(DISTINCT post_id) AS post_id_unique_count
#     , COUNT(DISTINCT comment_id) AS comment_id_unique_count
#     , COUNT(*)        AS total_rows
# FROM tl_unique_with_meta
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


