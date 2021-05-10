

-- Turns out that the language_detect_v2 table doesn't have unique posts/comments
--    so I have to create an intermediary table to remove duplicates
-- Select POSTS + detected language for topic modeling
-- Ambassador program only started aroud 05-01 so try to get data that includes posts after that date
DECLARE start_date DATE DEFAULT '2021-04-01';
DECLARE end_date DATE DEFAULT '2021-05-08';

CREATE TABLE `reddit-employee-datasets.david_bermejo.posts_for_germany_topic_clustering_20210510`
PARTITION BY submit_date
AS (

WITH geo AS
(
SELECT
    # Keys & IDS
    gsubs.subreddit_name
    , sp.subreddit_id
    , sp.post_id
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

FROM `reddit-employee-datasets.david_bermejo.subclu_selected_subs_20210506` AS gsubs
LEFT JOIN `data-prod-165221.cnc.successful_posts` AS sp
    ON gsubs.subreddit_name = sp.subreddit_name

WHERE sp.dt BETWEEN start_date AND end_date
    AND sp.removed = 0
),

tl_with_meta AS (
SELECT
    # counts
    # COUNT(DISTINCT(tl.id)) AS unique_post_ids
    # , COUNT(DISTINCT(tl.subreddit_id)) AS unique_subreddits

    # Mostly Keys/IDs to join
    # tl.id. # this one's not that helpful because it's actually not unique
    geo.uuid
    , geo.subreddit_name
    , tl.subreddit_id
    , tl.post_id
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

    # Language predictions
    , tl.language
    , tl.probability
    , tl.weighted_language
    , tl.weighted_language_probability

    # Text
    , CHAR_LENGTH(tl.text) AS text_len
    , array_length(regexp_extract_all(tl.text, r"\b\w+\b")) text_word_count_estimate
    , tl.text

    # Metadata to add separately?
    # , tl.possible_alternatives  # unwieldy field, analyze later
    # , tl.toxicity

FROM (
    SELECT *
    FROM `reddit-protected-data.language_detection.comment_language_v2`
    WHERE _PARTITIONTIME BETWEEN TIMESTAMP(start_date) AND TIMESTAMP(end_date)
        AND thing_type = 'post'
) AS tl
INNER JOIN geo
    ON tl.subreddit_id = geo.subreddit_id
        AND tl.post_id = geo.post_id
        AND tl.thing_type = geo.noun
        AND tl.user_id = geo.user_id

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

-- Comment this section out if you want to preview with queries below
SELECT *
FROM tl_unique_with_meta
)
;

-- Export data to google cloud storage (GCS)
-- EXPORT DATA OPTIONS(
--   uri='gs://i18n-subreddit-clustering/posts/2021-05-10/*.parquet',
--   format='PARQUET',
--   overwrite=true
--   ) AS
--
-- SELECT * EXCEPT (uuid, created_timestamp)
-- FROM `reddit-employee-datasets.david_bermejo.posts_for_germany_topic_clustering_20210510`
-- ;


-- Preview table
-- SELECT * EXCEPT (uuid, created_timestamp)
-- FROM tl_unique_with_meta
-- LIMIT 100
-- ;


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


-- Count totals v. unique AFTER joining with language-detection page
# SELECT
#     COUNT(*)                AS total_rows
#     , COUNT(DISTINCT uuid)  AS uuid_unique
#     , COUNT(DISTINCT post_id)  AS post_id_unique
#     , COUNT(DISTINCT subreddit_id)  AS subreddit_id_unique
#     , COUNT(DISTINCT user_id)  AS user_id_unique
# FROM tl_unique_with_meta
# ;
