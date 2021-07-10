-- noinspection SqlNoDataSourceInspectionForFile

-- Turns out that the language_detect_v2 table doesn't have unique posts/comments
--    so I have to create an intermediary table to remove duplicates
-- Select POSTS + detected language for topic modeling
-- Ambassador program only started around 05-01 so try to get data that includes posts after that date
DECLARE start_date DATE DEFAULT '2021-06-01';
DECLARE end_date DATE DEFAULT '2021-07-07';
DECLARE MAX_POSTS_PER_SUB NUMERIC DEFAULT 1000;


CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20210709`
PARTITION BY submit_date
AS (

WITH geo AS
(
SELECT
    # Keys & IDS
    gs.subreddit_name
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
    , sp.comments
    # , (sp.upvotes - sp.downvotes) AS net_upvotes
    , sp.successful
    , sp.app_name
    , sp.post_type
    , sp.post_url
    , sp.post_nsfw

    -- Meta about subreddit
    , gs.combined_topic
    , gs.combined_topic_and_rating
    , gs.rating
    , gs.rating_version

FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20210709` AS gs
LEFT JOIN `data-prod-165221.cnc.successful_posts` AS sp
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
    geo.subreddit_name
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
    , geo.comments
    # , geo.net_upvotes
    , geo.successful
    , geo.app_name
    , geo.post_type
    , geo.post_nsfw
    , geo.post_url
    , tl.geolocation_country_code

    -- Meta about subreddit
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

),

tl_unique_with_meta_top_posts AS (
SELECT
    tl.* EXCEPT(rank_post_in_sub)

FROM (
    SELECT
        *
        , ROW_NUMBER() OVER(PARTITION BY subreddit_name ORDER BY upvotes DESC, comments DESC) AS rank_post_in_sub
    FROM tl_unique_with_meta
) AS tl

WHERE
    tl.rank_post_in_sub <= MAX_POSTS_PER_SUB
)
-- post_screenviews AS (
--     SELECT
--       pdr.post_id
--       , pdr.screenviews
--       , pdr.upvotes
--       , sp.upvotes AS sp_upvotes
--       , pdr.downvotes
--       -- , sp.downvotes
--       , pdr.net_upvotes
--       , LOWER(pdr.subreddit_name) AS subreddit_name
--       , pdr.pt
--       , pdr.rank_post_date
--       , ROW_NUMBER() OVER(PARTITION BY sp.subreddit_name ORDER BY pdr.net_upvotes DESC, pdr.screenviews DESC) AS rank_post_in_sub
--
--     -- Use sub-selection to keep only the latest update for a given post...
--     -- HOWEVER, it looks like each day is independent of each other, so to get totals, we might need to SUM() screenviews?
--     FROM (
--       SELECT
--         *
--         , _PARTITIONTIME AS pt
--         , (upvotes - downvotes) AS net_upvotes
--         , ROW_NUMBER() OVER(PARTITION BY post_id ORDER BY _PARTITIONTIME ASC) AS rank_post_date
--       FROM `data-prod-165221.ds_v2_aggregate_tables.post_daily_reporting`
--       WHERE _PARTITIONTIME BETWEEN TIMESTAMP(start_date) AND TIMESTAMP(end_date)
--     ) AS pdr
--
--     LEFT JOIN `data-prod-165221.cnc.successful_posts` AS sp
--         ON LOWER(pdr.subreddit_name) = sp.subreddit_name
--             AND pdr.post_id = sp.post_id
--
--     WHERE 1=1
--       AND rank_post_date = 1
--       AND sp.dt BETWEEN start_date AND end_date
--       AND sp.removed = 0
--
--       # test using only some subs
--         AND LOWER(sp.subreddit_name) IN ("de", "ich_iel", "bundesliga", "adidasgirls")
-- )



-- This is the de-duped table used for modeling
--   Comment this section out if you want to preview with queries below
SELECT
    * EXCEPT (text)
    , text

FROM (
    SELECT *
    -- Create a new column that cleans up the post_url col for embeddings
    -- Only create it if the link isn't posting to itself (otherwise we're leaking data about the subreddit)
    , CHAR_LENGTH(text) AS text_len
    , array_length(regexp_extract_all(text, r"\b\w+\b")) text_word_count
    , CASE
        WHEN REGEXP_INSTR(
            post_url,
            ARRAY_REVERSE(SPLIT(post_id, "_"))[SAFE_OFFSET(0)]
            ) > 0 THEN NULL
        ELSE TRIM(REGEXP_REPLACE(REGEXP_REPLACE(post_url, "https://|www.|/r/|.html", ""), r"/|-|_|\?", " "))
        END AS post_url_for_embeddings

    FROM tl_unique_with_meta_top_posts
)

)  -- close create table parens
;


-- Count totals v. unique BEFORE CREATING TABLE, and AFTER joining with language-detection page
# SELECT
#     COUNT(*)                AS total_rows
#     , COUNT(DISTINCT post_id)  AS post_id_unique
#     , COUNT(DISTINCT subreddit_id)  AS subreddit_id_unique
#     , COUNT(DISTINCT user_id)  AS user_id_unique
# FROM tl_unique_with_meta
# ;
-- # Expected counts for 04/01 to 06/14: ~262k posts

-- Count totals v. unique BEFORE CREATING TABLE, and AFTER joining with language-detection page
# SELECT
#     COUNT(*)                AS total_rows
#     , COUNT(DISTINCT post_id)  AS post_id_unique
#     , COUNT(DISTINCT subreddit_id)  AS subreddit_id_unique
#     , COUNT(DISTINCT user_id)  AS user_id_unique
# FROM tl_unique_with_meta_top_posts
# ;


-- Export data to google cloud storage (GCS)
# EXPORT DATA OPTIONS(
#   uri='gs://i18n-subreddit-clustering/posts/top/2021-07-09/*.parquet',
#   format='PARQUET',
#   overwrite=true
#   ) AS
# SELECT * EXCEPT (created_timestamp)
# FROM `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20210709`
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

