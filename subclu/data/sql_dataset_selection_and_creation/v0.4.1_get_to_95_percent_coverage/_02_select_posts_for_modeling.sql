-- noinspection SqlNoDataSourceInspectionForFile

-- Turns out that the language_detect_v2 table doesn't have unique posts/comments
--    so I have to create an intermediary table to remove duplicates
-- Select POSTS + detected language for topic modeling
-- Ambassador program only started around 05-01 so try to get data that includes posts after that date

-- Update checklist:
-- * start date
-- * end date
-- * max posts per sub
-- * name of new created table (update date)
-- * table with latest selected subreddits (e.g., subclu_subreddits_top_no_geo_20210709)
--      * update geo-relevant & ambassador columns if needed
-- * name of newly created table for exporting
-- * new GCS folder for new table

-- Create new POSTS table for v0.4.1 models
DECLARE start_date DATE DEFAULT '2021-10-14';
DECLARE end_date DATE DEFAULT '2021-12-14';
DECLARE MAX_POSTS_PER_SUB NUMERIC DEFAULT 1000;

-- Remove these from OCR text
DECLARE regex_remove_ocr_str STRING DEFAULT r"\d+[-:,\.]\d+([-:,\.]\d{2,4}){0,1}|\d|[\+\#]|[ur]/|http[s]{0,1}://|www\.|/r/|\.html|reddit|\.com|\.org";
DECLARE regex_remove_post_url STRING DEFAULT r"http[s]{0,1}://|www.|.html|utm|source=url";
DECLARE regex_replace_with_space_post_url STRING DEFAULT  r"/u/|/r/|/comments/|/|-|_+|\?|\&utm|\&|=|\+";


CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20211214`
PARTITION BY submit_date
AS (

    WITH
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
            -- No longer need to add additional filters because all rows here should be posts
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
    geo AS (
        SELECT
            # Keys & IDS
            gs.subreddit_name
            , sp.subreddit_id
            , sp.post_id
            , sp.user_id
            -- , sp.uuid

            # Meta content
            , sp.submit_date
            , sp.endpoint_timestamp
            , sp.noun
            , sp.removed
            , sp.upvotes
            , sp.comments
            -- , (sp.upvotes - sp.downvotes) AS net_upvotes -- net_upvotes in plo doesn't match GUI
            , sp.successful
            , sp.app_name
            , sp.post_type
            , sp.post_url
            , sp.post_nsfw

            -- Meta about subreddit
            --  for v0.4.1 there are 2 ways to qualify as geo-relevant, so we need extra columns
            , gs.geo_relevant_countries
            , gs.geo_relevant_country_codes
            , gs.geo_relevant_subreddit
            , gs.geo_relevant_subreddit_all
            , gs.geo_relevant_subreddit_v04
            , gs.ambassador_or_default_any
            , gs.ambassador_or_default_sub_france
            , gs.ambassador_or_default_sub_germany

            -- No longer use old/manual combined topic, use instead new tags/rating
            , gs.rating_short
            , gs.rating_name
            , gs.primary_topic

        FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20211214` AS gs
        LEFT JOIN posts_not_removed AS sp
            ON gs.subreddit_name = sp.subreddit_name AND gs.subreddit_id = sp.subreddit_id

        WHERE 1=1
            AND sp.row_num = 1

            -- for TESTING, keep only a few subreddits or only geo-relevant subs
            -- AND (
            --     gs.ambassador_subreddit = True
            --     OR gs.geo_relevant_subreddit = True
            -- )
    ),

    pl_with_meta AS (
        SELECT
            -- # counts check
            -- COUNT(DISTINCT(tl.id)) AS unique_post_ids
            -- , COUNT(DISTINCT(tl.subreddit_id)) AS unique_subreddits

            -- Mostly Keys/IDs to join
            geo.subreddit_name
            , tl.subreddit_id
            , tl.post_id
            , tl.user_id

            -- Metadata
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
            , geo.post_url
            , tl.geolocation_country_code

            -- Meta about subreddit
            , geo.rating_short
            , geo.rating_name
            , geo.primary_topic

            , geo.geo_relevant_countries
            , geo.geo_relevant_country_codes
            , geo.geo_relevant_subreddit
            , geo.geo_relevant_subreddit_all
            , geo.geo_relevant_subreddit_v04
            , geo.ambassador_or_default_any
            , geo.ambassador_or_default_sub_france
            , geo.ambassador_or_default_sub_germany

            # Language predictions
            -- Focus on weighted language b/c that's what I end up using anyway
            -- , tl.language
            -- , tl.probability
            , tl.weighted_language
            , tl.weighted_probability AS weighted_language_probability

            , plo.language_preference AS post_language_preference

            -- Text
            -- Wait to do relatively expensive string manipulation until AFTER removing duplicates
            -- , CHAR_LENGTH(tl.text) AS text_len
            -- , array_length(regexp_extract_all(tl.text, r"\b\w+\b")) text_word_count_estimate
            , plo.flair_text
            , tl.text

            -- Metadata to add separately?
            -- , tl.possible_alternatives  # unwieldy field, analyze later
            -- , tl.toxicity

        FROM posts_with_language AS tl
        INNER JOIN geo
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

        WHERE 1=1
            AND tl.post_thing_user_row_num = 1
            -- Filter out spam posts
            AND (
                COALESCE(plo.neutered, false) = false
                OR (
                    COALESCE(plo.neutered, false) = true
                    AND COALESCE(plo.verdict, 'None') IN ('mod-approved', 'admin-approved')
                )
            )
    ),

    ocr_text_agg AS (
        -- We need to agg the text because one post could have multiple images
        SELECT
            ocr.post_id
            , pt
            , TRIM(REGEXP_REPLACE(STRING_AGG(inferred_text, '. '), regex_remove_ocr_str, ""))  AS ocr_inferred_text_agg_clean

            , COUNT(media_url) AS ocr_images_in_post_count

        FROM `data-prod-165221.swat_tables.image_ocr_text` AS ocr

        WHERE DATE(ocr.pt) BETWEEN start_date AND end_date

        GROUP BY 1, 2
    ),

    pl_with_meta_top_posts AS (
        -- Here we rank each post in the sub by upvotes & comments
        --   (might try screenviews later, but that wasn't trivial)
        -- ALSO add OCR text here
        SELECT
            pl.* EXCEPT(flair_text, text)
            , ocr_images_in_post_count
            , pl.flair_text
            , pl.text
            , ocr_inferred_text_agg_clean

        FROM (
            SELECT
                -- There's something unexpected with the vote count in the `post_lookup` table
                --  (votes are missing when compared to the UI)
                --  so first do upvotes and then net upvotes
                ROW_NUMBER() OVER(
                    PARTITION BY subreddit_name
                    ORDER BY upvotes DESC, net_upvotes_lookup DESC, comments DESC
                ) AS rank_post_in_sub
                , *
            FROM pl_with_meta
        ) AS pl

        LEFT JOIN (
            SELECT * FROM ocr_text_agg
        WHERE COALESCE(ocr_inferred_text_agg_clean, "") != ""
        ) AS ocr
            ON pl.post_id = ocr.post_id

        WHERE 1=1
            AND pl.rank_post_in_sub <= MAX_POSTS_PER_SUB
    )

    -- This is the de-duped table used for modeling
    --   Comment this section out if to run TEST & PREVIEW queries BEFORE creating the table
    SELECT
        * EXCEPT (text, ocr_inferred_text_agg_clean)
        , text
        , ocr_inferred_text_agg_clean

    FROM (
        SELECT *
        -- Create a new column that cleans up the post_url col for embeddings
        -- Only create it if the link isn't posting to itself (otherwise we're leaking data about the subreddit)
        , CHAR_LENGTH(text) AS text_len
        , CHAR_LENGTH(ocr_inferred_text_agg_clean) AS ocr_text_len
        , array_length(regexp_extract_all(text, r"\b\w+\b")) text_word_count
        , array_length(regexp_extract_all(ocr_inferred_text_agg_clean, r"\b\w+\b")) ocr_text_word_count
        , CASE
            WHEN STARTS_WITH(post_url, 'https://i.redd.it') THEN NULL
            WHEN STARTS_WITH(post_url, 'https://v.redd.it') THEN NULL
            WHEN REGEXP_INSTR(
                post_url,
                ARRAY_REVERSE(SPLIT(post_id, "_"))[SAFE_OFFSET(0)]
                ) > 0 THEN NULL
            ELSE TRIM(REGEXP_REPLACE(REGEXP_REPLACE(post_url, regex_remove_post_url, ""), regex_replace_with_space_post_url, " "))
        END AS post_url_for_embeddings

        FROM pl_with_meta_top_posts
    )
);  -- close CREATE TABLE parens


-- ###############
-- Tests/CHECKS
-- ###
-- Check whether pl_with_meta is unique (should be)
--  and how many posts per sub overall
--  This shoud also include FLAIR_TEXT
-- SELECT
--     *
--     , (post_unique_count / subreddit_unique_count) AS posts_per_subreddit_mean
--     , (posts_with_flair_text / post_unique_count)  AS posts_with_flair_text_pct
-- FROM (
--     SELECT
--         COUNT(*)       AS row_count
--         , COUNT(DISTINCT post_id) AS post_unique_count
--         , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count
--         , COUNT(DISTINCT user_id)   AS user_id_unique
--         , COUNT(flair_text)         AS posts_with_flair_text
--     FROM pl_with_meta
-- )
-- ;
-- RESULT, using only successful_posts
-- row_count 	 post_unique_count 	 subreddit_unique_count 	 posts_per_subreddit_mean
-- 20,507,544 	 20,507,544 	 19,218 	 1,067.10

-- RESULT, using successful_posts AND inner JOIN with post_lookup:
--   Looks like we lose ~24 posts in inner join, which should be ok. posts remain unique
-- row_count 	 post_unique_count 	 subreddit_unique_count 	 posts_per_subreddit_mean
-- 20,507,520 	 20,507,520 	 19,218 	 1,067.10

-- RESULT, after adding flair text
-- row_count 	 post_unique_count 	 subreddit_unique_count 	 user_id_unique 	 posts_with_flair_text 	 posts_per_subreddit_mean 	posts_with_flair_text_pct
-- 20,507,520 	 20,507,520 	 19,218 	 5,336,786 	 10,428,807 	 1,067.10 	50.9%

-- RESULT, filter out spam (neutered=false)
--  We drop around 2.3 million posts that were marked as potential spam
--  row_count 	 post_unique_count 	 subreddit_unique_count 	 user_id_unique 	 posts_with_flair_text 	 posts_per_subreddit_mean 	posts_with_flair_text_pct
--  18,129,364 	 18,129,364 	 19,191 	 4,890,672 	 9,564,330 	 944.68 	52.8%



-- Check counts for pl_with_meta_top_posts (TOP ONLY)
--   Here we should only see the topN posts in each subreddit
--   This will also include FLAIR and OCR text
-- SELECT
--     *
--     , (post_unique_count / subreddit_unique_count) AS posts_per_subreddit_mean
--     , (posts_with_flair_text / post_unique_count)  AS posts_with_flair_text_pct
--     , (posts_with_ocr_text / post_unique_count)  AS posts_with_ocr_text_pct
-- FROM (
--     SELECT
--         COUNT(*)       AS row_count
--         , COUNT(DISTINCT post_id) AS post_unique_count
--         , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count
--         , COUNT(DISTINCT user_id)   AS user_id_unique
--         , COUNT(flair_text)         AS posts_with_flair_text
--         , COUNTIF(COALESCE(ocr_images_in_post_count, 0) > 0) AS posts_with_ocr_text
--     FROM pl_with_meta_top_posts
-- )
-- ;
-- RESULT, using only successful_posts
-- row_count 	 post_unique_count 	 subreddit_unique_count 	 posts_per_subreddit_mean
-- 9,110,979 	 9,110,979 	 19,218 	 474.09

-- RESULT, using successful_posts AND inner JOIN with post_lookup:
--   Again we lose a few posts, but this time it's only ~3 posts
-- row_count 	 post_unique_count 	 subreddit_unique_count 	 posts_per_subreddit_mean
-- 9,110,962 	 9,110,962 	 19,218 	 474.08

-- RESULT, after adding flair & OCR text
--  row_count 	 post_unique_count 	 subreddit_unique_count  user_id_unique posts_with_flair_text  posts_with_ocr_text  posts_per_subreddit_mean  posts_with_flair_text_pct	posts_with_ocr_text_pct
--  9,110,962 	 9,110,962 	 19,218 	 3,400,990 	 4,462,241 	 1,726,455 	 474.08 	49.0%	18.9%

-- RESULT, filter out spam (neutered=false)
--  We drop around 600k posts when we exclude neutered (spam) posts
--   Notice that 27 subreddits drop out altogether b/c all their posts are neutered... oh well
--  row_count 	 post_unique_count 	 subreddit_unique_count 	 user_id_unique 	 posts_with_flair_text 	 posts_with_ocr_text 	 posts_per_subreddit_mean 	posts_with_flair_text_pct	posts_with_ocr_text_pct
--  8,436,715 	 8,436,715 	 19,191 	 3,198,511 	 4,234,715 	 1,630,897 	 439.62 	50.2%	19.3%

-- RESULT, filter out spam BUT keep posts that are mod-approved or admin-approved
--   This gets us back about 3k posts (false-negative for spam?)
--  row_count 	 post_unique_count 	 subreddit_unique_count 	 user_id_unique 	 posts_with_flair_text 	 posts_with_ocr_text
--                                                                          posts_per_subreddit_mean 	posts_with_flair_text_pct	posts_with_ocr_text_pct
--  8,439,672 	 8,439,672 	 19,192 	 3,200,560 	 4,236,245 	 1,631,678 	 439.75 	50.2%	19.3%



-- ##############################
-- Count POST totals v. unique AFTER CREATING TABLE
-- SELECT
--     *
--     , (post_unique_count / subreddit_unique_count) AS posts_per_subreddit_mean
--     , (posts_with_flair_text / post_unique_count)  AS posts_with_flair_text_pct
--     , (posts_with_ocr_text / post_unique_count)  AS posts_with_ocr_text_pct
--     , (urls_for_embeddings_count / post_unique_count)  AS posts_with_urls_for_embeddings_pct
-- FROM (
--     SELECT
--         COUNT(*)       AS row_count
--         , COUNT(DISTINCT post_id) AS post_unique_count
--         , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count
--         , COUNT(DISTINCT user_id)   AS user_id_unique
--         , COUNT(flair_text)         AS posts_with_flair_text
--         , COUNTIF(COALESCE(ocr_images_in_post_count, 0) > 0) AS posts_with_ocr_text
--         , COUNT(post_url_for_embeddings)    AS urls_for_embeddings_count
--
--     FROM `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20211214`
-- )
-- ;
-- RESULT
-- post_unique_count	subreddit_unique_count	user_id_unique	posts_with_flair_text	posts_with_ocr_text	urls_for_embeddings_count
--                                                                                         posts_per_subreddit_mean     posts_with_flair_text_pct	posts_with_ocr_text_pct	posts_with_urls_for_embeddings_pct
-- 15,629,959 	 15,629,958 	 49,625 	 4,077,866 	 5,981,264 	 2,303,048 	 3,868,595 	314.9613703	38.3%	14.7%	24.8%



-- ##############################
-- PREVIEW posts with post rank and other meta BEFORE creating table
-- SELECT
--     rank_post_in_sub
--     , * EXCEPT(rank_post_in_sub)
-- FROM pl_with_meta_top_posts

-- WHERE 1=1
--     -- add filter to check OCR image content
--     AND ocr_images_in_post_count IS NOT NULL

--     -- Filters to check non-English posts
--     AND geo_relevant_countries LIKE '%Germany%'

-- ORDER BY subreddit_id ASC, rank_post_in_sub

-- LIMIT 3500
-- ;



-- ###############
-- Export data to google cloud storage (GCS)
-- ###
-- Export POST table to GCS for modeling
-- EXPORT DATA OPTIONS(
--   uri='gs://i18n-subreddit-clustering/posts/top/2021-12-14/*.parquet',
--   format='PARQUET',
--   overwrite=true
--   ) AS
-- SELECT * EXCEPT (created_timestamp)
-- FROM `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20211214`
-- ORDER BY subreddit_id DESC, rank_post_in_sub DESC
-- ;

-- FIX: there was one duplicate post_id, so we need to drop it to prevent weird things downstream
--  seems like dupe happened because it had multiple OCR image text
-- FIX: there was one duplicate post_id, so we need to drop it to prevent weird things downstream
--  seems like dupe happened because it had multiple OCR image text
EXPORT DATA OPTIONS(
  uri='gs://i18n-subreddit-clustering/posts/top/2021-12-14_fix/*.parquet',
  format='PARQUET',
  overwrite=true
  ) AS

SELECT
    * EXCEPT (created_timestamp, post_row_num)

    -- check counts
    -- COUNT(*) AS row_count
    -- , COUNT(DISTINCT post_id)  AS post_count_unique

FROM (
    SELECT
        *
        , ROW_NUMBER() OVER(
            PARTITION BY post_id, user_id
            ORDER BY created_timestamp ASC, text_len DESC
        ) AS post_row_num
    FROM `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20211214`
)
WHERE post_row_num = 1
ORDER BY subreddit_id DESC, rank_post_in_sub DESC
;

