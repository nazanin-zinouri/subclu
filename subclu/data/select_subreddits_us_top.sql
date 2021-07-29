-- noinspection SqlNoDataSourceInspectionForFile

-- Create table with selected subs for subs with a active flag &/or an activity threshold
-- NOTE: we can't use the Geo-relevant table because it doesn't seem like the US is part of these queries
--    For now, using simply subs with most views/posts & excluding those where over_18 = f
DECLARE partition_date DATE DEFAULT '2021-07-13';
DECLARE regex_remove_str STRING DEFAULT r"https://|http://|www\.|/r/|\.html|reddit|\.com|\.org";
DECLARE regex_replace_with_space_str STRING DEFAULT r"wiki/|/|-|_|\?|&nbsp;";
DECLARE min_users_l7 NUMERIC DEFAULT 100;
DECLARE min_posts_l28 NUMERIC DEFAULT 40;

DECLARE min_DACH_posts_l28 NUMERIC DEFAULT 4;
DECLARE AT_and_CH_min_num_users_l28 NUMERIC DEFAULT 2000;


CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20210716`
AS (

WITH dach_subs AS (
-- D.A.CH = Deutschland (Germany), Austria, & CH (Switzerland)
SELECT
    geo.subreddit_name

FROM `reddit-employee-datasets.wacy_su.geo_relevant_subreddits_2021` AS geo

LEFT JOIN (
    -- Using sub-selection in case there are subs that haven't been registered in asr table
    SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
    WHERE DATE(pt) = partition_date
        AND users_l7 >= 100
) AS asr
    ON geo.subreddit_name = asr.subreddit_name

-- Besides geo.rank_no, select based on posts + number of users w/ screen views
WHERE asr.posts_l28 >= min_DACH_posts_l28
    AND (
        geo.geo_country_code = "DE"
        OR (
            geo.geo_country_code IN ("AT", "CH")
            AND (geo.rank_no <= 10 OR asr.users_l28 >= AT_and_CH_min_num_users_l28)
        )
    )
),

ambassador_subs AS (
-- Wacy's table pulls data from a spreadsheet that Alex updates
SELECT
    LOWER(amb.subreddit_name)           AS subreddit_name

FROM `reddit-employee-datasets.wacy_su.ambassador_subreddits` AS amb
LEFT JOIN `reddit-employee-datasets.wacy_su.geo_relevant_subreddits_2021` AS geo
    ON LOWER(amb.subreddit_name) = geo.subreddit_name
WHERE amb.subreddit_name IS NOT NULL
),


top_subs AS (
-- Here we select subreddits from anywhere based on minimum users(views) & post counts
SELECT
    asr.subreddit_name

    -- Use for checks but drop for prod to reduce name conflicts
    # , acs.* EXCEPT( subreddit_name)
    # , asr.posts_l28
    # , asr.comments_l28

FROM (
    SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
    WHERE DATE(pt) = partition_date
) AS asr

LEFT JOIN (
    SELECT * FROM `data-prod-165221.ds_subreddit_whitelist_tables.active_subreddits`
    WHERE DATE(_PARTITIONTIME) = partition_date
) AS acs
    ON asr.subreddit_name = acs.subreddit_name
LEFT JOIN (
    SELECT * FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
    # Look back 2 days because looking back 1-day could be an empty partition
    WHERE dt = (CURRENT_DATE() - 2)
) AS slo
    ON asr.subreddit_name = slo.name

WHERE 1=1
    AND asr.users_l7 >= min_users_l7
    AND asr.posts_l28 >= min_posts_l28
    AND slo.over_18 = "f"

    -- Exclude active flag for now... it kills off a lot of subs that might be interesting
    # AND acs.active = True

),


selected_subs AS (
-- Here's where we merge all subreddits to cluster: top (no geo), German top, & ambassador subs
SELECT
    COALESCE(top.subreddit_name, das.subreddit_name, ams.subreddit_name)  AS subreddit_name

FROM top_subs AS top
FULL OUTER JOIN dach_subs AS das
    ON top.subreddit_name = das.subreddit_name
FULL OUTER JOIN ambassador_subs AS ams
    ON top.subreddit_name = ams.subreddit_name
),

subreddit_lookup AS (
    SELECT
        *
        , COALESCE(array_length(regexp_extract_all(clean_description, r"\b\w+\b")), 0)      AS subreddit_clean_description_word_count
        , array_length(regexp_extract_all(subreddit_name_title_public_description, r"\b\w+\b"))       AS subreddit_name_title_public_description_word_count
        # do word count for full concat column on final query
        , CASE
            WHEN (description = public_description) THEN subreddit_name_title_public_description
            ELSE CONCAT(subreddit_name_title_public_description, ". \n", COALESCE(clean_description, ""))
            END AS subreddit_name_title_and_clean_descriptions

    FROM (
        SELECT
            *
            , TRIM(REGEXP_REPLACE(REGEXP_REPLACE(description, regex_remove_str, ""), regex_replace_with_space_str, " ")) AS clean_description
            , CONCAT(
                name, ". \n", COALESCE(title, ""), ". \n",
                COALESCE(
                    TRIM(REGEXP_REPLACE(REGEXP_REPLACE(public_description, regex_remove_str, ""), regex_replace_with_space_str, " ")),
                    "")
                ) AS subreddit_name_title_public_description

        FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`

        # Look back 2 days because looking back 1-day could be an empty partition
        WHERE dt = (CURRENT_DATE() - 2)
    )
),

final_table AS (
SELECT
    -- Subreddit_id was missing from original query, adding it back here for the future
    slo.subreddit_id
    , sel.*

    , COALESCE (
        LOWER(dst.topic),
        "uncategorized"
    ) AS combined_topic
    , CASE
        WHEN rt.rating IN ("x", "nc17") THEN "over18_nsfw"
        WHEN dst.topic = "Mature Themes and Adult Content" THEN "over18_nsfw"
        WHEN slo.over_18 = "t" THEN "over18_nsfw"
        ELSE COALESCE (
            LOWER(dst.topic),
            "uncategorized"
        )
        END         AS combined_topic_and_rating

    , rt.rating
    , rt.version    AS rating_version

    , dst.topic
    , dst.version   AS topic_version

    -- Meta from lookup
    , slo.over_18
    , slo.allow_top
    , slo.video_whitelisted
    , slo.lang      AS subreddit_language
    , slo.whitelist_status
    , slo.subscribers

    , asr.first_screenview_date
    , asr.last_screenview_date
    , asr.users_l7
    , asr.users_l28
    , asr.posts_l7
    , asr.posts_l28
    , asr.comments_l7
    , asr.comments_l28

    , CURRENT_DATE() as pt

    -- Text from lookup
    , slo.subreddit_clean_description_word_count
    , array_length(regexp_extract_all(subreddit_name_title_and_clean_descriptions, r"\b\w+\b")) subreddit_name_title_and_clean_descriptions_word_count
    , slo.title     AS subreddit_title
    , slo.public_description AS subreddit_public_description
    , slo.description AS subreddit_description
    # , slo.clean_description AS subreddit_clean_description
    , slo.subreddit_name_title_and_clean_descriptions

-- Use distinct in case a sub qualifies for more than 1 reason
FROM (SELECT DISTINCT * FROM selected_subs) AS sel

LEFT JOIN (
    -- Using sub-selection in case there are subs that haven't been registered in asr table
    SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
    WHERE DATE(pt) = partition_date
) AS asr
    ON sel.subreddit_name = asr.subreddit_name

LEFT JOIN (
    SELECT * FROM `data-prod-165221.ds_v2_subreddit_tables.subreddit_ratings`
    WHERE DATE(pt) = partition_date
) AS rt
    ON sel.subreddit_name = rt.subreddit_name

LEFT JOIN(
    SELECT * FROM `data-prod-165221.ds_v2_subreddit_tables.subreddit_topics`
    WHERE DATE(pt) = partition_date
) AS dst
    ON sel.subreddit_name = dst.subreddit_name

LEFT JOIN subreddit_lookup AS slo
    ON sel.subreddit_name = LOWER(slo.name)

WHERE
    -- Re-apply minimum post count in case something unexpected happened in previous joins
    asr.posts_l28 >= min_DACH_posts_l28
)


-- Selection for table creation
SELECT DISTINCT * FROM final_table
ORDER BY users_l28 DESC, subscribers DESC, posts_l28 DESC
)  -- Close out CREATE TABLE parens
;


-- Count BEFORE creating table:
# SELECT
#     COUNT(*) AS row_count
#     , COUNT(DISTINCT subreddit_name)  AS unique_subreddits_count
#     , SUM(posts_l28)    AS total_posts_l28
#     , SUM(comments_l28) AS total_comments_l28
# FROM final_table
# ;
-- row_count	unique_subreddits_count	total_posts_l28	total_comments_l28
-- 3,767 	    3,767 	                3,164,327 	    42,786,190

-- Counts BEFORE creating table - for selected-subs only
--  This table might have duplicates and some of these subs might not have recent posts
# SELECT
#     COUNT(*) AS row_count
#     , COUNT(DISTINCT subreddit_name)  AS unique_subreddits_count
#     # , SUM(posts_l28)    AS total_posts_l28
#     # , SUM(comments_l28) AS total_comments_l28
# FROM selected_subs
# ;
-- Row	row_count	unique_subreddits_count
--  1 	3,809 	    3,804

-- Check selected-subs table
# SELECT
#     *
# FROM selected_subs
# LIMIT 200
# ;


-- Count AFTER creating table
# SELECT
#     COUNT(*) AS row_count
#     , COUNT(DISTINCT subreddit_name)  AS unique_subreddits_count
#     , SUM(posts_l28)    AS total_posts_l28
#     , SUM(comments_l28) AS total_comments_l28
# FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20210716`;
-- row_count	unique_subreddits_count	total_posts_l28	total_comments_l28
-- 3,767 	    3,767 	                3,164,327 	    42,786,190

-- Inspect data (w/o some text cols)
# SELECT
#     * EXCEPT(
#     subreddit_public_description,
#     subreddit_description,
#     subreddit_name_title_and_clean_descriptions
#     )
# FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20210716`
# ORDER BY subreddit_name ASC # posts_l28 ASC
# LIMIT 300
# ;


-- Export data to google cloud storage (GCS)
-- CHANGE/Update:
--  1) URI date folder
--  2) source table
-- EXPORT DATA OPTIONS(
--   uri='gs://i18n-subreddit-clustering/subreddits/top/2021-07-16/*.parquet',
--   format='PARQUET',
--   overwrite=true
--   ) AS
--
-- SELECT
--     -- Subreddit_id was missing from original query, adding it back here for the future
--     slo.subreddit_id
--     , sel.*
-- FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20210716` AS sel
-- LEFT JOIN `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup` AS slo
--     ON sel.subreddit_name = LOWER(slo.name)
--
-- WHERE slo.dt = (CURRENT_DATE() - 2)
-- # For some reason, adding the order by clause makes BigQuery export in fewer and larger files
-- ORDER BY users_l28 DESC, subscribers DESC, posts_l28 DESC
-- ;
