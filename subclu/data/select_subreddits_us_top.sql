-- noinspection SqlNoDataSourceInspectionForFile

-- Create table with selected subs for subs with a active flag &/or an activity threshold
-- NOTE: we can't use the Geo-relevant table because it doesn't seem like the US is part of these queries
--    For now, using simply subs with most views/posts & excluding those where over_18 = f
DECLARE partition_date DATE DEFAULT '2021-06-14';
DECLARE regex_remove_str STRING DEFAULT r"https://|http://|www\.|/r/|\.html|reddit|\.com|\.org";
DECLARE regex_replace_with_space_str STRING DEFAULT r"wiki/|/|-|_|\?|&nbsp;";
DECLARE min_users_l7 NUMERIC DEFAULT 800;
DECLARE min_posts_l28 NUMERIC DEFAULT 70;


CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20210616`
AS (

WITH selected_subs AS (
SELECT
    asr.subreddit_name
    # , acs.* EXCEPT( subreddit_name)

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
    AND acs.active = True
    AND slo.over_18 = "f"
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
    sel.*

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
)


-- Selection for table creation
SELECT DISTINCT * FROM final_table
ORDER BY users_l28 DESC, subscribers DESC, posts_l28 DESC
)
;


-- Count BEFORE creating table:
# SELECT
#     COUNT(*) AS row_count
#     , COUNT(DISTINCT subreddit_name)  AS unique_subreddits_count
#     , SUM(posts_l28)    AS total_posts_l28
#     , SUM(comments_l28) AS total_comments_l28
# FROM final_table
# ;

-- Counts BEFORE creating table - for selected-subs only
# SELECT
#     COUNT(*) AS row_count
#     , COUNT(DISTINCT subreddit_name)  AS unique_subreddits_count
#     , SUM(posts_l28)    AS total_posts_l28
#     , SUM(comments_l28) AS total_comments_l28
# FROM selected_subs
# ;

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
# FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20210616`

-- Inspect data (w/o some text cols)
# SELECT
#     * EXCEPT(
#     subreddit_public_description,
#     subreddit_description,
#     subreddit_name_title_and_clean_descriptions
#     )
# FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20210616`
# ;


-- Export data to google cloud storage (GCS)
-- CHANGE/Update:
# 1) URI date folder
# 2) source table
# EXPORT DATA OPTIONS(
#   uri='gs://i18n-subreddit-clustering/subreddits/2021-06-16/*.parquet',
#   format='PARQUET',
#   overwrite=true
#   ) AS
# SELECT *
# FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20210616`
# ;
