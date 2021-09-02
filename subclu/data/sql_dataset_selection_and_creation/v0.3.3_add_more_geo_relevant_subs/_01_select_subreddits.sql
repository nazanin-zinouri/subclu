-- noinspection SqlNoDataSourceInspectionForFile

-- Create table with selected subs for subs with a active flag &/or an activity threshold
--    For now, select subs with most views/posts & exclude those where over_18 = f
-- Filter NOTE:
--  over_18="f" set BY THE MODS! So we still might seem some NSFW subreddits
-- TODO(djb) in v0.3.2 pull we had 3,700 subs
DECLARE partition_date DATE DEFAULT '2021-08-31';
DECLARE regex_remove_str STRING DEFAULT r"https://|http://|www\.|/r/|\.html|reddit|\.com|\.org";
DECLARE regex_replace_with_space_str STRING DEFAULT r"wiki/|/|-|_|\?|&nbsp;";
DECLARE min_users_l7 NUMERIC DEFAULT 3000;
DECLARE min_posts_l28 NUMERIC DEFAULT 100;

DECLARE min_users_geo_l7 NUMERIC DEFAULT 15;
DECLARE min_posts_geo_l28 NUMERIC DEFAULT 4;

-- CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_geo_20210831`
-- AS (

-- First select subreddits based on geo-relevance
WITH geo_subs_raw AS (
SELECT
    LOWER(geo.subreddit_name)  AS subreddit_name
    , geo.* EXCEPT(subreddit_name)
    -- We need to split the name b/c: "Bolivia, State of..."
    , SPLIT(cm.country_name, ', ')[OFFSET(0)] AS country_name
    , cm.country_code
    , cm.region
    , RANK () OVER (PARTITION BY subreddit_id, country ORDER BY pt desc) as sub_geo_rank_no
FROM `data-prod-165221.i18n.geo_sfw_communities` AS geo
LEFT JOIN `data-prod-165221.ds_utility_tables.countrycode_region_mapping` AS cm
    ON geo.country = cm.country_code
WHERE DATE(pt) BETWEEN (CURRENT_DATE() - 56) AND (CURRENT_DATE() - 2)
    AND (
        cm.country_name IN ('Germany', 'Austria', 'Switzerland', 'India', 'France', 'Spain', 'Brazil', 'Portugal', 'Italy')
        OR cm.region = 'LATAM'
    )
-- Order by country name here so that the aggregation sorts the names alphabetically
ORDER BY subreddit_name ASC, cm.country_name ASC
),

subs_selected_by_geo AS (
SELECT
    geo.subreddit_id
    , geo.subreddit_name
    , STRING_AGG(geo.country_name, ', ') AS geo_relevant_countries
    , COUNT(geo.country_code) AS geo_relevant_country_count

    -- use for checks but exclude for joins to prevent naming conflicts
    -- , asr.users_l7
    -- , asr.posts_l28
    -- , asr.comments_l28
    -- , nt.rating_name
    -- , nt.primary_topic

FROM geo_subs_raw AS geo
LEFT JOIN `data-prod-165221.all_reddit.all_reddit_subreddits` AS asr
    ON geo.subreddit_name = asr.subreddit_name
LEFT JOIN `data-prod-165221.ds_subreddit_whitelist_tables.active_subreddits`    AS acs
    ON asr.subreddit_name = acs.subreddit_name
LEFT JOIN `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`             AS slo
    ON asr.subreddit_name = LOWER(slo.name)
LEFT JOIN `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsourced_topic_and_rating` AS nt
    ON acs.subreddit_id = nt.subreddit_id

WHERE 1=1
    -- Drop duplicated country names
    AND geo.sub_geo_rank_no = 1

    AND DATE(asr.pt) = partition_date
    AND DATE(acs._PARTITIONTIME) = partition_date
    AND slo.dt = partition_date
    AND nt.pt = partition_date

    AND slo.quarantine = false
    AND asr.users_l7 >= min_users_geo_l7
    AND asr.posts_l28 >= min_posts_geo_l28

GROUP BY 1, 2
    -- , 5, 6, 7, 8, 9
ORDER BY geo.subreddit_name
),

-- ###############
-- Now, select subs based on activity
-- ###
subs_selected_by_activity AS (
-- Here we select subreddits from anywhere based on minimum users(views) & post counts
SELECT
    asr.subreddit_name
    , slo.subreddit_id

    -- Use for checks but drop for prod to reduce name conflicts
    # , acs.* EXCEPT( subreddit_name)
    , asr.users_l7
    , asr.posts_l28
    , asr.comments_l28

FROM `data-prod-165221.all_reddit.all_reddit_subreddits` AS asr

LEFT JOIN `data-prod-165221.ds_subreddit_whitelist_tables.active_subreddits`    AS acs
    ON asr.subreddit_name = acs.subreddit_name
LEFT JOIN `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`             AS slo
    ON asr.subreddit_name = LOWER(slo.name)
LEFT JOIN `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsourced_topic_and_rating` AS nt
    ON acs.subreddit_id = nt.subreddit_id

WHERE 1=1
    AND DATE(asr.pt) = partition_date
    AND DATE(acs._PARTITIONTIME) = partition_date
    AND slo.dt = partition_date
    AND nt.pt = partition_date

    AND slo.quarantine = false
    AND asr.users_l7 >= min_users_l7
    AND asr.posts_l28 >= min_posts_l28

    -- Thousands of subs have the over_18 flag as null, so we need to account for it
    AND (
        slo.over_18 = 'f'
        OR slo.over_18 IS NULL
    )
    AND nt.rating_short != 'X'

    AND acs.active = True
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

        -- Look back 2+ days because looking back 1-day could be an empty partition
        WHERE dt = partition_date
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
    asr.posts_l28 >= min_posts_l28
)


-- Selection for table creation
SELECT DISTINCT * FROM final_table
ORDER BY users_l28 DESC, subscribers DESC, posts_l28 DESC

)  -- Close out CREATE TABLE parens
;


-- Count BEFORE creating table:
-- SELECT
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT subreddit_name)  AS unique_subreddits_count
--     , SUM(posts_l28)    AS total_posts_l28
--     , SUM(comments_l28) AS total_comments_l28
-- FROM final_table
-- ;
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
