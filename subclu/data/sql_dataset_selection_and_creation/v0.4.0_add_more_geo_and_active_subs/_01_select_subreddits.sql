-- noinspection SqlNoDataSourceInspectionForFile

-- Create table with selected subs for subs with a active flag &/or an activity threshold
--    For now, select subs with most views/posts & exclude those where over_18 = f
-- Filter NOTE:
--  over_18="f" set BY THE MODS! So we still might seem some NSFW subreddits
-- TODO(djb) in v0.3.2 pull we had 3,700 subs; now we want ~10k subs
-- CREATE TABLE with new selected subreddits for v0.4.0 clustering
DECLARE partition_date DATE DEFAULT '2021-09-07';
DECLARE regex_remove_str STRING DEFAULT r"https://|http://|www\.|/r/|\.html|reddit|\.com|\.org";
DECLARE regex_replace_with_space_str STRING DEFAULT r"wiki/|/|-|_|\?|&nbsp;";
DECLARE min_users_l7 NUMERIC DEFAULT 3100;
DECLARE min_posts_l28 NUMERIC DEFAULT 115;

DECLARE min_users_geo_l7 NUMERIC DEFAULT 300;
DECLARE min_posts_geo_l28 NUMERIC DEFAULT 10;

CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20210910`
AS (

-- First select subreddits based on geo-relevance
WITH
    -- These subs come from a custom table that lowers the percent to qualify as geo-relevant
    --   this table includes subs even if they are `active=false`
    geo_subs_custom_raw AS (
        SELECT
            subreddit_name
            , subreddit_id
            , country_name
            , geo2.geo_country_code AS country_code
            , geo2.geo_region AS region
            , ROW_NUMBER() OVER (PARTITION BY subreddit_id, geo_country_code ORDER BY pt desc) as sub_geo_rank_no
        -- This table is a single snapshot, so no need to filter by partition
        FROM `reddit-employee-datasets.david_bermejo.subclu_geo_subreddits_20210909` AS geo2
        WHERE 1=1
            AND (
                country_name IN ('Germany', 'Austria', 'Switzerland', 'India', 'France', 'Spain', 'Brazil', 'Portugal', 'Italy')
                OR geo_region = 'LATAM'
            )
        -- ORDER BY subreddit_name ASC, country_name ASC
    ),

    subs_selected_by_geo AS (
        SELECT
            geo.subreddit_id
            , geo.subreddit_name

            -- Order so that we get (Mexico, US) only, and not (US, Mexico)
            , STRING_AGG(geo.country_name, ', ' ORDER BY geo.country_name) AS geo_relevant_countries
            , STRING_AGG(geo.country_code, ', ' ORDER BY geo.country_code) AS geo_relevant_country_codes
            , COUNT(geo.country_code) AS geo_relevant_country_count

            -- cols for checking/debugging
            , asr.users_l7
            , asr.posts_l28
            , asr.comments_l28
            , nt.rating_name
            , nt.primary_topic

        FROM geo_subs_custom_raw AS geo
        LEFT JOIN (
            SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
            WHERE DATE(pt) = partition_date
        ) AS asr
            ON geo.subreddit_name = asr.subreddit_name
        LEFT JOIN (
            SELECT * FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
            WHERE dt = partition_date
        ) AS slo
            ON asr.subreddit_name = LOWER(slo.name)
        LEFT JOIN `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsourced_topic_and_rating` AS nt
            ON geo.subreddit_id = nt.subreddit_id

        WHERE 1=1
            -- Drop duplicated country names
            AND geo.sub_geo_rank_no = 1

            -- AND DATE(acs._PARTITIONTIME) = partition_date
            AND nt.pt = partition_date

            -- remove quarantine filter, b/c if we score them we might be able to clusters
            --   of subreddits that are similar to previoiusly quarantined subs
            -- AND slo.quarantine = false
            AND asr.users_l7 >= min_users_geo_l7
            AND asr.posts_l28 >= min_posts_geo_l28

            -- Filter out subs that are highly likely porn to reduce processing overhead & improve similarities
            -- Better to include some in clustering than exclude a sub that was mislabeled
            -- REMOVE `NOT` to reverse (show only the things we filtered out)
            --      'askredditespanol' -- rated as X... sigh
            AND NOT (
                (
                    COALESCE(slo.whitelist_status, '') = 'no_ads'
                    AND COALESCE(nt.rating_short, '') = 'X'
                    AND (
                        COALESCE(nt.primary_topic, '') IN ('Mature Themes and Adult Content', 'Celebrity')
                        OR 'sex_porn' IN UNNEST(mature_themes)
                        OR 'sex_content_arousal' IN UNNEST(mature_themes)
                        OR 'nudity_explicit' IN UNNEST(mature_themes)
                    )
                )
                OR COALESCE(nt.primary_topic, '') = 'Celebrity'
                OR (
                    COALESCE(nt.rating_short, '') = 'X'
                    AND 'nudity_explicit' IN UNNEST(mature_themes)
                    AND 'nudity_full' IN UNNEST(mature_themes)
                )
                OR (
                    -- r/askredditEspanol doesn't get caught by this because it has a NULL primary_topic
                    COALESCE(nt.rating_short, 'M') = 'X'
                    AND COALESCE(nt.primary_topic, '') IN ('Celebrity', 'Mature Themes and Adult Content')
                    AND (
                        'sex_porn' IN UNNEST(mature_themes)
                        OR 'sex_content_arousal' IN UNNEST(mature_themes)
                        OR 'nudity_explicit' IN UNNEST(mature_themes)
                        OR 'sex_explicit_ref' IN UNNEST(mature_themes)
                    )
                )
                OR (
                    'sex_porn' IN UNNEST(mature_themes)
                    AND (
                        'sex_content_arousal' IN UNNEST(mature_themes)
                        OR 'nudity_explicit' IN UNNEST(mature_themes)
                        OR 'sex_ref_regular' IN UNNEST(mature_themes)
                    )
                )
                OR (
                    COALESCE(nt.primary_topic, '') IN ('Celebrity', 'Mature Themes and Adult Content')
                    AND 'sex_content_arousal' IN UNNEST(mature_themes)
                )
            )

        GROUP BY 1, 2
            , 6, 7, 8, 9, 10
        ORDER BY geo.subreddit_name
    ),

    ambassador_subs AS (
        -- Wacy's table pulls data from a spreadsheet that Alex updates
        SELECT
            LOWER(amb.subreddit_name)           AS subreddit_name
            , slo.subreddit_id

        FROM `reddit-employee-datasets.wacy_su.ambassador_subreddits` AS amb
        LEFT JOIN `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`             AS slo
            ON amb.subreddit_name = LOWER(slo.name)
        WHERE amb.subreddit_name IS NOT NULL
            AND slo.dt = partition_date
    ),

    -- ###############
    -- Here we select subreddits from anywhere based on minimum users(views) & post counts
    subs_selected_by_activity AS (
        SELECT
            asr.subreddit_name
            , slo.subreddit_id

            -- Use for checks but drop for prod to reduce name conflicts & reduce query complexity
            --  we'll add these later for all subreddits in the final table
            # , acs.* EXCEPT( subreddit_name)
            -- , asr.users_l7
            -- , asr.posts_l28
            -- , asr.comments_l28

            -- , nt.rating_short
            -- , nt.rating_name
            -- , nt.primary_topic
            -- , array_to_string(secondary_topics,", ") as secondary_topics
            -- , array_to_string(mature_themes,", ") as mature_themes_list

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

            -- remove quarantine filter, b/c if we score them we might be able to clusters
            --   of subreddits that are similar to previoiusly quarantined subs
            -- AND slo.quarantine = false
            AND asr.users_l7 >= min_users_l7
            AND asr.posts_l28 >= min_posts_l28

            -- Need COALESCE because thousands of subs have an empty field
            AND COALESCE(slo.over_18, 'f') = 'f'

            -- For the high-activity subs I'll keep the active flag
            AND acs.active = True
            -- Filter out subs that are highly likely porn to reduce processing overhead & improve similarities
            -- Better to include some in clustering than exclude a sub that was mislabeled
            -- REMOVE `NOT` to reverse (show only the things we filtered out)
            --      'askredditespanol' -- rated as X... sigh
            -- Test edge cases by only looking at subs not rated as E
            -- AND COALESCE(nt.rating_short, '') NOT IN ('E')
            AND NOT (
                (
                    COALESCE(slo.whitelist_status, '') = 'no_ads'
                    AND COALESCE(nt.rating_short, '') = 'X'
                    AND (
                        COALESCE(nt.primary_topic, '') IN ('Mature Themes and Adult Content', 'Celebrity')
                        OR 'sex_porn' IN UNNEST(mature_themes)
                        OR 'sex_content_arousal' IN UNNEST(mature_themes)
                        OR 'nudity_explicit' IN UNNEST(mature_themes)
                    )
                )
                OR COALESCE(nt.primary_topic, '') = 'Celebrity'
                OR (
                    COALESCE(nt.rating_short, '') = 'X'
                    AND 'nudity_explicit' IN UNNEST(mature_themes)
                    AND 'nudity_full' IN UNNEST(mature_themes)
                )
                OR (
                    -- r/askredditEspanol doesn't get caught by this because it has a NULL primary_topic
                    COALESCE(nt.rating_short, 'M') = 'X'
                    AND COALESCE(nt.primary_topic, '') IN ('Celebrity', 'Mature Themes and Adult Content')
                    AND (
                        'sex_porn' IN UNNEST(mature_themes)
                        OR 'sex_content_arousal' IN UNNEST(mature_themes)
                        OR 'nudity_explicit' IN UNNEST(mature_themes)
                        OR 'sex_explicit_ref' IN UNNEST(mature_themes)
                    )
                )
                OR (
                    'sex_porn' IN UNNEST(mature_themes)
                    AND (
                        'sex_content_arousal' IN UNNEST(mature_themes)
                        OR 'nudity_explicit' IN UNNEST(mature_themes)
                        OR 'sex_ref_regular' IN UNNEST(mature_themes)
                    )
                )
                OR (
                    COALESCE(nt.primary_topic, '') IN ('Celebrity', 'Mature Themes and Adult Content')
                    AND 'sex_content_arousal' IN UNNEST(mature_themes)
                )
            )
    ),

    -- Here's where we merge all subreddits to cluster: top (no geo), Geo-top, & ambassador subs
    selected_subs AS (
        SELECT DISTINCT
            merged.subreddit_name
            , merged.subreddit_id
            , geo.geo_relevant_country_codes
            , geo.geo_relevant_countries
            , geo.geo_relevant_country_count

            , CASE
                WHEN (geo.subreddit_id IS NOT NULL) THEN true
                ELSE false
                END AS geo_relevant_subreddit
            , CASE
                WHEN (amb.subreddit_id IS NOT NULL) THEN true
                ELSE false
                END AS ambassador_subreddit

        -- Join on itself to find why a subreddit qualified
        FROM (
            SELECT
                COALESCE(top1.subreddit_name, geo1.subreddit_name, amb1.subreddit_name)  AS subreddit_name
                , COALESCE(top1.subreddit_id, geo1.subreddit_id, amb1.subreddit_id) AS subreddit_id

            FROM subs_selected_by_activity  AS top1
            FULL OUTER JOIN subs_selected_by_geo  AS geo1
                ON top1.subreddit_id = geo1.subreddit_id AND top1.subreddit_name = geo1.subreddit_name
            FULL OUTER JOIN ambassador_subs AS amb1
                ON top1.subreddit_id = amb1.subreddit_id AND top1.subreddit_name = amb1.subreddit_name
        ) AS merged
        FULL OUTER JOIN subs_selected_by_geo  AS geo
            ON merged.subreddit_id = geo.subreddit_id AND merged.subreddit_name = geo.subreddit_name
        FULL OUTER JOIN ambassador_subs AS amb
            ON merged.subreddit_id = amb.subreddit_id AND merged.subreddit_name = amb.subreddit_name
    ),

    subreddit_lookup_clean_text_meta AS (
        SELECT
            *
            , COALESCE(array_length(regexp_extract_all(clean_description, r"\b\w+\b")), 0)      AS subreddit_clean_description_word_count
            , array_length(regexp_extract_all(subreddit_name_title_public_description, r"\b\w+\b"))       AS subreddit_name_title_public_description_word_count
            -- do word count for full concat column on final query
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
            partition_date AS pt_date
            , sel.*

            -- TODO replace my old manual rating cols with the NEW ratings & tag columns
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

            , nt.rating_short
            , nt.rating_name
            , nt.primary_topic
            , array_to_string(secondary_topics,", ") as secondary_topics
            , array_to_string(mature_themes,", ") as mature_themes_list

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
            -- , slo.clean_description AS subreddit_clean_description
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
        LEFT JOIN subreddit_lookup_clean_text_meta AS slo
            ON sel.subreddit_name = LOWER(slo.name)
        LEFT JOIN (
            SELECT * FROM `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsourced_topic_and_rating`
            WHERE pt = partition_date
         ) AS nt
            ON sel.subreddit_id = nt.subreddit_id

        -- WHERE 1=1
            -- Re-apply minimum post count in case something unexpected happened in previous joins
            -- UPDATE: remove this check because some ambassador subs might get dropped by it
            -- AND asr.posts_l28 >= min_posts_geo_l28
    )


    -- SELECT for TABLE CREATION (or table preview)
    SELECT *
    FROM final_table
    ORDER BY users_l28 DESC, posts_l28 DESC
)  -- Close CREATE TABLE parens
;


-- Export data to google cloud storage (GCS)
-- CHANGE/Update:
--  1) URI date folder
--  2) source table
EXPORT DATA
    OPTIONS(
        uri='gs://i18n-subreddit-clustering/subreddits/top/2021-09-10/*.parquet',
        format='PARQUET',
        overwrite=true
    ) AS

    SELECT
        sel.*
    FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20210910` AS sel
    ORDER BY users_l28 DESC, subscribers DESC, posts_l28 DESC
;


-- ==============================
-- Check  final_table
-- SELECT
--     COUNT(*)    AS row_count
--     , COUNT(DISTINCT subreddit_name) AS subreddit_name_count
--     , COUNT(DISTINCT subreddit_id) AS subreddit_id_count
-- FROM final_table
-- ;


-- ###############
-- Tests/CHECKS
-- ###
-- Counts for all tables together
-- TODO(djb) maybe also add COUNT(DISTINCT subreddit_id) to check dupes
-- SELECT
--     (SELECT COUNT(*) FROM geo_subs_custom_raw) AS geo_subs_custom_raw_count
--     , (SELECT COUNT(*) FROM subs_selected_by_geo) AS subs_selected_by_geo_count
--     , (SELECT COUNT(*) FROM ambassador_subs) AS ambassador_subs_count
--     , (SELECT COUNT(*) FROM subs_selected_by_activity) AS subs_selected_by_activity_count
--     , (SELECT COUNT(*) FROM selected_subs) AS selected_subs_count
--     , (SELECT COUNT(*) FROM final_table) AS final_table_count
-- ;


-- Test selecting from subs_selected_by_geo subs
-- SELECT *
-- FROM subs_selected_by_geo AS geo
-- WHERE 1=1
--     -- AND geo.geo_relevant_country_count > 1

-- ORDER BY users_l7 DESC, posts_l28 DESC, geo.geo_relevant_countries
-- ;

-- Count subreddits for each country GEO
-- SELECT
--     geo_relevant_countries
--     -- , rating_name
--     , COUNT( DISTINCT subreddit_id) AS subreddit_count

-- FROM subs_selected_by_geo AS geo
-- GROUP BY 1
-- ORDER BY 1, 2 DESC
-- ;


-- Test subs count by ACTIVITY
-- SELECT
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count
-- FROM subs_selected_by_activity
-- ;


-- Check whether r/reggeaton makes the cut by activity...
-- SELECT
--     *
-- FROM subs_selected_by_activity
-- WHERE 1=1
--     AND subreddit_name LIKE "regg%"

-- ORDER BY subreddit_name
-- ;


-- Check ACTIVITY subreddits that are not rated as E
-- SELECT
--     -- COUNT(*)
--     *
-- FROM subs_selected_by_activity
-- WHERE 1=1
    -- need to add commented out cols for rating to show up
    -- AND COALESCE(rating_short, '') NOT IN ('E')
-- ORDER BY users_l7 DESC, posts_l28 DESC
-- ;


-- ==============================
-- Check selected sub count
--   NOTE that we need to do SELECT DISTINCT to remove duplicates
--   I moved the DISTINCT statement to the query itself so we shouldn't need it here anymore
-- SELECT
--     COUNT(*)    AS row_count
--     , COUNT(DISTINCT subreddit_name) AS subreddit_name_count
--     , COUNT(DISTINCT subreddit_id) AS subreddit_id_count
-- FROM selected_subs
-- ;

-- Check selected subs missing name & ID (duplicates)
-- We still need to do SELECT DISTINCT(*) to remove duplicates
-- SELECT
--     subreddit_name
--     , ROW_NUMBER () OVER (PARTITION BY subreddit_id ORDER BY subreddit_name) as sub_row_number
--     , * EXCEPT(subreddit_name )
-- -- FROM selected_subs
-- FROM (SELECT DISTINCT * FROM selected_subs) AS sel
-- WHERE 1=1
--     -- AND sel.ambassador_subreddit = true
--     AND sel.subreddit_name LIKE "vega%"
-- ORDER BY sub_row_number DESC, subreddit_name
-- ;

