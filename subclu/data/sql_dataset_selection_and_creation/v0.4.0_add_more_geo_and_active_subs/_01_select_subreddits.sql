-- noinspection SqlNoDataSourceInspectionForFile

-- Create table with selected subs for subs with a active flag &/or an activity threshold
--    For now, select subs with most views/posts & exclude those where over_18 = f
-- Filter NOTE:
--  over_18="f" set BY THE MODS! So we still might seem some NSFW subreddits
-- TODO(djb) in v0.3.2 pull we had 3,700 subs; now we're getting ~19k subs
-- Notebook with EDA comparing subreddits b/n different versions
--  https://colab.research.google.com/drive/12GA7u_gWMlTCH4or9AKUm3u5mufMBshV#scrollTo=t2Z6E47Ohgde

-- CREATE TABLE with new selected subreddits for v0.4.0 clustering
DECLARE partition_date DATE DEFAULT '2021-09-21';
DECLARE regex_remove_str STRING DEFAULT r"https://|http://|www\.|/r/|\.html|reddit|\.com|\.org";
DECLARE regex_replace_with_space_str STRING DEFAULT r"wiki/|/|-|_|\?|&nbsp;";
DECLARE min_users_l7 NUMERIC DEFAULT 2000;
DECLARE min_posts_l28 NUMERIC DEFAULT 90;
DECLARE min_comments_l28 NUMERIC DEFAULT 3;
DECLARE min_posts_plus_comments_top_l28 NUMERIC DEFAULT 250;

-- eng-i18n = Canada, UK, Australia
DECLARE min_users_eng_i18n_l7 NUMERIC DEFAULT 2000;
DECLARE min_posts_eng_i18n_l28 NUMERIC DEFAULT 80;

-- All other i18n countries
--  From these, only India is expected to have a large number of English-language subreddits
--  Some i18n subs (like 1fcnuernberg) are only really active once a week b/c of game schedule
--   so they have few posts, but many comments. Add post + comment filter instead of only post
DECLARE min_users_geo_l7 NUMERIC DEFAULT 32;
DECLARE min_posts_geo_l28 NUMERIC DEFAULT 9;
DECLARE min_posts_plus_comments_geo_l28 NUMERIC DEFAULT 35;


-- Start CREATE TABLE statement
CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20210924`
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
        FROM `reddit-employee-datasets.david_bermejo.subclu_geo_subreddits_20210922` AS geo2
        WHERE 1=1
            AND (
                country_name IN ('Germany', 'Austria', 'Switzerland', 'India', 'France', 'Spain', 'Brazil', 'Portugal', 'Italy')
                OR geo_region = 'LATAM'
                -- eng-i18n =  Canada, UK, Australia
                OR geo_country_code IN ('CA', 'GB', 'AU')
            )
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
        LEFT JOIN (
            SELECT * FROM `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsourced_topic_and_rating`
            WHERE pt = partition_date
        ) AS nt
            ON geo.subreddit_id = nt.subreddit_id

        WHERE 1=1
            -- Drop duplicated country names
            AND geo.sub_geo_rank_no = 1

            -- remove quarantine filter, b/c if we score them we might be able to clusters
            --   of subreddits that are similar to previoiusly quarantined subs
            -- AND slo.quarantine = false

            -- Apply activity by geo:
            AND (
                (
                    (
                        country_name IN ('Germany', 'Austria', 'Switzerland', 'India', 'France', 'Spain', 'Brazil', 'Portugal', 'Italy')
                        OR region = 'LATAM'
                    )
                    AND asr.users_l7 >= min_users_geo_l7
                    AND asr.posts_l28 >= min_posts_geo_l28
                )
                OR (
                    (
                        country_name IN ('Germany', 'Austria', 'Switzerland', 'India', 'France', 'Spain', 'Brazil', 'Portugal', 'Italy')
                        OR region = 'LATAM'
                    )
                    AND asr.users_l7 >= min_users_geo_l7
                    AND (asr.posts_l28 + asr.comments_l28) >= min_posts_plus_comments_geo_l28
                )
                OR (
                    country_code IN ('CA', 'GB', 'AU')
                    AND asr.users_l7 >= min_users_eng_i18n_l7
                    AND asr.posts_l28 >= min_posts_eng_i18n_l28
                )
            )

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
        -- ORDER BY geo.subreddit_name
    ),

    ambassador_subs AS (
        -- Wacy's table pulls data from a spreadsheet that Alex updates
        SELECT
            LOWER(amb.subreddit_name)           AS subreddit_name
            , slo.subreddit_id

        FROM `reddit-employee-datasets.wacy_su.ambassador_subreddits` AS amb
        LEFT JOIN (
            SELECT * FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
            WHERE dt = partition_date
        ) AS slo
            ON LOWER(amb.subreddit_name) = LOWER(slo.name)
        WHERE amb.subreddit_name IS NOT NULL
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
        LEFT JOIN (
            SELECT * FROM `data-prod-165221.ds_subreddit_whitelist_tables.active_subreddits`
            WHERE DATE(_PARTITIONTIME) = partition_date
        ) AS acs
            ON asr.subreddit_name = acs.subreddit_name
        LEFT JOIN (
            SELECT * FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
            WHERE dt = partition_date
        ) AS slo
            ON asr.subreddit_name = LOWER(slo.name)
        LEFT JOIN (
            SELECT * FROM `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsourced_topic_and_rating`
            WHERE pt = partition_date
        ) AS nt
            ON acs.subreddit_id = nt.subreddit_id

        WHERE 1=1
            AND DATE(asr.pt) = partition_date

            -- remove quarantine filter, b/c if we score them we might be able to clusters
            --   of subreddits that are similar to previoiusly quarantined subs
            -- AND slo.quarantine = false
            AND asr.users_l7 >= min_users_l7
            AND (
                (
                    asr.posts_l28 >= min_posts_l28
                    AND asr.comments_l28 >= min_comments_l28
                )
                OR ((asr.posts_l28 + asr.comments_l28) >= min_posts_plus_comments_top_l28)
            )

            -- Need COALESCE because thousands of subs have an empty field
            AND COALESCE(slo.over_18, 'f') = 'f'

            -- For the high-activity subs, keep the active flag?
            -- AND COALESCE(acs.active, False) = True

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
    -- Split into 2 steps:
    --  1st: get the distinct sub-name & sub-ID
    --  2nd: get flags for why a sub qualified (join with geo-relevant & ambassador tables)
    selected_subs_base AS (
        SELECT
            COALESCE(top1.subreddit_name, geo1.subreddit_name, amb1.subreddit_name)  AS subreddit_name
            , COALESCE(top1.subreddit_id, geo1.subreddit_id, amb1.subreddit_id) AS subreddit_id

        FROM subs_selected_by_activity  AS top1
        FULL OUTER JOIN subs_selected_by_geo  AS geo1
            ON top1.subreddit_id = geo1.subreddit_id AND top1.subreddit_name = geo1.subreddit_name
        FULL OUTER JOIN ambassador_subs AS amb1
            ON top1.subreddit_id = amb1.subreddit_id AND top1.subreddit_name = amb1.subreddit_name
    ),
    selected_subs AS (
        SELECT
            sel.subreddit_name
            , sel.subreddit_id
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
        -- Join to find why a subreddit qualified
        FROM (SELECT DISTINCT * FROM selected_subs_base) AS sel
        FULL OUTER JOIN subs_selected_by_geo  AS geo
            ON sel.subreddit_id = geo.subreddit_id AND sel.subreddit_name = geo.subreddit_name
        FULL OUTER JOIN ambassador_subs AS amb
            ON sel.subreddit_id = amb.subreddit_id AND sel.subreddit_name = amb.subreddit_name

        -- We can get null IDS if an ambassador subreddit is planned but not created yet
        WHERE sel.subreddit_id IS NOT NULL
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


-- ==============================
-- Check  final_table COUNTS
-- SELECT
--     COUNT(*)    AS row_count
--     , COUNT(DISTINCT subreddit_name) AS subreddit_name_count
--     , COUNT(DISTINCT subreddit_id) AS subreddit_id_count
--     , SUM(
--         CASE
--             WHEN (ambassador_subreddit = true) THEN 1
--             ELSE 0
--         END
--     ) AS ambassador_subreddit_count
--     , SUM(
--         CASE
--             WHEN (geo_relevant_subreddit = true) THEN 1
--             ELSE 0
--         END
--     ) AS geo_relevant_subreddit_count
-- FROM final_table
-- ;


-- Counts for all tables together
-- SELECT
--     (SELECT COUNT(*) FROM geo_subs_custom_raw) AS geo_subs_custom_raw_count
--     , (SELECT COUNT(*) FROM subs_selected_by_geo) AS subs_selected_by_geo_count
--     , (SELECT COUNT(*) FROM ambassador_subs) AS ambassador_subs_count  -- Expect 173+
    -- , (SELECT COUNT(*) FROM subs_selected_by_activity) AS subs_selected_by_activity_count_rows
    -- , (SELECT COUNT(*) FROM selected_subs_base) AS selected_subs_BASE_count_rows
    -- , (SELECT COUNT(DISTINCT subreddit_id) FROM selected_subs_base) AS selected_subs_BASE_unique_sub_ids_count
    -- , (SELECT COUNT(*) FROM selected_subs) AS selected_subs_count_rows
    -- , (SELECT COUNT(DISTINCT subreddit_id) FROM selected_subs) AS selected_subs_unique_sub_ids_count
--     , (SELECT COUNT(*) FROM final_table) AS final_table_rows_count
--     , (SELECT COUNT(DISTINCT subreddit_id) FROM final_table) AS final_table_sub_id_unique_count
-- ;

-- TODO(djb): Check that all ambassador subs are in


-- ==============================
-- Export data to google cloud storage (GCS)
-- CHANGE/Update:
--  1) URI date folder
--  2) source table
-- EXPORT DATA
--     OPTIONS(
--         uri='gs://i18n-subreddit-clustering/subreddits/top/2021-09-24/*.parquet',
--         format='PARQUET',
--         overwrite=true
--     ) AS

--     SELECT
--         sel.*
--     FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20210924` AS sel
--     ORDER BY users_l28 DESC, subscribers DESC, posts_l28 DESC
-- ;


-- ###############
-- Tests/CHECKS
-- ###
-- Test selecting from subs_selected_by_geo subs
-- SELECT *
-- FROM subs_selected_by_geo AS geo
-- WHERE 1=1
--     -- AND geo.geo_relevant_country_count > 1

-- ORDER BY users_l7 DESC, posts_l28 DESC, geo.geo_relevant_countries
-- ;

-- Count subreddits for each country GEO COUNTRY GROUPS
--  Output: 1 row per subreddit
-- SELECT
--     geo_relevant_countries
--     -- , rating_name
--     , COUNT( DISTINCT subreddit_id) AS subreddit_count

-- FROM subs_selected_by_geo AS geo
-- GROUP BY 1
-- ORDER BY 1, 2 DESC
-- ;

-- TODO(djb): Count subreddits for each country individually
--  a subreddit can be counted in multiple countries
-- Test subs count by ACTIVITY
-- SELECT
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count
-- FROM subs_selected_by_activity
-- ;


-- Check whether r/reggeaton makes the cut by activity...
--  does it make the cut in selected? & final?
-- SELECT
--     *
-- FROM subs_selected_by_activity
-- FROM selected_subs
-- FROM final_table
-- WHERE 1=1
--     AND subreddit_name LIKE "regg%"
--     OR subreddit_name LIKE "goodn%"

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
-- Check AMBASSADOR subreddits
--   For some reason we're dropping a few of them, like 'platzreife'
-- Check ambassadors subs are in base table
--  platzreife appears here
-- SELECT
--     *
-- FROM selected_subs_base
-- WHERE 1=1
--     AND subreddit_name IN ('platzreife', 'vegetarischkochen', 'heutelernteich', 'fussball', 'formel1')
-- ORDER BY subreddit_name
-- ;


-- Check ambassadors subs are in selected_subs table
--  platzreife: yes it's also here
-- SELECT
--     *
-- FROM selected_subs
-- WHERE 1=1
--     AND subreddit_name IN ('platzreife', 'vegetarischkochen', 'heutelernteich', 'fussball', 'formel1')
-- ORDER BY subreddit_name
-- ;

-- Check rows with missing subreddit_id
-- SELECT
--     *
-- FROM selected_subs
-- WHERE 1=1
--     AND subreddit_ID IS NULL
-- ORDER BY subreddit_name
-- ;


-- Check ambassadors subs are in selected_subs table
--  platzreife: yes it's also here
-- SELECT
--     *
-- FROM selected_subs
-- WHERE 1=1
--     AND subreddit_name IN ('platzreife', 'vegetarischkochen', 'heutelernteich', 'fussball', 'formel1')
-- ORDER BY subreddit_name
-- ;


-- Most of these other geo-relevant subs don't appear in:
--   `subs_selected_by_geo` OR `selected_subs`
--  whyyyy? A: looks like they had too few L7 users &/or too few posts in l28
-- SELECT
--     *
-- -- FROM geo_subs_custom_raw
-- FROM subs_selected_by_geo
-- -- FROM selected_subs

-- WHERE 1=1
--     -- AND subreddit_name IN ('platzreife', 'vegetarischkochen', 'heutelernteich', 'fussball', 'formel1')
--     AND LOWER(subreddit_name) IN (
--         '1fcnuernberg'
--         , 'de_it'
--         , 'de_podcasts'
--         , 'kurrent'
--         , 'luftraum'
--         , 'samplesize_dach'
--         , 'spabiergang'
--         , 'weltraum'
--         , 'zombey'
--         )
-- ORDER BY subreddit_name
-- ;



-- Try separate join of base tables to see if things are missing there
-- SELECT
--     COALESCE(geo.subreddit_name, amb.subreddit_name)  AS subreddit_name
--     , COALESCE(geo.subreddit_id, amb.subreddit_id) AS subreddit_id
--     , geo.subreddit_name
--     , geo.subreddit_id
--     , geo.geo_relevant_countries

-- FROM ambassador_subs AS amb
-- FULL OUTER JOIN subs_selected_by_geo AS geo
--     ON amb.subreddit_name = geo.subreddit_name
-- WHERE 1=1
--     AND amb.subreddit_name IN ('platzreife', 'vegetarischkochen', 'heutelernteich', 'fussball', 'formel1')
-- ORDER BY amb.subreddit_name
-- ;

-- Join with top subs... why would this cause the sub to get dropped?
-- SELECT
--     COALESCE(top.subreddit_name, geo.subreddit_name, amb.subreddit_name)  AS subreddit_name
--     , COALESCE(top.subreddit_name, geo.subreddit_name, amb.subreddit_id) AS subreddit_id
--     , geo.subreddit_name
--     , geo.subreddit_id
--     , geo.geo_relevant_countries
--     , top.*

-- FROM ambassador_subs AS amb
-- FULL OUTER JOIN subs_selected_by_geo AS geo
--     ON amb.subreddit_name = geo.subreddit_name
-- FULL OUTER JOIN subs_selected_by_activity AS top
--     ON amb.subreddit_name = top.subreddit_name
-- WHERE 1=1
--     AND amb.subreddit_name IN ('platzreife', 'vegetarischkochen', 'heutelernteich', 'fussball', 'formel1')
-- ORDER BY amb.subreddit_name
-- ;


-- ==============================
-- Check selected sub count
--   NOTE that we need to do SELECT DISTINCT on selected_subs_base to remove duplicates
--   Duplicates can come up when a sub qualifies for multiple reasons
-- SELECT
--     COUNT(*)    AS row_count
--     , COUNT(DISTINCT subreddit_name) AS subreddit_name_count
--     , COUNT(DISTINCT subreddit_id) AS subreddit_id_count
--     , SUM(
--         CASE
--             WHEN (ambassador_subreddit = true) THEN 1
--             ELSE 0
--         END
--     ) AS ambassador_subreddit_count
--     , SUM(
--         CASE
--             WHEN (geo_relevant_subreddit = true) THEN 1
--             ELSE 0
--         END
--     ) AS geo_relevant_subreddit_count
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
