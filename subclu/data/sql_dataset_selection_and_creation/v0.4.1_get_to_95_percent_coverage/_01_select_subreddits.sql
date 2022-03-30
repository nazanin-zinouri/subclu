-- noinspection SqlNoDataSourceInspectionForFile

-- Create table with selected subs for subs for topic modeling
--    For now, select subs with most views/posts & exclude those where over_18 = f
-- Filter NOTE:
--  over_18="f" set BY THE MODS! So we still might seem some NSFW subreddits
--  v0.3.2 pull we had 3,700 subs;
--  v0.4.0 ~19k subs
--  v0.4.1 ~50k subs
-- Old Notebook with EDA comparing subreddits b/n different versions
--  https://colab.research.google.com/drive/12GA7u_gWMlTCH4or9AKUm3u5mufMBshV#scrollTo=t2Z6E47Ohgde

-- CREATE TABLE with new selected subreddits for v0.4.1 topic model & clusters
DECLARE partition_date DATE DEFAULT '2021-12-14';
DECLARE regex_remove_str STRING DEFAULT r"https://|http://|www\.|/r/|\.html|reddit|\.com|\.org|#|\*";
DECLARE regex_replace_with_space_str STRING DEFAULT r"wiki/|/|-|_|\?|&nbsp;";

-- Besides making sure that English/high-activity communities have the `active` flag
--  we also make sure that there are a miniumn number of users in L7
DECLARE min_users_l7 NUMERIC DEFAULT 100;

-- All other i18n countries
--  From these, only India is expected to have a large number of English-language subreddits
--  Some i18n subs (like 1fcnuernberg) are only really active once a week b/c of game schedule
--   so they have few posts, but many comments. Add post + comment filter instead of only post
DECLARE min_users_geo_l7 NUMERIC DEFAULT 45;
DECLARE min_posts_geo_l28 NUMERIC DEFAULT 4;


-- Start CREATE TABLE statement
CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20211214`
AS (
    WITH
    -- Most of the logic has moved to a candidates table to make querying easier/faster

    -- We limit the count of posts to geo-relevant subs where we'll actually use it
    --  For highly active subs (usually English/US) we'll simply use CnC's "active" definition flag
    geo_candidate_subs_w_post_count AS (
        SELECT
            c.subreddit_id
            , c.subreddit_name
            , CASE
                WHEN COALESCE(c.geo_relevant_subreddit_v04, false) THEN true
                WHEN COALESCE(c.geo_relevant_subreddit, false) THEN true
                ELSE false
                END AS geo_relevant_subreddit_all
            , CASE
                WHEN (c.geo_relevant_countries_v04 IS NOT NULL) THEN c.geo_relevant_countries_v04
                WHEN COALESCE(c.geo_relevant_countries IS NOT NULL) THEN c.geo_relevant_countries
                ELSE NULL
                END AS geo_relevant_countries_all
            , CASE
                WHEN (c.ambassador_or_default_sub_germany = true) THEN true
                WHEN COALESCE(c.ambassador_or_default_sub_france = true) THEN true
                ELSE false
                END AS ambassador_or_default_any

            , COUNT(DISTINCT sp.post_id) as posts_not_removed_l28

        FROM `reddit-employee-datasets.david_bermejo.subclu_geo_subreddit_candidates_20211214` AS c
            LEFT JOIN (
                SELECT *
                FROM `data-prod-165221.cnc.successful_posts`
                WHERE (dt) BETWEEN (partition_date - 29) AND partition_date
                    AND removed = 0
            ) AS sp
                ON c.subreddit_id = sp.subreddit_id AND c.subreddit_name = sp.subreddit_name

        WHERE
            c.users_l7 >= 20
            AND c.posts_l28 >= 2
            AND (
                COALESCE(c.geo_relevant_subreddit_v04, false) = true
                OR COALESCE(c.geo_relevant_subreddit, false) = true
                OR COALESCE(c.ambassador_or_default_sub_germany, false) = true
                OR COALESCE(c.ambassador_or_default_sub_france, false) = true
            )
        GROUP BY 1, 2, 3, 4, 5
        -- ORDER BY 6 DESC
    ),

    -- Here's where we apply filters and update flags for: top (no geo), geo-top, & ambassador subs
    selected_subs AS (
        SELECT
            sel.subreddit_name
            , sel.subreddit_id
            , gc.geo_relevant_subreddit_all
            , gc.geo_relevant_countries_all
            , gc.ambassador_or_default_any
            , gc.posts_not_removed_l28
            , sel.* EXCEPT(subreddit_name, subreddit_id)

        FROM `reddit-employee-datasets.david_bermejo.subclu_geo_subreddit_candidates_20211214` AS sel
        LEFT JOIN geo_candidate_subs_w_post_count gc
            ON sel.subreddit_name = gc.subreddit_name AND sel.subreddit_id = gc.subreddit_id

        WHERE
            (
                sel.active = true
                AND sel.users_l7 >= min_users_l7
            )
            OR (
                (
                    gc.geo_relevant_subreddit_all = true
                    OR gc.ambassador_or_default_any = true
                )
                AND (
                    sel.users_l7 >= 50
                    AND gc.posts_not_removed_l28 >= 4
                )
            )
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

            -- Meta from lookup
            , slo.allow_top
            , slo.video_whitelisted
            , slo.lang      AS subreddit_language
            , slo.whitelist_status

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
        -- LEFT JOIN (
        --     -- Using sub-selection in case there are subs that haven't been registered in asr table
        --     SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
        --     WHERE DATE(pt) = partition_date
        -- ) AS asr
        --     ON sel.subreddit_name = asr.subreddit_name

        LEFT JOIN subreddit_lookup_clean_text_meta AS slo
            ON sel.subreddit_name = LOWER(slo.name)

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
--         uri='gs://i18n-subreddit-clustering/subreddits/top/2021-12-14/*.parquet',
--         format='PARQUET',
--         overwrite=true
--     ) AS
--     SELECT
--         sel.*
--     FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20211214` AS sel
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
