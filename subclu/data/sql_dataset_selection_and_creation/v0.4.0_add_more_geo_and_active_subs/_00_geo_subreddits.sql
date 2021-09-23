-- Create new geo-relevant table that includes subreddits NOT active
--  Because many i18n-relevant subreddits will NOT be active (they're too small
--  to make it into the regular table).
-- TODO: future work. Instead of % of users from a country in a subreddit:
--   % of users from a country that view the subreddit -- which sureddit(s) have a higher% of views in a country
-- Based on:
-- https://github.snooguts.net/reddit/data-science-airflow-etl/blob/master/dags/i18n/sql/geo_sfw_communities.sql

DECLARE active_pt_start DATE DEFAULT '2021-09-06';
DECLARE active_pt_end DATE DEFAULT '2021-09-20';
DECLARE regex_cleanup_country_name_str STRING DEFAULT r" of Great Britain and Northern Ireland| of America|";

-- Setting lower than 0.4 because some subreddits in LATAM
--  wouldn't show up as relevent b/c their country visits are split between too many countries
DECLARE min_pct_country NUMERIC DEFAULT 0.16;

-- To test activity filters
-- DECLARE min_users_geo_l7 NUMERIC DEFAULT 15;
-- DECLARE min_posts_geo_l28 NUMERIC DEFAULT 5;


CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_geo_subreddits_20210922`
AS (
WITH
    -- Get count of all users for each subreddit
    tot_subreddit AS (
        SELECT
            -- pt,
            subreddit_name,
            SUM(l1) AS total_users
        FROM `data-prod-165221.all_reddit.all_reddit_subreddits_daily` arsub
        WHERE pt BETWEEN TIMESTAMP(active_pt_start) AND TIMESTAMP(active_pt_end)
        GROUP BY subreddit_name  --, pt
    ),

    -- Add count of users PER COUNTRY
    geo_sub AS (
        SELECT
            -- tot.pt
            tot.subreddit_name
            , arsub.geo_country_code
            , tot.total_users
            , SUM(l1) AS users_country

        FROM `data-prod-165221.all_reddit.all_reddit_subreddits_daily` arsub
        LEFT JOIN tot_subreddit tot ON
            tot.subreddit_name = arsub.subreddit_name
            -- AND tot.pt = arsub.pt
        WHERE arsub.pt BETWEEN TIMESTAMP(active_pt_start) AND TIMESTAMP(active_pt_end)
        GROUP BY tot.subreddit_name, arsub.geo_country_code, tot.total_users --, tot.pt
    ),

    -- Keep only subreddits+country above the percent threshold
    filtered_subreddits AS (
        SELECT DISTINCT
            -- pt
            geo_sub.subreddit_name
            , total_users
            , geo_country_code
            , SAFE_DIVIDE(users_country, total_users) AS users_percent_in_country
        FROM geo_sub
        WHERE SAFE_DIVIDE(users_country, total_users) >= min_pct_country
    ),

    -- Merge with subreddit_lookup for additional filters
    --  Add country names (instead of only codes)
    final_geo_output AS (
        SELECT
            CURRENT_DATE() AS pt
            , LOWER(s.name) AS subreddit_name
            , s.subreddit_id
            , r.geo_country_code
            -- Split to remove long official names like:
            --   Tanzania, United Republic of; Bolivia, Plurinational State of
            -- Regex replace long names w/o a comma
            , REGEXP_REPLACE(
                SPLIT(cm.country_name, ', ')[OFFSET(0)],
                regex_cleanup_country_name_str, ""
            ) AS country_name
            , cm.region AS geo_region
            , r.users_percent_in_country
            , r.total_users
            , active_pt_start   AS views_dt_start
            , active_pt_end     AS views_dt_end
            , over_18
            , verdict
            , type

        FROM filtered_subreddits r
        INNER JOIN (
            SELECT *
            FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
            WHERE dt = DATE(active_pt_end)
        ) AS s
            ON LOWER(r.subreddit_name) = LOWER(s.name)

        LEFT JOIN `data-prod-165221.ds_utility_tables.countrycode_region_mapping` AS cm
            ON r.geo_country_code = cm.country_code

        -- No longer using the active flag
        -- INNER JOIN `data-prod-165221.ds_subreddit_whitelist_tables.active_subreddits` a ON
        --     LOWER(r.subreddit_name) = LOWER(a.subreddit_name)

        WHERE 1=1
            AND COALESCE(verdict, 'f') <> 'admin_removed'
            AND COALESCE(is_spam, FALSE) = FALSE
            AND COALESCE(over_18, 'f') = 'f'
            AND COALESCE(is_deleted, FALSE) = FALSE
            AND deleted IS NULL
            AND type IN ('public', 'private', 'restricted')
            AND NOT REGEXP_CONTAINS(LOWER(s.name), r'^u_.*')
            -- AND a.active = TRUE

        ORDER BY total_users DESC, subreddit_name, users_percent_in_country DESC
    )

-- Select for table creation
SELECT *
FROM final_geo_output

)  -- close CREATE TABLE statement
;


-- ===========================
-- Tests/checks for query
-- ===
-- final output COUNT
-- SELECT
--     COUNT(*)  AS row_count
--     , COUNT(DISTINCT subreddit_id)  AS subreddit_unique_count
--     , COUNT(DISTINCT country_name)  AS country_unique_count
-- FROM final_geo_output AS geo
-- LEFT JOIN `data-prod-165221.all_reddit.all_reddit_subreddits` AS asr
--     ON LOWER(geo.subreddit_name) = asr.subreddit_name
-- WHERE 1=1
--     -- country filters
--     AND (
--         country_name IN ('Germany', 'Austria', 'Switzerland', 'India', 'France', 'Spain', 'Brazil', 'Portugal', 'Italy')
--         OR geo_region = 'LATAM'
--         -- OR country_code IN ('CA', 'GB', 'AU')
--     )
--     -- activity filters
--     AND asr.users_l7 >= min_users_geo_l7
--     AND asr.posts_l28 >= min_posts_geo_l28
-- ;


-- Check geo_sub
--   All subreddits appear here
-- SELECT
--     *
--     , (users_country / total_users)  AS users_percent_by_country
-- FROM geo_sub
-- WHERE 1=1
--     -- David's filter specific subs
--     AND LOWER(subreddit_name ) IN (
--         'fussball', 'fifa_de', 'borussiadortmund', 'futbol', 'fcbayern',
--         'barca', 'realmadrid', 'psg'
--     )
--     AND users_country >= 88
-- ORDER BY subreddit_name, users_country DESC
-- ;


-- Check filtered subs
-- Expected: fifa_de & fussball
--      `borussiadortmund` gets dropped b/c no country is over 40%
-- Output: as expected :)
-- SELECT
--     *
-- FROM filtered_subreddits
-- WHERE 1=1
--     -- David's filter specific subs
--     AND LOWER(subreddit_name ) IN ('fussball', 'fifa_de', 'borussiadortmund', 'futbol', 'fcbayern')

-- ORDER BY subreddit_name, users_percent_by_country DESC
-- ;


-- Check final output
--  Expected: fifa_de, fussball
--  Output: fifa_de used to get drop b/c of old `active=true` filter
-- SELECT
--     *
-- FROM final_geo_output
-- WHERE 1=1
--     -- David's filter specific subs
--     -- AND LOWER(subreddit_name ) IN (
--     --     'fussball', 'fifa_de', 'borussiadortmund', 'futbol', 'soccer'
--     --     , 'dataisbeautiful', 'fcbayern'
--     --     )
--     AND geo_country_code NOT IN ("US", "GB")

-- LIMIT 10000
-- ;

-- Count subreddits per country
-- SELECT
--     geo_country_code
--     , country_name
--     , geo_region

--     , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count

-- FROM final_geo_output
-- WHERE 1=1
--     AND total_users >= 1000

--     -- David's filter specific subs
--     -- AND LOWER(subreddit_name ) IN (
--     --     'fussball', 'fifa_de', 'borussiadortmund', 'futbol', 'soccer'
--     --     , 'dataisbeautiful'
--     --     )
--     -- AND geo_country_code NOT IN ("US", "GB")

-- GROUP BY 1, 2, 3

-- ORDER BY subreddit_unique_count DESC
-- ;



-- WIP/SCRATCH FILTER OUT porn subreddits to select for clustering
-- use it for v0.4.0 filtering
SELECT
    rating_short
    , rating_name
    , primary_topic

    , geo.subreddit_name
    , geo.geo_country_code
    , geo.total_users
    , slo.whitelist_status AS ads_allowlist_status

    , array_to_string(secondary_topics,", ") as secondary_topics
    , array_to_string(mature_themes,", ") as mature_themes_list
    , rating_weight
    , geo.* EXCEPT (subreddit_name, geo_country_code, total_users)
    , nt.survey_version  AS tag_survey_version
    , nt.pt AS new_rating_pt

FROM `reddit-employee-datasets.david_bermejo.subclu_geo_subreddits_20210909` AS geo
LEFT JOIN `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsourced_topic_and_rating` AS nt
    ON nt.subreddit_id = geo.subreddit_id
LEFT JOIN (
    SELECT *
    FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
    WHERE dt = (CURRENT_DATE() - 2)
)AS slo
    ON geo.subreddit_id = slo.subreddit_id

WHERE 1=1
    AND nt.pt = (CURRENT_DATE() - 1)
    -- AND geo_country_code NOT IN ('US')
    -- AND geo_country_code IN ('MX', "DE")
    AND total_users >= 800
    AND (
        country_name IN ('Germany', 'Austria', 'Switzerland', 'India', 'France', 'Spain', 'Brazil', 'Portugal', 'Italy')
        OR geo_region = 'LATAM'
    )

    -- Test to see X, M, or unrated communities
    -- AND COALESCE(nt.rating_short, '') != 'E'

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

ORDER BY total_users DESC, subreddit_name
-- LIMIT 2000
;
