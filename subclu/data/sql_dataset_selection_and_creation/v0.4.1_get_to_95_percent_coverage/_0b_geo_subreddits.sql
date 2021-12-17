-- Create new geo-relevant table that includes subreddits at a different threshold & time period
--  Because many i18n-relevant subreddits will NOT be active (they're too small
--  to make it into the regular table).
-- TODO: future work. Instead of % of users from a country in a subreddit:
--   % of users from a country that view the subreddit -- which sureddit(s) have a higher% of views in a country
-- Based on:
-- https://github.snooguts.net/reddit/data-science-airflow-etl/blob/master/dags/i18n/sql/geo_sfw_communities.sql
-- Notebook comparing different geo-relevant definitions
--   https://colab.research.google.com/drive/1dhVcrxnViiJFATQmPoPFI3l5ZqvReDtg#scrollTo=fyQYdk1VH2PX

-- Create new geo-relevant table that includes subreddits at a different threshold & time period
DECLARE active_pt_start DATE DEFAULT '2021-11-26';
DECLARE active_pt_end DATE DEFAULT '2021-12-10';
DECLARE regex_cleanup_country_name_str STRING DEFAULT r" of Great Britain and Northern Ireland| of America|";

-- Setting lower than 0.4 because some subreddits in LATAM
--  wouldn't show up as relevent b/c their country visits are split between too many countries
DECLARE min_pct_country NUMERIC DEFAULT 0.16;


CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_geo_subreddits_20211213`
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
