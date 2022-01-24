-- Get new geo-relevance score: % DAU per country (instead of per subreddit)
--  The table actually calculates both scores so we can compare side by side

DECLARE PARTITION_DATE DATE DEFAULT '2022-01-22';
DECLARE GEO_PT_START DATE DEFAULT PARTITION_DATE - 29;

DECLARE regex_cleanup_country_name_str STRING DEFAULT r" of Great Britain and Northern Ireland| of America|";

-- Setting lower includes more subreddits, do EDA to figure out what's a good threshold
--  b/c some general subs (soccer, cricket) wouldn't show up as relevent b/c their country visits are split between too many countries
-- Previously we had a minimum of 45 users for geo-relevant subs, so 5/45 = 11.1%, 4/45 = 8.89%, 3/45 = 6.67%
DECLARE MIN_USERS_IN_SUBREDDIT_FROM_COUNTRY NUMERIC DEFAULT 3;

CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_score_pct_of_country_20220122`
AS (
    WITH
        -- Get count of all users for each subreddit
        tot_subreddit AS (
            SELECT
                -- pt,
                subreddit_name,
                SUM(l1) AS total_users_in_subreddit
            FROM `data-prod-165221.all_reddit.all_reddit_subreddits_daily` arsub
            WHERE pt BETWEEN TIMESTAMP(GEO_PT_START) AND TIMESTAMP(PARTITION_DATE)
                AND subreddit_name IS NOT NULL
            GROUP BY subreddit_name
        ),
        -- Get Unique of users PER COUNTRY
        -- NOTE: A user could be active in multiple countries (e.g., VPN or traveling)
        unique_users_per_country AS (
            SELECT
                geo_country_code
                , COUNT(DISTINCT user_id) as total_users_in_country
            FROM (
                SELECT
                    -- tot.pt
                    arsub.user_id
                    , arsub.geo_country_code

                    -- , SUM(l1) AS total_subreddits_visited  --l1 = 1 if: user active on that sub on that day

                -- arsub gives us info at subreddit_id + user_id
                --  so we need stop double counting by grouping by user_id + country
                FROM `data-prod-165221.all_reddit.all_reddit_subreddits_daily` arsub
                WHERE arsub.pt BETWEEN TIMESTAMP(GEO_PT_START) AND TIMESTAMP(PARTITION_DATE)
                    AND l1 = 1
                GROUP BY arsub.geo_country_code, arsub.user_id
            )
            GROUP BY 1
        ),
        -- Add count of users in subreddit PER COUNTRY & PER SUBREDDIT
        geo_sub AS (
            SELECT
                -- tot.pt
                arsub.subreddit_name
                , arsub.geo_country_code
                , tot2.total_users_in_subreddit
                , tot.total_users_in_country
                , SUM(l1) AS users_in_subreddit_from_country

            -- subreddit_name can be null, so exclude those
            FROM (
                SELECT *
                FROM `data-prod-165221.all_reddit.all_reddit_subreddits_daily`
                WHERE subreddit_name IS NOT NULL
            ) AS arsub
            LEFT JOIN unique_users_per_country tot ON
                tot.geo_country_code = arsub.geo_country_code
            LEFT JOIN tot_subreddit AS tot2
                ON arsub.subreddit_name = tot2.subreddit_name

            WHERE arsub.pt BETWEEN TIMESTAMP(GEO_PT_START) AND TIMESTAMP(PARTITION_DATE)

            GROUP BY
                arsub.subreddit_name, arsub.geo_country_code, tot.total_users_in_country,
                tot2.total_users_in_subreddit
        ),
        -- Keep only subreddits+country above the percent threshold
        filtered_subreddits AS (
            SELECT DISTINCT
                -- pt
                geo_sub.subreddit_name
                , total_users_in_subreddit
                , total_users_in_country
                , users_in_subreddit_from_country
                , geo_country_code
                , SAFE_DIVIDE(users_in_subreddit_from_country, total_users_in_country) AS users_percent_by_country
                , SAFE_DIVIDE(users_in_subreddit_from_country, total_users_in_subreddit) AS users_percent_by_subreddit
            FROM geo_sub
            WHERE users_in_subreddit_from_country >= MIN_USERS_IN_SUBREDDIT_FROM_COUNTRY
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
                , r.users_percent_by_country
                , r.users_percent_by_subreddit
                , r.users_in_subreddit_from_country
                , r.total_users_in_country
                , r.total_users_in_subreddit
                , GEO_PT_START AS views_dt_start
                , PARTITION_DATE        AS views_dt_end
                , over_18
                , verdict
                , type

            FROM filtered_subreddits r
            INNER JOIN (
                SELECT *
                FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
                WHERE dt = DATE(PARTITION_DATE)
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

        )

    -- final selection
    SELECT
        posts_not_removed_l28
        , users_l7
        , geo.*
    FROM final_geo_output AS geo
        LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_candidates_posts_no_removed_20220122` AS c
            ON geo.subreddit_id = c.subreddit_id

    ORDER BY total_users_in_subreddit DESC, subreddit_name, users_percent_by_country DESC
);  -- Close create table parens


-- Check filtered subreddits
-- SELECT
--     *
-- FROM filtered_subreddits
-- ORDER BY users_in_subreddit_from_country ASC
