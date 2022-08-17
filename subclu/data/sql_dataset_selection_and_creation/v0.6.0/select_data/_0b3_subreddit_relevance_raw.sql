-- Get new geo-relevance score: % DAU per country (instead of per subreddit)
--  The table actually calculates both scores so we can compare side by side
--  * % of users from a country / subreddit users
--  * % of users from a country / users from country

DECLARE PARTITION_DATE DATE DEFAULT ${end_date};
-- For relevance date we want to keep it at last month only
DECLARE GEO_PT_START DATE DEFAULT PARTITION_DATE - 29;

-- Set min-users-in-sub to 1 because there could be baseline subs with only one visit
--  We can remove low-activity subs downstream
DECLARE MIN_USERS_IN_SUBREDDIT_FROM_COUNTRY NUMERIC DEFAULT 1;

CREATE OR REPLACE TABLE `reddit-relevance.${dataset}.subclu_subreddit_geo_relevance_raw_${run_id}`
AS (
    WITH tot_subreddit AS (
        -- Get count of all UNIQUE users for each subreddit
        SELECT
            subreddit_name,
            COUNT(DISTINCT user_id) AS total_users_in_subreddit_l28
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
            , COUNT(DISTINCT user_id) as total_users_in_country_l28
        FROM (
            SELECT
                -- tot.pt
                arsub.user_id
                , arsub.geo_country_code

            -- arsub gives us info at subreddit_name + user_id
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
            arsub.subreddit_name
            , arsub.geo_country_code
            , tot2.total_users_in_subreddit_l28
            , tot.total_users_in_country_l28
            -- Fixed: need UNIQUE users, to stop double counting someone who visits multiple times in a month
            , COUNT(DISTINCT user_id) AS users_in_subreddit_from_country_l28

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
            arsub.subreddit_name, arsub.geo_country_code, tot.total_users_in_country_l28,
            tot2.total_users_in_subreddit_l28
    ),
    -- Keep only subreddits+country above min user threshold
    --  Merge with subreddit_lookup for additional filters
    filtered_subreddits AS (
        SELECT DISTINCT
            c.pt
            , c.subreddit_id
            , c.subreddit_name
            , geo_country_code

            , SAFE_DIVIDE(users_in_subreddit_from_country_l28, total_users_in_subreddit_l28) AS users_percent_by_subreddit_l28
            , SAFE_DIVIDE(users_in_subreddit_from_country_l28, total_users_in_country_l28) AS users_percent_by_country_l28

            , users_in_subreddit_from_country_l28
            , total_users_in_subreddit_l28
            , total_users_in_country_l28

            -- Keep users_l7 & posts_not_removed b/c I might need to filter subs w/o re-joining
            , c.users_l7
            , c.posts_not_removed_l28

        FROM geo_sub AS geo
            -- Keep only subs that have at least 1 view & 1 unremoved post in L28
            -- This table already has SLO data so we need one fewer join
            INNER JOIN `reddit-relevance.${dataset}.subclu_subreddit_candidates_${run_id}` AS c
                ON LOWER(geo.subreddit_name) = c.subreddit_name

        WHERE 1=1
            AND users_in_subreddit_from_country_l28 >= MIN_USERS_IN_SUBREDDIT_FROM_COUNTRY

            -- For v0.6.0 keep all subs b/c we want to be able to predict their rating & topic(s)
            -- AND COALESCE(verdict, 'f') <> 'admin-removed'
            -- AND COALESCE(is_spam, FALSE) = FALSE
            -- AND COALESCE(is_deleted, FALSE) = FALSE
            -- AND deleted IS NULL
            -- AND type IN ('public', 'private', 'restricted')
            AND NOT REGEXP_CONTAINS(LOWER(c.subreddit_name), r'^u_.*')
            -- We can remove over_18 later.
            --   Keep b/c we might want to include recommendations for NSFW listing below
            -- AND COALESCE(over_18, 'f') = 'f'
    )


    -- final selection
    SELECT
        geo.*
    FROM filtered_subreddits AS geo

    ORDER BY geo_country_code ASC, users_in_subreddit_from_country_l28 DESC
);  -- Close create table parens
