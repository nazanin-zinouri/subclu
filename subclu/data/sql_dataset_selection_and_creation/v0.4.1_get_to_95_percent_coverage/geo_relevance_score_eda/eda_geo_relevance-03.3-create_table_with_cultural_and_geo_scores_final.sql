-- Use this view to calculate standard score, rank, & final output for geo-relevant countries
-- Select pre-calculated columns & some newly calculated columns
--  - B: % of users by subreddit
--  - C: % of users by country
--  - D: subreddit rank for country (by % of users by subreddit)
--  - E: standardized(C, % of users by country)
-- for subs that meet minimum activity:
--  >= 4 posts in L28 days
--  >= 45 users (unique user screenviews) in L28 days

DECLARE MIN_POSTS_L28_NOT_REMOVED NUMERIC DEFAULT 4;
DECLARE min_users_geo_l7 NUMERIC DEFAULT 45;

-- Set minimum thresholds for calculating standardized score
--  because Some small islands screw up standard scores
DECLARE MIN_USERS_IN_SUBREDDIT_BY_COUNTRY DEFAULT 4;
DECLARE MIN_USERS_IN_COUNTRY_FOR_STDEV DEFAULT 25000;  -- Min coutry size to calculate stdev
DECLARE MIN_USERS_IN_COUNTRY DEFAULT 5000;  -- Min country size to display
DECLARE STANDARD_VALUE_WHEN_STDEV_ZERO DEFAULT 4;

-- Set minimum thresholds for scores: b & e
DECLARE B_MIN_USERS_PCT_BY_SUB DEFAULT 0.14;
DECLARE E_MIN_USERS_PCT_BY_COUNTRY_STANDARDIZED DEFAULT 2.3;


CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_score_standardized_20220212`
AS (
    WITH users_mean_and_stdev AS (
        -- get the mean, std, & subreddit count for each subreddit
        SELECT
            subreddit_name
            , subreddit_id

            , COUNT(DISTINCT geo_country_code) as num_of_countries_with_visits_l28
            , AVG(users_percent_by_country) as users_percent_by_country_avg
            , COALESCE(STDDEV(users_percent_by_country), 0) as users_percent_by_country_stdev

            -- , AVG(users_percent_by_subreddit) as users_percent_by_subreddit_avg
            -- , STDDEV(users_percent_by_subreddit) as users_percent_by_subreddit_stdev
        FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_score_pct_of_country_20220212` as geo
        WHERE
            -- subreddit filters
            posts_not_removed_l28 >= MIN_POSTS_L28_NOT_REMOVED
            AND users_l7 >= min_users_geo_l7
            AND subreddit_name != 'profile'

            -- country filters
            AND users_in_subreddit_from_country >= MIN_USERS_IN_SUBREDDIT_BY_COUNTRY
            AND total_users_in_country >= MIN_USERS_IN_COUNTRY_FOR_STDEV
            AND geo_country_code IS NOT NULL
        GROUP BY 1, 2
    ),

    standard_score_and_rank_per_country AS (
        SELECT
            geo.subreddit_id
            , geo.subreddit_name
            -- some country names are null, so fill with code
            , COALESCE(geo.country_name, geo.geo_country_code) AS country_name

            , geo.users_percent_by_subreddit AS b_users_percent_by_subreddit
            , CASE
                WHEN (m.users_percent_by_country_stdev = 0) THEN STANDARD_VALUE_WHEN_STDEV_ZERO
                ELSE (geo.users_percent_by_country - m.users_percent_by_country_avg) / m.users_percent_by_country_stdev
            END AS e_users_percent_by_country_standardized
            , geo.users_percent_by_country AS c_users_percent_by_country
            , ROW_NUMBER() OVER (PARTITION BY country_name ORDER BY users_percent_by_country DESC) AS d_users_percent_by_country_rank

            , geo.users_in_subreddit_from_country AS users_in_subreddit_from_country_l28
            , geo.total_users_in_country AS total_users_in_country_l28
            , geo.total_users_in_subreddit AS total_users_in_subreddit_l28

            , geo.geo_country_code
            , geo.posts_not_removed_l28
            , geo.users_l7

            , m.* EXCEPT(subreddit_name, subreddit_id)

            , geo.* EXCEPT(
                views_dt_start, views_dt_end, pt,
                posts_not_removed_l28, users_l7,
                subreddit_id, subreddit_name, geo_country_code, country_name,
                users_in_subreddit_from_country, total_users_in_country, total_users_in_subreddit,
                users_percent_by_subreddit, users_percent_by_country
            )

        FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_score_pct_of_country_20220212` as geo
            LEFT JOIN users_mean_and_stdev AS m
                ON geo.subreddit_id = m.subreddit_id AND geo.subreddit_name = m.subreddit_name

        WHERE
            -- subreddit filters
            posts_not_removed_l28 >= MIN_POSTS_L28_NOT_REMOVED
            AND users_l7 >= min_users_geo_l7
            AND geo.subreddit_name != 'profile'

            -- country filters
            AND geo_country_code IS NOT NULL
            AND users_in_subreddit_from_country >= MIN_USERS_IN_SUBREDDIT_BY_COUNTRY
            AND total_users_in_country >= MIN_USERS_IN_COUNTRY
    )


SELECT
    COALESCE(s.subreddit_id, base.subreddit_id)  AS subreddit_id
    , COALESCE(s.subreddit_name, base.subreddit_name)  AS subreddit_name
    , COALESCE(s.country_name, base.country_name)  AS country_name
    , COALESCE(base.geo_relevance_default, False)  AS geo_relevance_default
    , s.b_users_percent_by_subreddit
    , s.e_users_percent_by_country_standardized
    , s.c_users_percent_by_country
    , s.d_users_percent_by_country_rank
    , (s.b_users_percent_by_subreddit >= B_MIN_USERS_PCT_BY_SUB) relevance_percent_by_subreddit
    , (s.e_users_percent_by_country_standardized >= E_MIN_USERS_PCT_BY_COUNTRY_STANDARDIZED ) relevance_percent_by_country_standardized

    , s.* EXCEPT(
        subreddit_id, subreddit_name,
        country_name, geo_region,
        b_users_percent_by_subreddit,
        e_users_percent_by_country_standardized,
        c_users_percent_by_country,
        d_users_percent_by_country_rank
    )
    , CURRENT_DATE() as pt

FROM standard_score_and_rank_per_country AS s
FULL OUTER JOIN (
    SELECT
        subreddit_id
        , subreddit_name
        , geo_country_code
        , country_name
        , True  AS geo_relevance_default
    FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_score_default_daily_20220212`
) AS base
    ON s.subreddit_id = base.subreddit_id AND s.geo_country_code = base.geo_country_code


WHERE 1=1
    AND posts_not_removed_l28 >= MIN_POSTS_L28_NOT_REMOVED
    AND users_l7 >= min_users_geo_l7
    AND (
        s.e_users_percent_by_country_standardized >= E_MIN_USERS_PCT_BY_COUNTRY_STANDARDIZED
        OR s.b_users_percent_by_subreddit >= B_MIN_USERS_PCT_BY_SUB
        -- Include geo-default subs for target countries, even if they're below the thresholds
        OR base.geo_relevance_default = True
        -- Do country filtering in a separate call
        -- OR (
        --     s.country_name IN (
        --         'Germany', 'Austria', 'Switzerland',
        --         'France', 'Spain', 'Italy',
        --         'Brazil', 'Portugal',
        --         'Mexico', 'Argentina', 'Chile',
        --         'Canada', 'United Kingdom', 'Australia', 'India'
        --     )
        --     -- Could use country code for places where country name might not be standardized (e.g., UK & US)
        --     -- OR geo_country_code IN ('CA', 'GB', 'AU')
        -- )
    )

ORDER BY users_l7 DESC, subreddit_name, c_users_percent_by_country DESC
);  -- close create view parens
