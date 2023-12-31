-- %%time
-- %%bigquery df_geo_new --project data-science-prod-218515

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

-- set minimum thresholds for calculating standardized score
DECLARE MIN_USERS_IN_SUBREDDIT_BY_COUNTRY DEFAULT 4;
DECLARE MIN_USERS_IN_COUNTRY DEFAULT 5000;  -- Some random islands screw up standard scores
DECLARE STANDARD_VALUE_WHEN_STDEV_ZERO DEFAULT 4;

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
    FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_score_pct_of_country_20220122` as geo
    WHERE
        -- subreddit filters
        posts_not_removed_l28 >= MIN_POSTS_L28_NOT_REMOVED
        AND users_l7 >= min_users_geo_l7
        AND subreddit_name != 'profile'

        -- country filters
        AND users_in_subreddit_from_country >= MIN_USERS_IN_SUBREDDIT_BY_COUNTRY
        AND total_users_in_country >= MIN_USERS_IN_COUNTRY
        AND geo_country_code IS NOT NULL
    GROUP BY 1, 2
),

standard_score_and_rank_per_country AS (
    SELECT
        geo.geo_country_code
        , geo.posts_not_removed_l28
        , geo.users_l7
        , geo.subreddit_id
        , geo.subreddit_name
        -- some country names are null, so fill with code
        , COALESCE(geo.country_name, geo.geo_country_code) AS country_name

        , geo.users_percent_by_subreddit AS b_users_percent_by_subreddit
        , geo.users_percent_by_country AS c_users_percent_by_country
        , ROW_NUMBER() OVER (PARTITION BY country_name ORDER BY users_percent_by_country DESC) AS d_subreddit_rank_by_percent_of_users_in_country
        , CASE
            WHEN (m.users_percent_by_country_stdev = 0) THEN STANDARD_VALUE_WHEN_STDEV_ZERO
            ELSE (geo.users_percent_by_country - m.users_percent_by_country_avg) / m.users_percent_by_country_stdev
        END AS e_users_percent_by_country_standardized

        , geo.users_in_subreddit_from_country AS users_in_subreddit_from_country_l28
        , geo.total_users_in_country AS total_users_in_country_l28
        , geo.total_users_in_subreddit AS total_users_in_subreddit_l28

        , m.* EXCEPT(subreddit_name, subreddit_id)

        , geo.* EXCEPT(
            views_dt_start, views_dt_end, pt,
            posts_not_removed_l28, users_l7,
            subreddit_id, subreddit_name, geo_country_code, country_name,
            users_in_subreddit_from_country, total_users_in_country, total_users_in_subreddit,
            users_percent_by_subreddit, users_percent_by_country
        )

    FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_score_pct_of_country_20220122` as geo
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
    s.* EXCEPT(geo_region)
FROM standard_score_and_rank_per_country AS s

WHERE
    posts_not_removed_l28 >= MIN_POSTS_L28_NOT_REMOVED
    AND users_l7 >= min_users_geo_l7
    AND (
        e_users_percent_by_country_standardized >= 0.3
        OR s.country_name IN (
            'Germany', 'Austria', 'Switzerland', 'India', 'France', 'Spain', 'Brazil', 'Portugal', 'Italy',
            'Mexico', 'Argentina', 'Chile',
            'Canada', 'United Kingdom', 'Australia',
            'United States'  # use U.S. as a check
        )
        -- Could use country code for places where country name might not be standardized (e.g., UK & US)
        -- OR geo_country_code IN ('CA', 'GB', 'AU')
    )
ORDER BY users_l7 DESC, subreddit_name, c_users_percent_by_country DESC
;
