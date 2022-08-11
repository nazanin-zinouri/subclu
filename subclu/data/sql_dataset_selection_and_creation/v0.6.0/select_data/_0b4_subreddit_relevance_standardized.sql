-- Calculate standard score, rank, & final output for geo-relevant countries
-- Select pre-calculated columns & some newly calculated columns
--  - B: % of users by subreddit
--  - C: % of users by country
--  - D: subreddit rank for country (by % of users by subreddit)
--  - E: standardized(C, % of users by country)
-- for subs that meet minimum activity:
--  >= 4 posts in L28 days
--  >= 45 users (unique user screenviews) in L28 days

-- Lower these thresholds to get scores for low-activity subs
DECLARE MIN_POSTS_L28_NOT_REMOVED NUMERIC DEFAULT 2;
DECLARE MIN_USERS_L7 NUMERIC DEFAULT 10;

-- Set minimum thresholds for calculating standardized score
--  because some small islands screw up standard-dev scores
-- Rough percentiles:
--  p25:        10k
--  p50:        67k
--  p75:       610k
--  AVG:     2,700k (2.7 million)
DECLARE MIN_USERS_IN_COUNTRY_FOR_STDEV DEFAULT 11000;  -- Min coutry size to calculate stdev
DECLARE MIN_USERS_IN_COUNTRY DEFAULT 9000;  -- Min country size to SAVE to table. Monaco ~10k

-- Set weights for combined score
DECLARE STANDARD_VALUE_WHEN_STDEV_ZERO DEFAULT 9.0;
DECLARE PCT_BY_SUB_WEIGHT DEFAULT 0.8;
DECLARE PCT_BY_COUNTRY_WEIGHT DEFAULT 0.2;
DECLARE MAX_STDEV_CAP DEFAULT 5.0;
DECLARE ADJUSTMENT_TO_STDEV_FOR_COMBINED_SCORE DEFAULT (PCT_BY_COUNTRY_WEIGHT) / LN(1 + MAX_STDEV_CAP);

-- Set minimum thresholds for scores: b & e
--  Lower threshold => more "relevant" subreddits, but also more misses.
--  Higher gets fewer subs, but we're more confident on their locality
-- B: some general subs (soccer, cricket) wouldn't show up as relevent b/c their country visits are split between too many countries
--  if sub has 45 users_l28, % per country: 5/45 = 11.1%, 4/45 = 8.89%, 3/45 = 6.67%
DECLARE B_MIN_USERS_PCT_BY_SUB DEFAULT 0.14;
DECLARE E_MIN_USERS_PCT_BY_COUNTRY_STANDARDIZED DEFAULT 2.5;

-- For final table output, exclude subs that have too few users from a country,
--  otherwise each country could have 110k+ rows
DECLARE MIN_USERS_IN_SUBREDDIT_BY_COUNTRY DEFAULT 3;


CREATE OR REPLACE TABLE `reddit-relevance.${dataset}.subclu_subreddit_geo_relevance_standardized_${run_id}`
AS (
    WITH users_mean_and_stdev AS (
        -- get the mean, std, & country count for each subreddit
        SELECT
            subreddit_name
            , subreddit_id

            , COUNT(DISTINCT geo_country_code) as num_of_countries_with_visits_for_stdev_l28
            , AVG(users_percent_by_country_l28) as users_percent_by_country_avg
            , COALESCE(STDDEV(users_percent_by_country_l28), 0) as users_percent_by_country_stdev

            -- , AVG(users_percent_by_subreddit) as users_percent_by_subreddit_avg
            -- , STDDEV(users_percent_by_subreddit) as users_percent_by_subreddit_stdev
        FROM `reddit-relevance.${dataset}.subclu_subreddit_geo_relevance_raw_${run_id}` as geo
        WHERE
            -- subreddit filters
            posts_not_removed_l28 >= MIN_POSTS_L28_NOT_REMOVED
            AND users_l7 >= MIN_USERS_L7
            AND subreddit_name != 'profile'

            -- country size filters
            AND users_in_subreddit_from_country_l28 >= 1
            AND total_users_in_country_l28 >= MIN_USERS_IN_COUNTRY_FOR_STDEV
            -- AND geo_country_code IS NOT NULL
        GROUP BY 1, 2
    ),

    standard_score_and_rank_per_country AS (
        SELECT
            geo.subreddit_id
            , geo.subreddit_name
            , geo.geo_country_code

            -- B-metric = users_percent_by_subreddit
            , geo.users_percent_by_subreddit_l28
            -- E-metric = users_percent_by_country_standardized
            , CASE
                WHEN (m.users_percent_by_country_stdev = 0) THEN STANDARD_VALUE_WHEN_STDEV_ZERO
                ELSE (geo.users_percent_by_country_l28 - m.users_percent_by_country_avg) / m.users_percent_by_country_stdev
            END AS users_percent_by_country_standardized
            , geo.users_percent_by_country_l28
            , ROW_NUMBER() OVER (PARTITION BY geo_country_code ORDER BY users_percent_by_country_l28 DESC) AS users_percent_by_country_rank

            , geo.users_in_subreddit_from_country_l28
            , geo.total_users_in_country_l28
            , geo.total_users_in_subreddit_l28

            , m.* EXCEPT(subreddit_name, subreddit_id)
            , geo.pt
            , geo.users_l7
            , geo.posts_not_removed_l28

        FROM `reddit-relevance.${dataset}.subclu_subreddit_geo_relevance_raw_${run_id}` as geo
            LEFT JOIN users_mean_and_stdev AS m
                ON geo.subreddit_id = m.subreddit_id

        WHERE
            -- subreddit filters
            posts_not_removed_l28 >= MIN_POSTS_L28_NOT_REMOVED
            AND users_l7 >= MIN_USERS_L7
            AND geo.subreddit_name != 'profile'

            -- country filters
            AND users_in_subreddit_from_country_l28 >= 1
            AND total_users_in_country_l28 >= MIN_USERS_IN_COUNTRY

            -- get % for visits from a null country b/c it'd be good to know
            --  if there are subs that have a high # of visits w/o IP addresses
            -- AND geo_country_code IS NOT NULL
    ),
    merged_default_and_standardized_scores AS (
        SELECT
            s.users_percent_by_country_rank AS subreddit_rank_in_country
            , COALESCE(s.subreddit_id, base.subreddit_id)  AS subreddit_id
            , COALESCE(s.subreddit_name, base.subreddit_name)  AS subreddit_name
            , COALESCE(s.geo_country_code, base.geo_country_code)  AS geo_country_code

            , COALESCE(base.geo_relevance_default, False)  AS geo_relevance_default
            , (s.users_percent_by_subreddit_l28 >= B_MIN_USERS_PCT_BY_SUB) relevance_percent_by_subreddit
            , (s.users_percent_by_country_standardized >= E_MIN_USERS_PCT_BY_COUNTRY_STANDARDIZED ) relevance_percent_by_country_standardized

            -- calculate new combined score
            , CASE WHEN users_percent_by_country_standardized <= 0 THEN (
                    PCT_BY_SUB_WEIGHT * users_percent_by_subreddit_l28
                )
                WHEN (users_percent_by_country_standardized >= MAX_STDEV_CAP) THEN (
                    PCT_BY_COUNTRY_WEIGHT +
                    (PCT_BY_SUB_WEIGHT * users_percent_by_subreddit_l28)
                )
                ELSE (
                    -- We don't need to multiply the the weight b/c that's already taken into account in the adjustment constant
                    (ADJUSTMENT_TO_STDEV_FOR_COMBINED_SCORE * LN(1 + users_percent_by_country_standardized)) +
                    (PCT_BY_SUB_WEIGHT * users_percent_by_subreddit_l28)
                )
                END AS relevance_combined_score
            , s.users_percent_by_subreddit_l28
            , s.users_percent_by_country_standardized
            , s.users_in_subreddit_from_country_l28

            , s.num_of_countries_with_visits_for_stdev_l28
            , s.users_percent_by_country_l28
            , users_percent_by_country_avg
            , users_percent_by_country_stdev
            , COALESCE(s.pt, base.pt) AS pt

        FROM standard_score_and_rank_per_country AS s
            FULL OUTER JOIN (
                SELECT *
                FROM `reddit-relevance.${dataset}.subclu_subreddit_geo_score_default_${run_id}`
                WHERE 1=1
                    AND posts_not_removed_l28 >= MIN_POSTS_L28_NOT_REMOVED
                    AND users_l7 >= MIN_USERS_L7
            ) AS base
                ON s.subreddit_id = base.subreddit_id AND s.geo_country_code = base.geo_country_code

        WHERE 1=1
            AND s.users_in_subreddit_from_country_l28 >= MIN_USERS_IN_SUBREDDIT_BY_COUNTRY

            -- We can apply other filters AFTERWARDS. Exclude these from table
            --  creation because otherwise we'll need to re-run the whole table every time
            --  we want to compare why a subreddit didn't make the cut
    )


SELECT
    s.subreddit_rank_in_country
    , s.subreddit_id
    , s.geo_country_code
    , s.subreddit_name
    , COALESCE(cm.country_name, s.geo_country_code) AS country_name
    , s.* EXCEPT(
        subreddit_rank_in_country,
        subreddit_id, subreddit_name,
        geo_country_code
    )
FROM merged_default_and_standardized_scores AS s
    LEFT JOIN `reddit-relevance.${dataset}.countrycode_name_mapping` AS cm
        ON s.geo_country_code = cm.country_code

ORDER BY country_name ASC, subreddit_rank_in_country ASC
);  -- close create view parens
