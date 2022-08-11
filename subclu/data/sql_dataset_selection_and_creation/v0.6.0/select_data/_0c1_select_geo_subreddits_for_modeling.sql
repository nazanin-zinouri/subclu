-- Create stand-alone table for subs selected because of geo-relevance

-- Initial filter for activity (also depends on previous tables)
DECLARE MIN_USERS_L7 NUMERIC DEFAULT 25;
DECLARE MIN_POSTS_L28 NUMERIC DEFAULT 4;


CREATE OR REPLACE TABLE `reddit-relevance.${dataset}.subclu_subreddit_geo_selected_${run_id}`
AS (
    WITH
    subs_i18n_core_agg AS (
        -- Get subreddits flagged as important to i18n + the countries they're relevant to
        --  Include them even if they are below activity thresholds
        SELECT
            geo.subreddit_id

            -- Order so that we get (Mexico, US) only, and not both: (US, Mexico) AND (Mexico, US)
            , STRING_AGG(geo.country_name, ', ' ORDER BY geo.country_name) AS geo_relevant_countries
            , STRING_AGG(geo.geo_country_code, ', ' ORDER BY geo.geo_country_code) AS geo_relevant_country_codes
            , COUNT(geo.geo_country_code) AS geo_relevant_country_count
        FROM `reddit-relevance.${dataset}.subclu_subreddit_candidates_${run_id}` AS ssc
            LEFT JOIN `reddit-relevance.${dataset}.subclu_subreddit_geo_relevance_standardized_${run_id}` AS geo
                ON geo.subreddit_id = ssc.subreddit_id
        WHERE 1=1
            -- Keep subs that are flagged for i18n
            AND ssc.i18n_type IS NOT NULL

            -- Pick countries that qualify under at least one of the thresholds
            AND (
                geo_relevance_default = TRUE
                OR relevance_percent_by_subreddit = TRUE
                -- For now, set it as default value so we can include more subreddits
                --   if needed, set a higher (>=3.0) threshold AFTERWARDS
                --   to remove noise from non-local subs
                OR relevance_percent_by_country_standardized = TRUE
            )
        GROUP BY 1
    ),
    subs_geo_custom_agg AS (
        -- Select subreddits that meet country + relevance + activity thresholds
        SELECT
            geo.subreddit_id

            -- Order so that we get (Mexico, US) only, and not both: (US, Mexico) AND (Mexico, US)
            , STRING_AGG(geo.country_name, ', ' ORDER BY geo.country_name) AS geo_relevant_countries
            , STRING_AGG(geo.geo_country_code, ', ' ORDER BY geo.geo_country_code) AS geo_relevant_country_codes
            , COUNT(geo.geo_country_code) AS geo_relevant_country_count
        FROM `reddit-relevance.${dataset}.subclu_subreddit_geo_relevance_standardized_${run_id}` AS geo
            LEFT JOIN `reddit-relevance.${dataset}.subclu_subreddit_candidates_${run_id}` AS ssc
                ON geo.subreddit_id = ssc.subreddit_id

        WHERE 1=1
            -- Pick subs above activity threshold
            AND (
                users_l7 >= MIN_USERS_L7
                AND posts_not_removed_l28 >= MIN_POSTS_L28
            )
            -- Pick subreddits that qualify under at least one metric/threshold
            --   For now, set use default upstream values. We can always raise it when
            --   creating recommendations later
            AND (
                geo_relevance_default = TRUE
                OR relevance_percent_by_subreddit = TRUE
                OR relevance_percent_by_country_standardized = TRUE
                -- Try the combined score to include a few more subreddits
                OR relevance_combined_score >= 0.17
            )
            -- pick subs that are relevant to target countries
            AND (
                -- tier 0
                geo.geo_country_code IN ('GB','AU','CA')

                -- tier 1
                OR geo.geo_country_code IN ('DE','FR','BR','MX','IN')

                -- tier 2 - only some subs from t2
                OR geo.geo_country_code IN (
                    'IT', 'ES'
                    , 'NL', 'RO'
                    , 'DK', 'SE', 'FI'
                    , 'PH', 'TR', 'PL', 'RU'
                    -- Exclude, no support expected in 2022
                    , 'JP'
                    , 'ID', 'KR'
                )
                -- Other top 50/companion countries
                --  Brazil ~ Portugal
                --  Germany ~ Austria, Switzerland (DACH)
                OR geo.geo_country_code IN (
                    'PK'          -- India companion
                    , 'AT', 'CH'    -- Germany
                    , 'PT'          -- Brazil
                    , 'AR', 'CO'    -- Mexico

                    -- Exclude, no support expected in 2022
                    , 'SG'
                    -- , 'NZ', 'MY', 'NO', 'BE', 'IE'
                    -- , 'CZ', 'HU', 'ZA', 'CL', 'VN', 'HK', 'TH', 'GR', 'UA'
                    -- , 'IL', 'AE', 'TW', 'SA', 'PE', 'RS', 'HR'
                )
                -- Latin America: choose countries individually b/c region includes
                --  many small islands that add noise
                OR geo.geo_country_code IN (
                    'AR', 'CL', 'CO', 'PE', 'PR'
                    -- Too small, no support expected
--                     , 'BZ', 'BO', 'CR', 'CU', 'SV', 'DO', 'EC', 'GT'
--                     , 'HN', 'NI', 'PA', 'PY',  'UY', 'VE'
--                     , 'JM'
                )
            )
        GROUP BY 1
    ),
    -- Merge all subs together
    subs_geo_final AS (
        SELECT
            COALESCE(g.subreddit_id, i.subreddit_id) AS subreddit_id
            , COALESCE(g.geo_relevant_country_count, i.geo_relevant_country_count) AS geo_relevant_country_count
            , COALESCE(g.geo_relevant_countries, i.geo_relevant_countries) AS geo_relevant_countries
            , COALESCE(g.geo_relevant_country_codes, i.geo_relevant_country_codes) AS geo_relevant_country_codes
        FROM subs_geo_custom_agg AS g
            FULL OUTER JOIN subs_i18n_core_agg AS i
                ON g.subreddit_id = i.subreddit_id
    )


SELECT
    DISTINCT
    g2.pt
    , ga.subreddit_id
    , g2.subreddit_name
    , g2.users_l7
    , g2.posts_not_removed_l28
    , g2.i18n_type
    , ga.* EXCEPT(subreddit_id)
FROM subs_geo_final AS ga
LEFT JOIN  `reddit-relevance.${dataset}.subclu_subreddit_candidates_${run_id}` AS g2
    ON ga.subreddit_id = g2.subreddit_id

ORDER BY users_l7 DESC, posts_not_removed_l28 DESC
);  -- close create table parens
