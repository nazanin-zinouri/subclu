-- Get QA flag + i18n relevance
-- NEW: only use the local_score (prod) table
--   but lower the standardized score (it's lower than in my previous table)
-- UPDATE: nvm, don't use this because the relevance score appears broken in a new way:
--  r/askreddit and other large US subreddits now appear relevant to a ton of countries
--   Not sure why, but it looks like the perc_by_country_l28 in the prod table is ~2% higher for US subs
--     was there a recent change or is there something odd about how that gets computed in the prod table??

-- Get QA flag + i18n relevance
DECLARE PT_DATE DATE DEFAULT CURRENT_DATE() - 2;

DECLARE TARGET_COUNTRIES DEFAULT [
    -- primarily English-speaking countries
    'AU', 'CA', 'GB'
    -- English, but smaller
    , 'IN', 'IE'
    -- DACH - Germany, Austria, & Switzerland
    , 'DE', 'AT', 'CH'
    -- LATAM & EUROPE
    , 'PT', 'BR'
    , 'FR', 'IT'
    , 'MX', 'ES', 'AR', 'CO', 'CR', 'PA'
    , 'RO', 'NL', 'GR', 'BE', 'PL'
    , 'TR', 'ZA', 'PH'
    -- Nordic countries
    , 'SE', 'FI', 'NO', 'DK'
];

WITH
subs_geo_custom_agg AS (
    -- Select subreddits that meet country local relevance
    SELECT
        g.subreddit_id
        -- Order so that we get (Mexico, US) only, and not both: (US, Mexico) AND (Mexico, US)
        , STRING_AGG(
            c.country_name, ', '
            ORDER BY c.country_name
        ) AS geo_relevant_countries
        , STRING_AGG(
            g.geo_country_code, ', '
            ORDER BY g.geo_country_code
        ) AS geo_relevant_country_codes
        , COUNT(DISTINCT g.geo_country_code) AS geo_relevant_country_count

    FROM `data-prod-165221.i18n.community_local_scores` AS g
        LEFT JOIN `reddit-employee-datasets.david_bermejo.countrycode_name_mapping` AS c
            ON g.geo_country_code = c.country_code
    WHERE DATE(g.pt) = PT_DATE
        AND geo_country_code IN UNNEST(TARGET_COUNTRIES)
        AND (
            sub_dau_perc_l28 >= 0.145
            -- The standardized score seems to consistenly be lower than the table that I tested,
            --  so we'll slightly lower the score below 2.0 (the old default)
            OR perc_by_country_sd >= 2.0
        )
    GROUP BY 1
)


SELECT
    qa.*
    , g.* EXCEPT(subreddit_id)
FROM `reddit-employee-datasets.david_bermejo.subreddit_qa_flags` AS qa
    LEFT JOIN subs_geo_custom_agg AS g
        ON qa.subreddit_id = g.subreddit_id
WHERE qa.pt = PT_DATE
    AND subreddit_name != 'profile'
    -- Select only subs with a rating OR minimum activity
    AND COALESCE(users_l7, 0) >= 100
    -- AND whitelist_status IS NOT NULL
    -- AND (
    --     primary_topic IS NOT NULL
    --     OR predicted_topic IS NOT NULL

    --     OR rating_short IS NOT NULL
    --     OR predicted_rating IS NOT NULL
    -- )
ORDER BY users_l7 DESC, subreddit_name
;
