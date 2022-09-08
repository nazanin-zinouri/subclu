-- Get QA flag + i18n relevance (for dashboard)
--   https://app.mode.com/reddit/reports/828d1aea4901
-- This version relies on both the local score AND my snapshot
--   because the standardized local score was broken

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
        COALESCE(g1.subreddit_id, g2.subreddit_id) AS subreddit_id

        -- Order so that we get (Mexico, US) only, and not both: (US, Mexico) AND (Mexico, US)
        , STRING_AGG(
            DISTINCT COALESCE(g1.country_name, g2.country_name),
            ', '
            ORDER BY COALESCE(g1.country_name, g2.country_name)
        ) AS geo_relevant_countries
        , STRING_AGG(
            DISTINCT COALESCE(g1.geo_country_code, g2.geo_country_code),
            ', '
            ORDER BY COALESCE(g1.geo_country_code, g2.geo_country_code)
        ) AS geo_relevant_country_codes
        , COUNT(DISTINCT COALESCE(g1.geo_country_code, g2.geo_country_code)) AS geo_relevant_country_count

    FROM (
        SELECT
                g.*
                , c.country_name
        FROM `data-prod-165221.i18n.community_local_scores` AS g
            LEFT JOIN `reddit-employee-datasets.david_bermejo.countrycode_name_mapping` AS c
                ON g.geo_country_code = c.country_code
        WHERE DATE(g.pt) = PT_DATE
            AND geo_country_code IN UNNEST(TARGET_COUNTRIES)
            AND (
                sub_dau_perc_l28 >= 0.20
                -- The standardized score is broken as of 2022-09-07
                -- OR perc_by_country_sd >= 2.5
            )
    ) AS g1
        -- The standardized score is broken as of 2022-09-07 (waitint for PR to merge)
        -- In the meantime, we need to use an older table to get it
        FULL OUTER JOIN (
            SELECT *
            FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220901`
            WHERE 1=1
                AND geo_country_code IN UNNEST(TARGET_COUNTRIES)

                -- Pick subreddits that qualify under at least one LOCAL metric
                AND (
                    users_percent_by_subreddit_l28 >= 0.15
                    OR users_percent_by_country_standardized >= 2.5
                    -- Try the combined score to include a few more relevant subreddits
                    OR relevance_combined_score >= 0.18
                )
        ) AS g2
            ON g1.subreddit_id = g2.subreddit_id

    GROUP BY 1
)


SELECT
    qa.* EXCEPT(
        blocklist_reason, k_0100_label_name, k_1000_label
    )
    , (g.subreddit_id IS NOT NULL) AS i18n_relevant_sub
    , g.* EXCEPT(subreddit_id)
FROM `reddit-employee-datasets.david_bermejo.subreddit_qa_flags` AS qa
    LEFT JOIN subs_geo_custom_agg AS g
        ON qa.subreddit_id = g.subreddit_id

WHERE qa.pt = PT_DATE
    AND subreddit_name != 'profile'
    -- Exclude spam & deleted subs b/c we can't act on them
    AND combined_filter_reason NOT IN ("spam_banned_or_deleted")

    -- Select only subs with minimum activity
    AND COALESCE(users_l7, 0) >= 50

    -- Alternative: only check subs with a rating
    -- AND (
    --     primary_topic IS NOT NULL
    --     OR predicted_topic IS NOT NULL

    --     OR rating_short IS NOT NULL
    --     OR predicted_rating IS NOT NULL
    -- )
ORDER BY users_l7 DESC, subreddit_name
;
