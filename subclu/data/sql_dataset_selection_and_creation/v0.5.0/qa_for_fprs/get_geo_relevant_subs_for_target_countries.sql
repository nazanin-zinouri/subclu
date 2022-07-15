-- Create query to filter out subreddits for FPRs

-- Define sensitive topics (actualy & predicted) to filter out
DECLARE SENSITIVE_TOPICS DEFAULT [
    'Addiction Support'
    , 'Activism'
    , 'Culture, Race, and Ethnicity', 'Fitness and Nutrition'
    , 'Gender', 'Mature Themes and Adult Content', 'Medical and Mental Health'
    , 'Military'
    , "Men's Health", 'Politics', 'Sexual Orientation'
    , 'Trauma Support', "Women's Health"
];
DECLARE TARGET_COUNTRIES DEFAULT [
    'AU', 'CA', 'GB', 'IN', 'FR', 'DE', 'IT', 'MX', 'BR'
    , 'ES', 'SE', 'RO', 'NL', 'TR', 'PH'
    , 'GR', 'AU', 'AR', 'CO', 'BE', 'CH', 'PO', 'SA', 'CR', 'PA'
    , 'IR', 'IE'
];

WITH
subs_geo_target AS (
    -- Select subreddits that meet country + relevance + activity thresholds
    SELECT
        geo.subreddit_id
        , geo.geo_country_code
        , geo.subreddit_name
        , geo.country_name
        , qa.primary_topic
        , qa.rating_short
        , qa.predicted_rating
        , qa.predicted_topic
        , qa.combined_filter_detail
        , qa.combined_filter
        , qa.combined_filter_reason
        , qa.taxonomy_action
        , qa.k_1000_label

    FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220705` AS geo
        LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0050_subreddit_clusters_c_qa_flags` AS qa
            ON geo.subreddit_id = qa.subreddit_id

    WHERE 1=1
        AND qa.pt = "2022-07-12"
        -- Assume that all subs in model meet activity thresholds

        -- Pick subreddits that qualify under at least one metric/threshold
        --   Use the numeric values in case the defined threshold change
        AND (
            geo_relevance_default = TRUE
            OR users_percent_by_subreddit_l28 >= 0.14
            OR users_percent_by_country_standardized >= 2.5
            -- Try the combined score to include a few more relevant subreddits
            OR relevance_combined_score >= 0.175
        )
        -- pick subs that are relevant to target countries
        AND (
            geo.geo_country_code IN UNNEST(TARGET_COUNTRIES)
        )
)

SELECT * FROM subs_geo_target
ORDER BY country_name, k_1000_label
;
