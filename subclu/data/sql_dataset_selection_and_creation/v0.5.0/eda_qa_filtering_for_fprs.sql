-- Create query to apply QA to filter out subreddits for FPRs

DECLARE PARTITION_DATE DATE DEFAULT (CURRENT_DATE() - 2);

-- Define sensitive topics (actual & predicted) to filter out
-- NOTE: we can't DECLARE variables this in a VIEW ;_; so we should create a table, otherwise we'd have to copy & paste
--   the topics list 4+ times and it's too easy to screw it up
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


CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_v0050_subreddit_clusters_c_qa_flags`
PARTITION BY pt
AS (

WITH
subs_geo_custom_agg AS (
    -- Select subreddits that meet country + relevance + activity thresholds
    SELECT
        geo.subreddit_id

        -- Order so that we get (Mexico, US) only, and not both: (US, Mexico) AND (Mexico, US)
        , STRING_AGG(geo.country_name, ', ' ORDER BY geo.country_name) AS geo_relevant_countries
        , STRING_AGG(geo.geo_country_code, ', ' ORDER BY geo.geo_country_code) AS geo_relevant_country_codes
        , COUNT(geo.geo_country_code) AS geo_relevant_country_count
    FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220705` AS geo
        INNER JOIN `reddit-employee-datasets.david_bermejo.subclu_v0050_subreddit_clusters_c_full` AS c
            ON geo.subreddit_id = c.subreddit_id

    WHERE 1=1
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
    GROUP BY 1
)

, base_filters AS (
    -- Use subquery so we don't have to define detailed & generic filter twice
    SELECT
        *
        , SPLIT(taxonomy_filter_detail, '-')[SAFE_OFFSET(0)] AS taxonomy_filter
        , SPLIT(taxonomy_filter_detail, '-')[SAFE_OFFSET(1)] AS taxonomy_filter_reason
        , SPLIT(predictions_filter_detail, '-')[SAFE_OFFSET(0)] AS predictions_filter

    FROM (
        SELECT
            c.subreddit_id
            , s.over_18
            , s.allow_discovery
            , (COALESCE(geo.geo_relevant_country_count, 0) >= 1) AS i18n_relevant_sub
            , m.k_0100_label_name
            , c.k_1000_majority_primary_topic AS k_1000_majority_topic
            , c.k_1000_label
            , geo.geo_relevant_country_count
            , geo.geo_relevant_country_codes
            , geo.geo_relevant_countries

            , c.subreddit_name

            , nt.primary_topic
            , nt.rating_short
            , nt.rating_name
            , vr.predicted_rating
            , vt.predicted_topic
            , asr.users_l7

            -- Flag based on NSFW clusters
            , IF(m.k_1000_label_recommend = 'no', 'remove', NULL) sensitive_cluster_filter
            -- Flag based on actual ratings & primary topics
            --  We need to be careful with some COALESCE() statements
            , CASE
                WHEN s.over_18 = 't' THEN 'remove-over_18'
                WHEN (COALESCE(nt.rating_short, '') = 'E') AND (nt.primary_topic NOT IN UNNEST(SENSITIVE_TOPICS)) THEN 'recommend'
                WHEN (nt.rating_short != 'E') AND (nt.primary_topic IN UNNEST(SENSITIVE_TOPICS)) THEN 'remove-rating_and_topic'
                WHEN (nt.rating_short != 'E') THEN 'remove-rating'
                WHEN (nt.primary_topic IN UNNEST(SENSITIVE_TOPICS)) THEN 'remove-topic'
                WHEN (nt.rating_short IS NULL) AND (nt.primary_topic IS NULL) THEN 'missing-rating_and_topic'
                WHEN nt.primary_topic IS NULL THEN 'missing-topic'
                WHEN nt.rating_short IS NULL THEN 'missing-rating'
                ELSE NULL
            END AS taxonomy_filter_detail

            -- Flag based on PREDICTED ratings & topics
            , CASE
                WHEN s.over_18 = 't' THEN 'remove-over_18'
                WHEN (COALESCE(vr.predicted_rating, '') = 'E') AND (vt.predicted_topic NOT IN UNNEST(SENSITIVE_TOPICS)) THEN 'recommend'
                WHEN (vr.predicted_rating != 'E') AND (vt.predicted_topic IN UNNEST(SENSITIVE_TOPICS)) THEN 'remove-rating_and_topic'
                WHEN (vr.predicted_rating != 'E') THEN 'remove-rating'
                WHEN (vt.predicted_topic IN UNNEST(SENSITIVE_TOPICS)) THEN 'remove-topic'
                -- No filter if fields are null/below threshold
                -- WHEN (vr.predicted_rating IS NULL) AND (vt.predicted_topic IS NULL) THEN 'missing-rating_and_topic'
                -- WHEN vt.predicted_topic IS NULL THEN 'missing-topic'
                -- WHEN vr.predicted_rating IS NULL THEN 'missing-rating'
                ELSE NULL
            END AS predictions_filter_detail

        FROM `reddit-employee-datasets.david_bermejo.subclu_v0050_subreddit_clusters_c_full` AS c
            -- Reduce list to only subs we'll use for FPR right away?
            LEFT JOIN subs_geo_custom_agg AS geo
                ON c.subreddit_id = geo.subreddit_id

            -- Get subreddit activity so we can prioritize larger/more active subs
            LEFT JOIN (
                SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
                WHERE DATE(pt) = PARTITION_DATE
            ) AS asr
                ON c.subreddit_name = LOWER(asr.subreddit_name)
            LEFT JOIN (
                SELECT
                    subreddit_id, over_18, allow_discovery
                    , verdict, is_spam, is_deleted, deleted
                    , type, quarantine
                FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
                WHERE dt = PARTITION_DATE
            ) AS s
                ON c.subreddit_id = s.subreddit_id
            LEFT JOIN (
                SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
                WHERE pt = PARTITION_DATE
            ) AS nt
                ON c.subreddit_id = nt.subreddit_id

            LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0050_subreddit_clusters_c_manual_names` m
                ON c.k_1000_label = m.k_1000_label
            LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_subreddits_for_modeling_20220705` AS sm
                ON c.subreddit_id = sm.subreddit_id

            -- We join rating & topic separately because the predictions are indepentent
            --  and we want to prevent bad filter outcomes
            LEFT JOIN (
                SELECT subreddit_id, predicted_topic, topic_prediction_score
                FROM `reddit-employee-datasets.anna_scaramuzza.reddit_vault_predictions_20220411`
                WHERE topic_prediction_score >= 0.5
            ) AS vt
                ON c.subreddit_id = vt.subreddit_id
            LEFT JOIN (
                SELECT subreddit_id, predicted_rating, rating_prediction_score
                FROM `reddit-employee-datasets.anna_scaramuzza.reddit_vault_predictions_20220411`
                WHERE rating_prediction_score >= 0.5
            ) AS vr
                ON c.subreddit_id = vr.subreddit_id

        WHERE 1=1
            -- OPTIONAL: Exclude spam, removed, & sketchy subs
            AND COALESCE(s.verdict, '') != 'admin-removed'
            AND COALESCE(s.is_spam, FALSE) = FALSE
            AND COALESCE(s.is_deleted, FALSE) = FALSE
            AND s.deleted IS NULL
            AND COALESCE(s.type, '') IN ('public', 'private', 'restricted')
            AND COALESCE(s.quarantine, FALSE) = FALSE
    )
)
, combined_filters AS (
    -- Again, use subquery to reduce the need to define case statements twice
    SELECT
        PARTITION_DATE AS pt
        , subreddit_id
        , over_18
        , allow_discovery
        , geo_relevant_countries
        , users_l7
        , subreddit_name
        , primary_topic
        , rating_short
        , predicted_rating
        , predicted_topic
        , CASE
            WHEN combined_filter_reason IN (
                'sensitive_cluster', 'over_18', 'allow_discovery_f'
                , 'predictions_clean', 'predictions_missing'
                , 'rating', 'topic', 'rating_and_topic'
            ) THEN NULL
            ELSE combined_filter_reason
        END AS taxonomy_action
        , combined_filter_detail
        , combined_filter
        , combined_filter_reason
        , taxonomy_filter_detail
        , predictions_filter_detail
        , sensitive_cluster_filter

        , * EXCEPT (
            subreddit_id
            , over_18
            , allow_discovery
            , geo_relevant_countries
            , users_l7
            , subreddit_name
            , primary_topic
            , rating_short
            , predicted_rating
            , predicted_topic
            , combined_filter_detail
            , combined_filter
            , combined_filter_reason
            , taxonomy_filter_detail
            , predictions_filter_detail
            , sensitive_cluster_filter
        )


    FROM (
        SELECT
            *
            , SPLIT(combined_filter_detail, '-')[SAFE_OFFSET(0)] AS combined_filter
            , SPLIT(combined_filter_detail, '-')[SAFE_OFFSET(1)] AS combined_filter_reason
        FROM (
            SELECT
                *
                -- NOTE that order of filters can make a big difference.
                --   Remove over_18 & sensitive clusters first
                , CASE
                    WHEN (over_18 = 't') THEN 'remove-over_18'
                    WHEN (sensitive_cluster_filter = 'remove') THEN 'remove-sensitive_cluster'

                    -- Exclude geo-subreddits that don't want to be discovered
                    --  NOTE: We can use them as seeds, but we can't use them as recommendations so we don't need to rate them
                    WHEN COALESCE(allow_discovery, '') = 'f' THEN 'remove-allow_discovery_f'

                    WHEN (taxonomy_filter_detail = 'recommend') AND (predictions_filter_detail = 'recommend') THEN 'recommend-predictions_clean'
                    WHEN (taxonomy_filter_detail = 'recommend') AND (predictions_filter_detail IS NULL) THEN 'recommend-predictions_missing'

                    -- Apply some overrides
                    -- Gaming & History subs sometimes get an "M" rating (incorrectly)
                    WHEN (
                        (taxonomy_filter_detail = 'recommend')
                        AND (predictions_filter_detail = 'remove-rating')
                        AND (primary_topic IN ('Gaming', 'History'))
                        AND (predicted_rating = 'M')
                    ) THEN 'recommend'

                    -- Animal/pet subreddits about cats with "pussy" in the title get mis-rated
                    WHEN (
                        (taxonomy_filter_detail = 'recommend')
                        AND (predictions_filter = 'remove')
                        AND (primary_topic = 'Animals and Pets')
                        AND (predicted_rating = 'X')
                    ) THEN 'recommend'

                    -- Sports subreddits mis-labeled as "fitness & nutrition"
                    WHEN (
                        (taxonomy_filter_detail = 'recommend')
                        AND (predictions_filter_detail = 'remove-topic')
                        AND (primary_topic = 'Sports')
                        AND (predicted_topic = 'Fitness and Nutrition')
                        AND (predicted_rating = 'E')
                    ) THEN 'recommend'

                    -- Careers in medicine that get labeled as "medical"
                    WHEN (
                        (taxonomy_filter_detail = 'recommend')
                        AND (predictions_filter_detail = 'remove-topic')
                        AND (primary_topic = 'Careers')
                        AND (predicted_topic = 'Medical and Mental Health')
                        AND (predicted_rating = 'E')
                    ) THEN 'recommend'

                    -- Remove & flag for review
                    WHEN (taxonomy_filter_detail = 'recommend') AND (predictions_filter_detail = 'remove-rating') THEN 'remove-review_rating'
                    WHEN (taxonomy_filter_detail = 'recommend') AND (predictions_filter_detail = 'remove-topic') THEN 'remove-review_topic'
                    WHEN (taxonomy_filter_detail = 'recommend') AND (predictions_filter_detail = 'remove-rating_and_topic') THEN 'remove-review_rating_and_topic'

                    -- Recommend & flag missing topic/rating
                    WHEN (taxonomy_filter_detail = 'missing-rating') AND  (predictions_filter_detail = 'recommend') THEN 'review-missing_rating'
                    WHEN (taxonomy_filter_detail = 'missing-topic') AND   (predictions_filter_detail = 'recommend') THEN 'recommend-missing_topic'
                    WHEN (taxonomy_filter_detail = 'missing-rating_and_topic') AND (predictions_filter_detail = 'recommend') THEN 'review-missing_rating_and_topic'

                    -- Flag missing topic/rating
                    WHEN (taxonomy_filter_detail = 'missing-rating') AND  (predictions_filter_detail IS NULL) THEN 'review-missing_rating'
                    WHEN (taxonomy_filter_detail = 'missing-topic') AND   (predictions_filter_detail IS NULL) THEN 'review-missing_topic'
                    WHEN (taxonomy_filter_detail = 'missing-rating_and_topic') AND (predictions_filter_detail IS NULL) THEN 'review-missing_rating_and_topic'

                    -- Apply remove cases last to allow overrides above
                    WHEN (taxonomy_filter_detail = 'remove-rating_and_topic') THEN 'remove-rating_and_topic'
                    WHEN (taxonomy_filter_detail = 'remove-rating') THEN 'remove-rating'
                    WHEN (taxonomy_filter_detail = 'remove-topic') THEN 'remove-topic'

                    -- Remove by predicted-rating, for now apply general remove to be safe, might break it down if need to dig into details
                    WHEN (predictions_filter_detail = 'remove-rating') THEN 'remove'
                    WHEN (predictions_filter_detail = 'remove-topic') THEN 'remove'
                    WHEN (predictions_filter_detail = 'remove-rating_and_topic') THEN 'remove'

                    ELSE NULL
                END AS combined_filter_detail

            FROM base_filters
        )
    )
    ORDER BY k_1000_label DESC, combined_filter_detail
)

SELECT * FROM combined_filters
);  -- close CREATE table parens

-- Check counts for each CTE
-- SELECT
--     (SELECT COUNT(*) FROM subs_geo_custom_agg) AS geo_rows
--     , (SELECT COUNT(DISTINCT subreddit_id) FROM subs_geo_custom_agg) AS geo_subreddits
--     , (SELECT COUNT(*) FROM base_filters) AS base_rows
--     , (SELECT COUNT(DISTINCT subreddit_id) FROM base_filters) AS base_subreddits
--     , (SELECT COUNT(*) FROM combined_filters) AS combined_rows
--     , (SELECT COUNT(DISTINCT subreddit_id) FROM combined_filters) AS combined_subreddits


-- Check interesting subs where taxonomy recommends but models remove
-- SELECT
--   *
-- FROM combined_filters
-- WHERE 1=1
--     AND taxonomy_filter = "recommend"
--     AND predictions_filter_detail IN ('remove-rating_and_topic')
--     -- AND COALESCE(predictions_filter, '') IN ('', 'remove')
-- ORDER BY
--     k_1000_label, combined_filter_detail, taxonomy_filter_detail, predictions_filter_detail
--     , predicted_rating, predicted_topic, primary_topic, subreddit_name
-- ;


-- Select full table
-- SELECT
--     *
-- FROM base_flags
-- ORDER BY taxonomy_filter, rating_name, primary_topic, subreddit_name
-- ;


-- Select aggregates
-- SELECT
--     taxonomy_filter
--     , COUNT(DISTINCT subreddit_id) AS subreddit_count
--     , COUNT(*) row_count
-- FROM base_flags
-- GROUP BY 1
-- ORDER BY subreddit_count DESC
-- ;


-- Select cross-tab aggregates, tax + pred
-- SELECT
--     taxonomy_filter_detail
--     , predictions_filter
--     , COUNT(DISTINCT subreddit_id) AS subreddit_count
--     -- , COUNT(*) row_count  -- Check for dupes
-- FROM base_flags
-- GROUP BY 1, 2
-- ORDER BY 1, 2
-- ;

-- Select cross-tab aggregates, all combined, detail
-- SELECT
--     combined_filter_detail
--     , taxonomy_filter_detail
--     , predictions_filter
--     , COUNT(DISTINCT subreddit_id) AS subreddit_count
--     -- , COUNT(*) row_count  -- Check for dupes
-- FROM combined_filters
-- WHERE 1=1
--     -- AND COALESCE(combined_filter_detail, '') NOT IN (
--     --     'remove-sensitive_cluster'
--     -- )

-- GROUP BY 1, 2, 3
-- ORDER BY 1, 2, 3
-- -- ORDER BY 4 DESC
-- ;

-- Select cross-tab aggregates, all combined, agg
-- SELECT
--     combined_filter
--     , taxonomy_action
--     , taxonomy_filter
--     -- , predictions_filter
--     , COUNT(DISTINCT subreddit_id) AS subreddit_count
--     -- , COUNT(*) row_count  -- Check for dupes
-- FROM combined_filters
-- WHERE 1=1
--     -- AND COALESCE(combined_filter_detail, '') NOT IN (
--     --     'remove-sensitive_cluster'
--     -- )

-- GROUP BY 1, 2, 3
-- -- ORDER BY 1, 2, 3
-- ORDER BY 4 DESC
-- ;


-- Check aggs, for i18n subs
-- SELECT
--     i18n_relevant_sub
--     , combined_filter
--     , taxonomy_action
--     , taxonomy_filter_detail
--     -- , predictions_filter
--     , COUNT(DISTINCT subreddit_id) AS subreddit_count
--     -- , COUNT(*) row_count  -- Check for dupes
-- FROM combined_filters
-- WHERE 1=1
--     AND i18n_relevant_sub = TRUE
--     -- AND COALESCE(combined_filter_detail, '') NOT IN (
--     --     'remove-sensitive_cluster'
--     -- )

-- GROUP BY 1, 2, 3, 4
-- -- ORDER BY 1, 2, 3
-- ORDER BY 5 DESC
-- ;


-- Agg just i18n, final filter + taxonomy action
SELECT
    i18n_relevant_sub
    , combined_filter
    , taxonomy_action
    , COUNT(DISTINCT subreddit_id) AS subreddit_count
    , SUM(users_l7) AS users_l7_sum
    -- , COUNT(*) row_count  -- Check for dupes
FROM combined_filters
WHERE 1=1
    -- AND i18n_relevant_sub = TRUE
    -- AND COALESCE(combined_filter_detail, '') NOT IN (
    --     'remove-sensitive_cluster'
    -- )

GROUP BY 1, 2, 3
-- ORDER BY 1, 2, 3
ORDER BY 4 DESC
;


-- View subreddits for taxonomy to review
-- SELECT
--     *
-- FROM combined_filters
-- WHERE 1=1
--     AND i18n_relevant_sub = TRUE
--     AND combined_filter = 'recommend'
--     AND COALESCE(taxonomy_action, '') IN (
--         'missing_rating', 'missing_rating_and_topic'
--     )
--     -- AND COALESCE(combined_filter_detail, '') NOT IN (
--     --     'remove-sensitive_cluster'
--     -- )

-- ORDER BY k_1000_label
-- ;
