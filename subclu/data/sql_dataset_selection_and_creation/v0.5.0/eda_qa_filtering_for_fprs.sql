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


WITH base_filters AS (
    -- Use subquery so we don't have to define detailed & generic filter twice
    SELECT
        *
        , SPLIT(taxonomy_filter_detail, '-')[SAFE_OFFSET(0)] AS taxonomy_filter
        , SPLIT(predictions_filter_detail, '-')[SAFE_OFFSET(0)] AS predictions_filter

    FROM (
        SELECT
            c.subreddit_id
            , s.over_18
            , s.allow_discovery
            , (COALESCE(sm.geo_relevant_country_count, 0) >= 1) AS i18n_relevant_sub
            , c.k_0400_label
            , m.k_0100_label_name
            , c.k_1000_majority_primary_topic AS k_1000_majority_topic
            , c.k_1000_label
            , sm.geo_relevant_country_count
            , sm.geo_relevant_countries
            , c.subreddit_name

            , nt.primary_topic
            , nt.rating_short
            , nt.rating_name
            , vr.predicted_rating
            , vt.predicted_topic

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
            LEFT JOIN (
                SELECT
                    subreddit_id, over_18, allow_discovery
                FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
                WHERE dt = CURRENT_DATE() - 2
            ) AS s
                ON c.subreddit_id = s.subreddit_id
            LEFT JOIN (
                SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
                WHERE pt = (CURRENT_DATE() - 2)
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
    )
)
, combined_filters AS (
    -- Again, use subquery to reduce the need to define case statements twice
    SELECT
        * EXCEPT(sensitive_cluster_filter, taxonomy_filter_detail, predictions_filter_detail, taxonomy_filter, predictions_filter)
        , SPLIT(combined_filter_detail, '-')[SAFE_OFFSET(0)] AS combined_filter
        , CASE
            WHEN SPLIT(combined_filter_detail, '-')[SAFE_OFFSET(1)] IN (
                'no_predictions', 'sensitive_cluster', 'over_18', 'allow_discovery_f'
            ) THEN NULL
            ELSE SPLIT(combined_filter_detail, '-')[SAFE_OFFSET(1)]
        END AS taxonomy_action
        , taxonomy_filter_detail, predictions_filter_detail, sensitive_cluster_filter, taxonomy_filter, predictions_filter
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

                WHEN (taxonomy_filter_detail = 'recommend') AND (predictions_filter_detail = 'recommend') THEN 'recommend'
                WHEN (taxonomy_filter_detail = 'recommend') AND (predictions_filter_detail IS NULL) THEN 'recommend-no_predictions'
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
                WHEN (taxonomy_filter_detail = 'missing-rating') AND  (predictions_filter_detail = 'recommend') THEN 'recommend-missing_rating'
                WHEN (taxonomy_filter_detail = 'missing-topic') AND   (predictions_filter_detail = 'recommend') THEN 'recommend-missing_topic'
                WHEN (taxonomy_filter_detail = 'missing-rating_and_topic') AND (predictions_filter_detail = 'recommend') THEN 'recommend-missing_rating_and_topic'

                -- Flag missing topic/rating
                WHEN (taxonomy_filter_detail = 'missing-rating') AND  (predictions_filter_detail IS NULL) THEN 'recommend-missing_rating'
                WHEN (taxonomy_filter_detail = 'missing-topic') AND   (predictions_filter_detail IS NULL) THEN 'recommend-missing_topic'
                WHEN (taxonomy_filter_detail = 'missing-rating_and_topic') AND (predictions_filter_detail IS NULL) THEN 'recommend-missing_rating_and_topic'

                -- Apply remove cases last to allow overrides above
                WHEN (taxonomy_filter_detail = 'remove-rating_and_topic') THEN 'remove'
                WHEN (taxonomy_filter_detail = 'remove-rating') THEN 'remove'
                WHEN (taxonomy_filter_detail = 'remove-topic') THEN 'remove'

                -- Remove by predicted-rating, for now apply general remove to be safe, might break it down if need to dig into details
                WHEN (predictions_filter_detail = 'remove-rating') THEN 'remove'
                WHEN (predictions_filter_detail = 'remove-topic') THEN 'remove'
                WHEN (predictions_filter_detail = 'remove-rating_and_topic') THEN 'remove'

                ELSE NULL
            END AS combined_filter_detail

        FROM base_filters
    )
)


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


-- Agg just the final filter + taxonomy action
-- SELECT
--     i18n_relevant_sub
--     , combined_filter
--     , taxonomy_action
--     , COUNT(DISTINCT subreddit_id) AS subreddit_count
--     -- , COUNT(*) row_count  -- Check for dupes
-- FROM combined_filters
-- WHERE 1=1
--     AND i18n_relevant_sub = TRUE
--     -- AND COALESCE(combined_filter_detail, '') NOT IN (
--     --     'remove-sensitive_cluster'
--     -- )

-- GROUP BY 1, 2, 3
-- -- ORDER BY 1, 2, 3
-- ORDER BY 4 DESC
-- ;


-- Get subreddits for taxonomy to review
SELECT
    *
FROM combined_filters
WHERE 1=1
    AND i18n_relevant_sub = TRUE
    AND combined_filter = 'recommend'
    AND COALESCE(taxonomy_action, '') IN (
        'missing_rating', 'missing_rating_and_topic'
    )
    -- AND COALESCE(combined_filter_detail, '') NOT IN (
    --     'remove-sensitive_cluster'
    -- )

ORDER BY k_1000_label
;
