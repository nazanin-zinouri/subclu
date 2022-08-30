-- Check counts for each CTE
SELECT
    (SELECT COUNT(*) FROM rating_and_topic_curator_and_crowd) AS topic_and_rating_rows
    , (SELECT COUNT(DISTINCT subreddit_id) FROM rating_and_topic_curator_and_crowd) AS topic_and_rating_subs
    , (SELECT COUNT(*) FROM base_filters) AS base_rows
    , (SELECT COUNT(DISTINCT subreddit_id) FROM base_filters) AS base_subreddits
    , (SELECT COUNT(*) FROM combined_filters) AS combined_rows
    , (SELECT COUNT(DISTINCT subreddit_id) FROM combined_filters) AS combined_subreddits
;

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
-- SELECT
--     i18n_relevant_sub
--     , combined_filter
--     , taxonomy_action
--     , COUNT(DISTINCT subreddit_id) AS subreddit_count
--     , SUM(users_l7) AS users_l7_sum
--     -- , COUNT(*) row_count  -- Check for dupes
-- FROM combined_filters
-- WHERE 1=1
--     -- AND i18n_relevant_sub = TRUE
--     -- AND COALESCE(combined_filter_detail, '') NOT IN (
--     --     'remove-sensitive_cluster'
--     -- )
--
-- GROUP BY 1, 2, 3
-- -- ORDER BY 1, 2, 3
-- ORDER BY 4 DESC
-- ;


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
