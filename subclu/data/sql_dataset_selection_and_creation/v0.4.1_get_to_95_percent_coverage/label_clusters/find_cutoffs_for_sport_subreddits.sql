-- Get the cluster ID & cluster subs, given an input sub names
-- With this query we're trying to find good cut-offs to find the
--  k-value (cluster number) where known sports leagues are different
--  We need them to be different so that we can create human labels at a level
--   that is helpful for onboarding and other surfaces.

WITH clusters_for_selected_subs AS(
    SELECT
        DISTINCT k_0150_label
    FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full`
    WHERE subreddit_name IN (
        'soccer', 'nba', 'formula1', 'nhl', 'nfl', 'baseball'
    )
)


-- Get summary of distinct clusters
-- SELECT
--     lbl.k_0100_label
--     , lbl.k_0150_label
--     , lbl.k_0400_label

--     , COUNT(DISTINCT subreddit_id) AS subreddits_count

-- FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full` AS lbl
-- INNER JOIN clusters_for_selected_subs AS sel
--     ON lbl.k_0150_label = sel.k_0150_label

-- GROUP BY 1, 2, 3

-- ORDER BY 1, 2, 3
-- ;


-- Check Top subreddits in each cluster
SELECT
    lbl.k_0100_label
    , lbl.k_0150_label
    , lbl.k_0400_label
    , subreddit_name
    , posts_for_modeling_count

FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full` AS lbl
INNER JOIN clusters_for_selected_subs AS sel
    ON lbl.k_0150_label = sel.k_0150_label

WHERE 1=1
    AND posts_for_modeling_count >= 500

ORDER BY 1, 2, 3, posts_for_modeling_count DESC
;
