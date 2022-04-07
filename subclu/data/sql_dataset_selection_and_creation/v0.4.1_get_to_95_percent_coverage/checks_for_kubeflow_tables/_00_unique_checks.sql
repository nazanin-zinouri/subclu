-- Queries to check that subreddit_ids are unique
--  for tables where we expect them to be unique

-- =====================
--  ambassador union
-- ===
SELECT
    COUNT(*) AS row_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count
FROM `reddit-relevance.tmp.ambassador_subreddits_union_20220323`
;
-- Ambassador expected: ~204


-- =====================
--  subreddit_candidates CTEs - BEFORE table is created
-- ===
-- Use these to check the CTEs (before creating the table)

-- For this query we don't use subreddit_id b/c it can be inconsistent due to sub-names
SELECT
    COUNT(*) AS row_count
    , COUNT(DISTINCT subreddit_name) AS subreddit_unique_count
FROM subs_with_views_and_posts_raw
;


SELECT
    COUNT(*) AS row_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count
FROM subs_above_view_and_post_threshold
;


SELECT
    COUNT(*) AS row_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count
FROM unique_posters_per_subreddit
;


-- =====================
--  subreddit_candidates AFTER table is created
-- ===
-- Table AFTER it's created
SELECT
    COUNT(*) AS row_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count
FROM `reddit-relevance.tmp.subclu_subreddit_candidates_20220323`
;
-- Candidates expected: ~205k


-- Inspect duplicate rows (if they exist)
WITH duplicate_ids AS (
    SELECT
        subreddit_id
        , COUNT(*)
    FROM `reddit-relevance.tmp.subclu_subreddit_candidates_20220323`

    GROUP BY 1
    HAVING COUNT(*) > 1
)

SELECT
    s.*
FROM `reddit-relevance.tmp.subclu_subreddit_candidates_20220323` AS s
    INNER JOIN duplicate_ids AS dup
        ON s.subreddit_id = dup.subreddit_id
;


-- =====================
--  geo-selected subreddits
-- ===
SELECT
    COUNT(*) AS row_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count
FROM `reddit-relevance.tmp.subclu_subreddit_geo_selected_20220323`
;


-- =====================
--  final table of selected subs for modeling
-- ===
SELECT
    COUNT(*) AS row_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_id_unique_count
    , COUNT(DISTINCT subreddit_name) AS subreddit_name_unique_count
FROM `reddit-relevance.tmp.subclu_subreddits_for_modeling_20220406`
;

