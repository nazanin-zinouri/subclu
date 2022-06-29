

-- ==================
-- Check CTE Counts (Before creating table)
-- ===
-- Selected subreddit counts & percentiles
SELECT
    COUNT(*) as row_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_id_unique_count
    , COUNT(DISTINCT subreddit_name) AS subreddit_name_unique_count
    , SUM(
        CASE WHEN (active = TRUE) THEN 1
        ELSE 0
        END
    ) AS active_subreddit_count

    , MIN(activity_7_day) AS activity_7_day_min
    , APPROX_QUANTILES(activity_7_day, 100)[OFFSET(50)] AS activity_7_day_median
    , AVG(activity_7_day) AS activity_7_day_avg
    , APPROX_QUANTILES(activity_7_day, 100)[OFFSET(95)] AS activity_7_day_p95

    , MIN(users_l7) AS users_l7_min
    , APPROX_QUANTILES(users_l7, 100)[OFFSET(50)] AS users_l7_median
    , AVG(users_l7) AS users_l7_avg
    , APPROX_QUANTILES(users_l7, 100)[OFFSET(95)] AS users_l7_p95

    , MIN(posts_not_removed_l28) AS posts_not_removed_l28_min
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(50)] AS posts_not_removed_l28_median
    , AVG(posts_not_removed_l28) AS posts_not_removed_l28_avg
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(95)] AS posts_not_removed_l28_p95

FROM selected_subs
;


-- Check slo clean text
SELECT
    COUNT(*) as row_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_id_unique_count
FROM subreddit_lookup_clean_text_meta
;


-- Check clean text columns
SELECT
    subreddit_id

    -- -- NOTE: For some reason bigquery shows a space before any string, but it doesn't mean it's part of the output
    , name
    , title
    -- , subreddits_in_descriptions
    , public_description
    , description

    , subreddit_name_title_and_clean_descriptions

FROM subreddit_lookup_clean_text_meta

WHERE 1=1
    -- AND description LIKE "%|%"
    AND LOWER(name) IN (
        -- non-English
        'de', 'mexico', 'france', 'turkey', 'ukraina', 'ja'
        -- r/ mentions
        , 'gardern', 'burgers', 'hawktalk', 'multihub', 'edcexchange'
        , 'uwaterloo', 'yojamba', 'kindness', 'hypotheticalsituation', 'ecigclassifieds'
        , 'motorcitykitties', 'minnesotatwins', 'mlb'
        -- tables and other markdown
        , 'clg', 'braves', 'sportingkc', 'atlantaunited', 'mma', 'barca'
        , 'askeurope', 'asoiaf', 'dogemarket', 'writingprompts', 'brewgearfs'
    )

ORDER BY LOWER(name)

LIMIT 1000
;

-- Ballpark numbers
-- row_count	 207k
-- subreddit_id_unique_count	 207k
-- subreddit_name_unique_count	 207k
-- subreddits_1_plus_posts	 207k
-- subreddits_2_plus_posts	 176
-- subreddits_3_plus_posts	 154
-- subreddits_4_plus_posts	 140
-- subreddits_5_plus_posts	 126
-- posts_not_removed_l28_min	1
-- posts_not_removed_l28_p25	2
-- posts_not_removed_l28_p30	3
-- posts_not_removed_l28_p35	4
-- posts_not_removed_l28_p40	5
-- posts_not_removed_l28_median	     8
-- posts_not_removed_l28_p75	    33
-- posts_not_removed_l28_avg	   130
-- posts_not_removed_l28_p95	   386
-- posts_not_removed_l28_p99	 1,997
-- posts_not_removed_l28_max   375k

-- Check how many posts each subreddit has.
--  Use to help how many subs have 3+ posts in L28 days
-- Selected subreddit counts & percentiles
SELECT
    COUNT(*) as row_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_id_unique_count
    , COUNT(DISTINCT subreddit_name) AS subreddit_name_unique_count
    , SUM(
        IF(posts_not_removed_l28 >= 1, 1, 0)
    ) AS subreddits_1_plus_posts
    , SUM(
        IF(posts_not_removed_l28 >= 2, 1, 0)
    ) AS subreddits_2_plus_posts
    , SUM(
        IF(posts_not_removed_l28 >= 3, 1, 0)
    ) AS subreddits_3_plus_posts
    , SUM(
        IF(posts_not_removed_l28 >= 4, 1, 0)
    ) AS subreddits_4_plus_posts
    , SUM(
        IF(posts_not_removed_l28 >= 5, 1, 0)
    ) AS subreddits_5_plus_posts
    , SUM(
        IF(posts_not_removed_l28 >= 6, 1, 0)
    ) AS subreddits_6_plus_posts

    , MIN(posts_not_removed_l28) AS posts_not_removed_l28_min
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(25)] AS posts_not_removed_l28_p25
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(30)] AS posts_not_removed_l28_p30
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(35)] AS posts_not_removed_l28_p35
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(40)] AS posts_not_removed_l28_p40
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(50)] AS posts_not_removed_l28_median
    , AVG(posts_not_removed_l28) AS posts_not_removed_l28_avg
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(75)] AS posts_not_removed_l28_p75
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(95)] AS posts_not_removed_l28_p95
    , APPROX_QUANTILES(posts_not_removed_l28, 100)[OFFSET(99)] AS posts_not_removed_l28_p99
    , MAX(posts_not_removed_l28) AS posts_not_removed_l28_max

FROM `reddit-relevance.tmp.subclu_subreddit_candidates_20220621`
;


-- Debug duplicates
-- Dupe subreddits:
--   jenfoxuwu=t5_3z2use
--   jenfoxuwu -> diff:           activity_7_day, submits_7_day, comments_7_day
--   test_automation_001 -> diff: activity_7_day, submits_7_day, comments_7_day, active, highly_active
WITH subs_ranked AS (
    SELECT
        c.*
        -- Rank by sub_name
        , ROW_NUMBER() OVER(
            PARTITION BY subreddit_name
            ORDER BY users_l7
        ) AS rank_sub_name
        -- Rank by sub_id
        , ROW_NUMBER() OVER(
            PARTITION BY subreddit_id
            ORDER BY users_l7
        ) AS rank_sub_id
    FROM `reddit-relevance.tmp.subclu_subreddit_candidates_20220619` AS c
)

, subs_duplicated AS (
  SELECT DISTINCT
    subreddit_id
    , subreddit_name
  FROM subs_ranked
  WHERE 1=1
  AND (
    rank_sub_name >= 2
    OR rank_sub_id >= 2
  )
)

SELECT
  *
FROM subs_ranked
WHERE 1=1
  AND subreddit_id IN (SELECT subreddit_id FROM subs_duplicated)
  OR subreddit_name IN (SELECT subreddit_name from subs_duplicated)

ORDER BY subreddit_name
;

-- ===============
-- Check for dupes (CTEs)
-- ===
-- SELECT
--     COUNT(*) as row_count
--     , COUNT(DISTINCT subreddit_id) AS subreddit_id_unique_count
--     , COUNT(DISTINCT subreddit_name) AS subreddit_name_unique_count
-- FROM final_meta


-- SELECT
--     *
-- FROM subs_with_views_and_posts_raw
-- WHERE 1=1
--     AND subreddit_name IN (
--     'jenfoxuwu', 'test_automation_001'
--   )
-- ;


-- SELECT
--     *
-- FROM subs_above_view_and_post_threshold
-- WHERE 1=1
--     AND subreddit_name IN (
--     'jenfoxuwu', 'test_automation_001'
--   )
-- ;

-- SELECT
--     *
-- FROM unique_posters_per_subreddit
-- WHERE 1=1
--     AND subreddit_id IN (
--     't5_3z2use', 't5_4sdleo'
--   )
-- ;


-- SELECT
--     -- this one had dupes
--     *
-- FROM final_meta
-- WHERE 1=1
--     AND subreddit_name IN (
--     'jenfoxuwu', 'test_automation_001'
--   )
-- ;


-- Duplicated subreddit_ids root query:
SELECT *
FROM `data-prod-165221.ds_subreddit_whitelist_tables.active_subreddits`
WHERE DATE(_PARTITIONTIME) = (CURRENT_DATE() - 2) -- "2022-06-17"
  AND (
      subreddit_name IN (
      'jenfoxuwu', 'test_automation_001'
    )
    OR subreddit_id IN (
      't5_3z2use', 't5_4sdleo'
    )
  )


-- Check new column for subreddit seeds
--  This way we can get subreddit meta & posts for all subreddits
--  But only use some subreddits as seeds to create topic clusters
SELECT
    subreddit_seed_for_clusters
    , COUNT(DISTINCT subreddit_id) AS subreddit_count
FROM `reddit-relevance.tmp.subclu_subreddits_for_modeling_20220620`
GROUP BY 1
;


-- Check new column for subreddit seeds
--  This way we can get subreddit meta & posts for all subreddits
--  But only use some subreddits as seeds to create topic clusters
SELECT
    subreddit_seed_for_clusters
    , COUNT(DISTINCT subreddit_id) AS subreddit_count
FROM `reddit-relevance.tmp.subclu_subreddits_for_modeling_20220622`
GROUP BY 1
;


-- Check new column for subreddit seeds + relevant countries for some sports subs
SELECT
    *
FROM `reddit-relevance.tmp.subclu_subreddits_for_modeling_20220622`

WHERE 1=1
    AND subreddit_name IN (
        'formula1', 'lewishamilton', 'sergioperez', 'redbullracing', 'maxverstappen33'
        , 'cricket', 'ipl', 'premierleague', 'afl'
        -- , 'golf', 'nba'
        , 'football', 'soccer', 'ligamx', 'bundesliga', 'fcbarcelona', 'realmadrid'
    )
ORDER BY users_l7 DESC
;
