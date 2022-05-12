-- Get TF-IDF & BM25 at CLUSTER level for the US
--  The best strategy is to get the top N from TF-IDF and top M from BM25 b/c they get complementary results

-- ===============
-- Limits for geo-relevance
-- ===
DECLARE MIN_USERS_PERCENT_BY_SUBREDDIT_L28 NUMERIC DEFAULT 0.14; -- default is 0.14 (14%)
DECLARE MIN_USERS_PERCENT_BY_COUNTRY_STANDARDIZED NUMERIC DEFAULT 3.0; -- default is 3.0

-- Variables for subreddits to show
DECLARE N_US_SUBREDDITS_IN_AGG_SUMMARY NUMERIC DEFAULT 15;
DECLARE N_GEO_SUBREDDITS_IN_AGG_SUMMARY NUMERIC DEFAULT 20;

-- ==================
-- TFIDF & BM25 parameters
-- ===
-- Num of words to show per cluster
DECLARE TOP_N_WORDS_FROM_TFIDF NUMERIC DEFAULT 12;
DECLARE TOP_N_WORDS_FROM_BM25 NUMERIC DEFAULT 12;


WITH
-- Get pre-computed TF-IDF & BM25 table
tf_idf_single_row_per_cluster AS (
    SELECT
        tf.k_0100_label_name
        , STRING_AGG(ngram, ', ') AS top_keywords
    FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_cluster_k100_tfidf_bm25` AS tf
        WHERE 1=1
            AND (
                ngram_rank_bm25 <= 12       -- TOP_N_WORDS_FROM_BM25
                OR ngram_rank_tfidf <= 12   -- TOP_N_WORDS_FROM_TFIDF
            )

    GROUP BY 1
)


-- Define US subreddits
, subreddits_us_relevant AS (
    SELECT
        sa.* EXCEPT(users_l7_rank_100, users_l7_rank_400)
        -- TODO(djb): rank over new manual cluster name!
        , ROW_NUMBER() OVER (PARTITION BY k_0100_label ORDER BY users_l7 DESC, users_l28 DESC) as users_l7_rank_100
    FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_activity` AS sa
        INNER JOIN `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220502` AS rel
            ON sa.subreddit_id = rel.subreddit_id
        -- TODO(djb): Merge with the new manual labels table so that we can get the rank by new cluster name
    WHERE 1=1
        AND (
            -- The US is so big that we're only taking into account the geo relevance default score
            geo_relevance_default = TRUE
        )
        AND geo_country_code = 'US'
)

, top_subreddits_us_per_cluster AS (
    SELECT
        k_0100_label
        , STRING_AGG(subreddit_name, ', ') AS top_subreddits
    FROM (
      -- TODO(djb): Change it so this only pulls from US subreddits
        SELECT
            k_0100_label
            , subreddit_name
            , users_l7_rank_400
            , users_l7_rank_100
            FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_activity`
            ORDER BY users_l7_rank_100
        )
    WHERE 1=1
        AND users_l7_rank_100 <= N_US_SUBREDDITS_IN_AGG_SUMMARY
    GROUP BY 1
)


SELECT COUNT(*)
FROM subreddits_us_relevant
;

-- -- Define Target-country subreddits
, subreddits_in_cluster_and_country AS (
    SELECT
        sa.* EXCEPT(users_l7_rank_100, users_l7_rank_400)
        , ROW_NUMBER() OVER (PARTITION BY k_0100_label ORDER BY users_l7 DESC, users_l28 DESC) as users_l7_rank_100
    FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_activity` AS sa
        INNER JOIN `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220502` AS rel
            ON sa.subreddit_id = rel.subreddit_id
    WHERE 1=1
        AND (
            geo_relevance_default = TRUE
            OR relevance_percent_by_subreddit = TRUE
            OR users_percent_by_country_standardized >= 3.0
        )
        AND geo_country_code IN ('DE')
)
, subreddits_in_country_cluster_agg AS (
    SELECT
        k_0100_label
        , COUNT(DISTINCT subreddit_id) AS subreddits_in_cluster_germany
        , SUM(users_l7) AS users_l7_cluster_total_germany
    FROM subreddits_in_cluster_and_country
    GROUP BY 1
)
, top_subreddits_per_cluster_country AS (
    SELECT
        sc.k_0100_label
        , STRING_AGG(subreddit_name, ', ') AS top_subreddits_germany

    FROM (
        SELECT
            k_0100_label
            , subreddit_name
            , users_l7_rank_100
        FROM subreddits_in_cluster_and_country
        ORDER BY k_0100_label ASC, users_l7_rank_100
     ) AS sc
    WHERE 1=1
        AND users_l7_rank_100 <= 20
    GROUP BY 1

)
, top_subs_and_agg_germany AS (
    SELECT
        sa.*
        , sc.top_subreddits_germany
    FROM subreddits_in_country_cluster_agg AS sa
        LEFT JOIN top_subreddits_per_cluster_country AS sc
            ON sa.k_0100_label = sc.k_0100_label
    ORDER BY 1
)


-- Get 1 row per cluster
SELECT
    tf.cluster_id AS k_0100_label

    -- , tf.cluster_id AS k_0400_label
    -- , pt.subreddits_in_cluster_count AS subreddits_in_cluster_count_400

    , pt1.top_topic AS top_topic_100
    , pt1.top_topic_percent AS top_topic_percent_100

    , pt1.top_topic AS top_topic_overall
    , pt1.top_topic_percent AS top_topic_percent_overall
    , pr1.top_rating AS top_rating_overall
    , top_rating_percent AS top_rating_percent_overall

    , pt1.subreddits_in_cluster_count AS subreddits_in_cluster_count_overall
    , tc.subreddits_in_cluster_germany

    , tc.users_l7_cluster_total_germany
    , ts.top_subreddits AS top_subreddits_overall
    , tc.top_subreddits_germany

    , top_keywords AS top_keywords_overall
    , pt1.cluster_primary_topics AS cluster_primary_topics_overall
    -- , pr1.* EXCEPT(k_0100_label, top_rating, subreddits_in_cluster_count, top_rating_percent)
FROM tf_idf_single_row_per_cluster tf
    LEFT JOIN top_subreddits_per_cluster AS ts
        ON tf.cluster_id = ts.k_0100_label
    LEFT JOIN top_subs_and_agg_germany AS tc
        ON tf.cluster_id = tc.k_0100_label

    -- topic & ratings from parent cluster ID
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_cluster_k100_top_topics` AS pt1
        ON tf.cluster_id = pt1.k_0100_label
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_cluster_k100_top_ratings` AS pr1
        ON tf.cluster_id = pr1.k_0100_label

    -- Need this to join both the k=100 and k=400 labels
    -- LEFT JOIN (
    --     SELECT DISTINCT k_0100_label, k_0400_label
    --     FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full`
    -- ) AS t
    --     ON tf.cluster_id = t.k_0400_label
        -- LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_cluster_k400_top_topics` AS pt
    --     ON tf.cluster_id = pt.k_0400_label
    -- LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_cluster_k400_top_ratings` AS pr
    --     ON tf.cluster_id = pr.k_0400_label

ORDER BY 1
;
