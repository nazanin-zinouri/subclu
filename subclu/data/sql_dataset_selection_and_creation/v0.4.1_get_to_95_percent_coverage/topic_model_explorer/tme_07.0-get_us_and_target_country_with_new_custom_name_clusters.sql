-- Get TF-IDF & BM25 at CLUSTER level for the US
--  The best strategy is to get the top N from TF-IDF and top M from BM25 b/c they get complementary results

-- ===============
-- Limits for geo-relevance
-- ===
DECLARE GEO_TARGET_COUNTRY_CODE STRING DEFAULT '{{ geo_country_code }}';

DECLARE MIN_USERS_PERCENT_BY_SUBREDDIT_L28 NUMERIC DEFAULT 0.18; -- default is 0.14 (14%)
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
-- Define US subreddits
subreddits_relevant_us AS (
    SELECT
        sa.* EXCEPT(
            users_l7_rank_100, users_l7_rank_400
            , posts_l7_rank_100, posts_l7_rank_400
            , app_users_l7, app_users_l28
        )
        , m.k_0100_label_name
        -- , m.k_0400_label_name

        -- Output table only groups by top level, so we don't need lower level rank here
        -- , ROW_NUMBER() OVER (PARTITION BY m.k_0400_label_name ORDER BY users_l7 DESC, users_l28 DESC) as users_l7_rank_subtopic
    FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_activity` AS sa
        INNER JOIN `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220502` AS rel
            ON sa.subreddit_id = rel.subreddit_id
        -- Merge with the new manual labels table so that we can get the rank by new cluster name
        LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_manual_names` AS m
            ON sa.k_0400_label = m.k_0400_label
    WHERE 1=1
        AND (
            -- The US is so big that we're only taking into account the geo relevance default score
            geo_relevance_default = TRUE
        )
        AND geo_country_code = 'US'
        -- Exclude NSFW subreddits in mostly SFW clusters
        AND COALESCE(m.use_for_global_tfidf, true) != false
)

, top_subreddits_per_cluster_us AS (
    -- Here we get the names of the top N subreddits in the country
    --  exclude over_18 subreddits for NAMES ONLY
    SELECT
        k_0100_label_name
        , STRING_AGG(subreddit_name, ', ' ORDER BY users_l7_rank_top_topic) AS top_subreddits_us

    FROM (
        SELECT
            k_0100_label_name
            , subreddit_name
            , ROW_NUMBER() OVER (PARTITION BY k_0100_label_name ORDER BY users_l7 DESC, users_l28 DESC) as users_l7_rank_top_topic
        FROM subreddits_relevant_us
        WHERE 1=1
            AND (
                k_0100_label_name NOT IN ('Porn & Celebrity', 'Relationships, Adult Content, Dating')
                AND COALESCE(over_18, '') != 't'
            )
            OR (
                k_0100_label_name IN ('Porn & Celebrity', 'Relationships, Adult Content, Dating')
            )
    )
    WHERE 1=1
        AND users_l7_rank_top_topic <= N_US_SUBREDDITS_IN_AGG_SUMMARY
    GROUP BY 1
)

, subreddits_agg_stats_per_cluster_us AS (
    -- Here we get stats for ALL subreddits in a country
    -- Get counts AND percentages
    SELECT
        k_0100_label_name
        , COUNT(DISTINCT subreddit_id) AS subreddits_in_cluster_count_us
        -- Don't calculate percentages of total here, do it in MODE b/c we can do it dynamically there
        -- , (
        --     COUNT(DISTINCT subreddit_id) * 1.0 /
        --     SUM(COUNT(DISTINCT subreddit_id)) OVER ()
        -- ) AS subreddits_in_cluster_pct_us

        , SUM(users_l7) AS users_l7_cluster_sum_us

        , SUM(seo_users_l7) AS seo_users_l7_cluster_sum_us
        , (
            SUM(seo_users_l7) * 1.0 /
            SUM(users_l7)
        ) AS seo_users_l7_pct_of_cluster_us

        , SUM(posts_l7) AS posts_l7_cluster_sum_us

    FROM subreddits_relevant_us
    GROUP BY 1
)
-- Join the top US subs and cluster aggregates into a single table
, top_subreddits_and_agg_per_cluster_us AS (
    SELECT
        st.top_subreddits_us
        , sa.*
    FROM subreddits_agg_stats_per_cluster_us AS sa
        LEFT JOIN top_subreddits_per_cluster_us AS st
            ON sa.k_0100_label_name = st.k_0100_label_name
    -- ORDER BY 1
)

-- Define Target-country subreddits
, subreddits_relevant_geo AS (
    SELECT
        sa.* EXCEPT(
            users_l7_rank_100, users_l7_rank_400
            , posts_l7_rank_100, posts_l7_rank_400
            , app_users_l7, app_users_l28
        )
        , m.k_0100_label_name

    FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_activity` AS sa
        INNER JOIN `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220502` AS rel
            ON sa.subreddit_id = rel.subreddit_id
        -- Merge with the new manual labels table so that we can get the rank by new cluster name
        LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_manual_names` AS m
            ON sa.k_0400_label = m.k_0400_label
    WHERE 1=1
        AND (
            -- For the target country include more subreddits that are relevant with other scores
            geo_relevance_default = TRUE
            OR users_percent_by_subreddit_l28 >= 0.14
            OR users_percent_by_country_standardized >= 3.0
        )
        AND geo_country_code = GEO_TARGET_COUNTRY_CODE

)
, top_subreddits_per_cluster_geo AS (
    -- Here we get the names of the top N subreddits in the country
    SELECT
        k_0100_label_name
        , STRING_AGG(subreddit_name, ', ' ORDER BY users_l7_rank_top_topic) AS top_subreddits_geo
    FROM (
        SELECT
            k_0100_label_name
            , subreddit_name
            , ROW_NUMBER() OVER (PARTITION BY k_0100_label_name ORDER BY users_l7 DESC, users_l28 DESC) as users_l7_rank_top_topic
        FROM subreddits_relevant_geo
        WHERE 1=1
            AND (
                k_0100_label_name NOT IN ('Porn & Celebrity', 'Relationships, Adult Content, Dating')
                AND COALESCE(over_18, '') != 't'
            )
            OR (
                k_0100_label_name IN ('Porn & Celebrity', 'Relationships, Adult Content, Dating')
            )
            -- Optional: Exclude NSFW subreddits in mostly SFW clusters
            -- AND COALESCE(m.use_for_global_tfidf, true) != false
    )
    WHERE 1=1
        AND users_l7_rank_top_topic <= N_GEO_SUBREDDITS_IN_AGG_SUMMARY
    GROUP BY 1
)
, subreddits_agg_stats_per_cluster_geo AS (
    -- Here we get stats for ALL subreddits in a country
    -- Get counts AND percentages
    SELECT
        k_0100_label_name
        , COUNT(DISTINCT subreddit_id) AS subreddits_in_cluster_count_geo

        , SUM(users_l7) AS users_l7_cluster_sum_geo

        , SUM(seo_users_l7) AS seo_users_l7_cluster_sum_geo
        , (
            SUM(seo_users_l7) * 1.0 /
            SUM(users_l7)
        ) AS seo_users_l7_pct_of_cluster_geo

        , SUM(posts_l7) AS posts_l7_cluster_sum_geo

    FROM subreddits_relevant_geo
    GROUP BY 1
)
-- Join the top GEO subs and cluster aggregates into a single table
, top_subreddits_and_agg_per_cluster_geo AS (
    SELECT
        st.top_subreddits_geo
        , sa.*
    FROM subreddits_agg_stats_per_cluster_geo AS sa
        LEFT JOIN top_subreddits_per_cluster_geo AS st
            ON sa.k_0100_label_name = st.k_0100_label_name
    -- ORDER BY 1
)

-- Merge US & Geo + Calculate over & under-index per cluster
, cluster_compare_us_v_geo AS (
    SELECT
        -- We need coalesce b/c not all clusters are guaranteed for all countries
        COALESCE(us.k_0100_label_name, geo.k_0100_label_name) AS k_0100_label_name
        , us.top_subreddits_us
        , geo.top_subreddits_geo
        , subreddits_in_cluster_count_us, subreddits_in_cluster_count_geo

        -- Calculate diff in MODE because we want to apply dynamic filters
        -- , (
        --     COALESCE(geo.subreddits_in_cluster_pct_geo, 0.0) -
        --     COALESCE(us.subreddits_in_cluster_pct_us, 0.0)
        -- ) AS subreddits_in_cluster_pct_diff_geo_from_us

        , us.* EXCEPT(k_0100_label_name, top_subreddits_us, subreddits_in_cluster_count_us)
        , geo.* EXCEPT(k_0100_label_name, top_subreddits_geo, subreddits_in_cluster_count_geo)
    FROM top_subreddits_and_agg_per_cluster_us AS us
        FULL OUTER JOIN top_subreddits_and_agg_per_cluster_geo AS geo
            ON us.k_0100_label_name = geo.k_0100_label_name
)
-- Get pre-computed TF-IDF & BM25 table
-- , tf_idf_single_row_per_cluster AS (
--     SELECT
--         tf.k_0100_label_name
--         , STRING_AGG(ngram, ', ') AS top_keywords
--     FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_cluster_k100_tfidf_bm25` AS tf
--     WHERE 1=1
--         AND (
--             ngram_rank_bm25 <= TOP_N_WORDS_FROM_BM25
--             OR ngram_rank_tfidf <= TOP_N_WORDS_FROM_TFIDF
--         )
--     GROUP BY 1
-- )


-- Get 1 row per cluster, final output
SELECT
    m.k_0100_label_name AS cluster_name

    , cc.* EXCEPT(k_0100_label_name)

    -- Exclude top keywords to speed up query
    -- , top_keywords AS cluster_top_keywords

    -- Topic summary. Exclude to speed up query
    -- , pt1.top_topic AS cluster_top_topic
    -- , pt1.top_topic_percent AS cluster_top_topic_percent
    -- , pt1.cluster_primary_topics AS cluster_primary_topics_and_pct

-- Start with the full cluster lest on the left because that way we guarantee we'll have infor for all clusters
--  Even if they're not in target country
FROM (
    SELECT DISTINCT k_0100_label_name
    FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_manual_names`
) AS m
    LEFT JOIN cluster_compare_us_v_geo AS cc
        ON m.k_0100_label_name = cc.k_0100_label_name

    -- Remove keywords for now b/c they take too much time. Maybe run as separate query
    -- LEFT JOIN tf_idf_single_row_per_cluster AS tf
    --     ON tf.k_0100_label_name = cc.k_0100_label_name

    -- Topic aggregates from parent cluster ID
    -- LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_cluster_k100_top_topics_manual` AS pt1
    --     ON cc.k_0100_label_name = pt1.k_0100_label_name

    -- Rating aggregates: don't use for now b/c they're not at the right grain
    -- LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_cluster_k100_top_ratings` AS pr1
    --     ON tf.cluster_id = pr1.k_0100_label

ORDER BY subreddits_in_cluster_count_geo DESC
;
