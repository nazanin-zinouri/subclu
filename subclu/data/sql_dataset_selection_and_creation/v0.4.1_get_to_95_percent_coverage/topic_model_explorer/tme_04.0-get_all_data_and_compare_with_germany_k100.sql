-- Get TF-IDF & BM25 at CLUSTER level
--  The best strategy might be to get the top N from TF-IDF and top M from BM25

-- EXCLUDE rare words
DECLARE MIN_NGRAM_COUNT DEFAULT 80;
DECLARE MIN_DOCS_WITH_NGRAM NUMERIC DEFAULT 2;  -- 1= no filter
DECLARE MIN_DF NUMERIC DEFAULT 0.0;  -- 0= no filter. higher num -> exclude rare words

-- EXCLUDE common words
-- 1.0= no filter. lower num -> exclude MORE words
--    Example: 0.9 -> exclude words tht appear in 90% of documents (clusters)
DECLARE MAX_DF NUMERIC DEFAULT 0.985;

-- k1 = 1.2 term frequency saturation paramete.
--  [0,3] Could be higher than 3 | [0.5,2.0] "Optimal" starting range
--  High -> staturation is slower (books)
--  Low  -> downweight counts quickly (news articles)
DECLARE K1 NUMERIC DEFAULT 24.0;

-- b  = 0.75 doc length penalty.  0 -> no penalty
--  [0,1] MUST be between 0 & 1 | [0.3, 0.9] "optimal" starting range
--  High -> broad articles, penalize a lot
--  Low  -> detailed/focused technical articles (low penalty)
DECLARE B NUMERIC DEFAULT 0.40;


WITH ngram_counts_per_subreddit AS (
    -- By default start with subreddit level, need to change this to get cluster-level
    SELECT
        -- Set the cluster grain here:
        a.k_0100_label AS cluster_id
        , n.ngram
        , SUM(ngram_count) AS ngram_count
    FROM `reddit-employee-datasets.david_bermejo.subreddit_ngram_test_20211215` AS n
        LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full` AS a
            USING (subreddit_id)
        LEFT JOIN (
            SELECT DISTINCT TRIM(ngram) AS ngram
            FROM `reddit-employee-datasets.david_bermejo.ngram_stop_words`
        ) AS sw
            ON n.ngram = sw.ngram
    WHERE 1=1
        -- AND n.ngram_char_len >= 2
        AND ngram_count >= MIN_NGRAM_COUNT
        -- Exclude stop words
        AND sw.ngram IS NULL

        -- For testing, filter subreddit names here, otherwise the IDF will be wrong

    -- WE NEED TO GROUP BY because otherwise we'll get duplicate ngrams per cluster
    GROUP BY 1, 2
)

, ngram_total_words AS (
    -- Total words in each CLUSTER or SUBBREDDIT
    --  If we want to do it by cluster we'd need to join with a table that has cluster IDs
    SELECT
        cluster_id  -- change this param to get a cluster grouping

        , COUNT(*) OVER() AS n_docs  -- how many subreddits/clusters (for idf)
        , SUM(ngram_count) AS total_count

    FROM ngram_counts_per_subreddit
    GROUP BY 1
)
, avg_ngrams_per_subreddit AS (
    -- We need this average for BM25
    SELECT
        AVG(total_count) AS avg
    FROM ngram_total_words
)
, ngram_tf AS (
    -- Term-Frequency for ngram in cluster
    SELECT
        n.cluster_id
        , n.ngram
        , ngram_count
        , n.ngram_count / t.total_count AS tf
        , ngram_count / (
            ngram_count +
            K1 * (
                1.0 - B +
                B * total_count / (SELECT avg FROM avg_ngrams_per_subreddit)
            )
        ) AS tf_bm25

    FROM ngram_counts_per_subreddit AS n
        INNER JOIN ngram_total_words AS t
            USING(cluster_id)
)
, ngram_in_docs AS (
    -- How many "documents" have a word
    --  docs could be subreddits or clusters
    SELECT
        ngram
        , COUNT(DISTINCT cluster_id) n_docs_with_ngram
    FROM ngram_counts_per_subreddit
    GROUP BY 1
)
, ngram_idf AS (
    -- df & idf for an ngram
    SELECT
        n.ngram
        , n.n_docs_with_ngram
        , n_docs
        , n_docs_with_ngram / t.n_docs         AS df
        , LOG(t.n_docs / n_docs_with_ngram)    AS idf
        , LN(1 + (n_docs - n_docs_with_ngram + 0.5) / (n_docs_with_ngram + 0.5)) as idf_prob
    FROM ngram_in_docs AS n
        CROSS JOIN (
           SELECT DISTINCT
              n_docs
           FROM ngram_total_words
        ) AS t
)
, tf_idf_and_bm25_raw AS (
    -- We can save this "raw" table
    --   and apply filters on demand like: min count, min & max df
    SELECT
        t.*
        , i.* EXCEPT(ngram)
        , tw.* EXCEPT(cluster_id, n_docs)
        , tf * idf AS tfidf
        , tf_bm25 * idf_prob AS bm25

    FROM ngram_tf AS t
        LEFT JOIN ngram_idf AS i
            USING(ngram)
        LEFT JOIN ngram_total_words AS tw
            USING(cluster_id)
)
, tf_idf_with_rank AS (
    SELECT
        t.cluster_id
        , t.ngram
        , ((ngram_rank_bm25 + ngram_rank_tfidf) / 2) AS ngram_rank_avg
        , ngram_type
        , ngram_char_len
        , t.* EXCEPT(cluster_id, ngram)
    FROM (
        SELECT
            cluster_id
            , ngram
            , ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY bm25 DESC, ngram_count DESC) as ngram_rank_bm25
            , ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY tfidf DESC, ngram_count DESC) as ngram_rank_tfidf
            , bm25
            , tfidf
            , ngram_count
            , * EXCEPT(cluster_id, ngram, tfidf, bm25, ngram_count)
        FROM tf_idf_and_bm25_raw
        WHERE 1=1
            -- We need to filter out before we calculate ranks
            AND df >= MIN_DF
            AND df <= MAX_DF
            AND n_docs_with_ngram >= MIN_DOCS_WITH_NGRAM
    ) AS t
    -- Join to get the len & type of ngram (unigram, bigram, trigram)
    LEFT JOIN (
        SELECT DISTINCT ngram, ngram_type, ngram_char_len
        FROM `reddit-employee-datasets.david_bermejo.subreddit_ngram_test_20211215`
    ) AS n
        ON t.ngram = n.ngram
)
, tf_idf_with_rank_and_limits AS (
    SELECT
        t.cluster_id
        -- , a.cluster_name
        , t.* EXCEPT(cluster_id)
    FROM tf_idf_with_rank AS t
        -- TODO(djb) Need to join to new table to get subreddit cluster name
        -- LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` AS a

    WHERE 1=1
        AND (
            ngram_rank_bm25 <= 10
            OR ngram_rank_tfidf <= 10
        )

    ORDER BY cluster_id, ngram_rank_avg
)
, tf_idf_single_row_per_cluster AS (
    SELECT
        tf.cluster_id
        , STRING_AGG(ngram, ', ') AS top_keywords
    FROM tf_idf_with_rank_and_limits tf

    GROUP BY 1
)
, top_subreddits_per_cluster AS (
    SELECT
        k_0100_label
        , STRING_AGG(subreddit_name, ', ') AS top_subreddits
    FROM (
        SELECT
            k_0100_label
            , subreddit_name
            , users_l7_rank_400
            , users_l7_rank_100
            FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_activity`
            ORDER BY users_l7_rank_100
        )
    WHERE 1=1
        AND users_l7_rank_100 <= 15
    GROUP BY 1
)
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
