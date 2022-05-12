-- Get TF-IDF & BM25 at CLUSTER level for ALL subreddits
--  The best strategy is to get the top N from TF-IDF and top M from BM25 b/c they get complementary results

-- Num of words to save per cluster
-- DECLARE TOP_N_WORDS_FROM_TFIDF NUMERIC DEFAULT 50;
-- DECLARE TOP_N_WORDS_FROM_BM25 NUMERIC DEFAULT 50;


-- EXCLUDE rare words
-- DECLARE MIN_NGRAM_COUNT DEFAULT 60;
-- DECLARE MIN_DOCS_WITH_NGRAM NUMERIC DEFAULT 2;  -- 1= no filter
-- DECLARE MIN_DF NUMERIC DEFAULT 0.0;  -- 0= no filter. higher num -> exclude rare words

-- EXCLUDE common words
-- 1.0= no filter. lower num -> exclude MORE words
--    Example: 0.9 -> exclude words tht appear in 90% of documents (clusters)
-- DECLARE MAX_DF NUMERIC DEFAULT 0.98;

-- k1 = 1.2 term frequency saturation parameter; my default is ~28.0 for 100 clusters
--  [0,3] Could be higher than 3 | [0.5,2.0] "Optimal" starting range
--  High -> staturation is slower (books)
--  Low  -> downweight counts quickly (news articles)
-- DECLARE K1 NUMERIC DEFAULT 30.0;

-- b  = 0.75 doc length penalty.  0 -> no penalty
--  [0,1] MUST be between 0 & 1 | [0.3, 0.9] "optimal" starting range
--  High -> broad articles, penalize a lot
--  Low  -> detailed/focused technical articles (low penalty)
-- DECLARE B NUMERIC DEFAULT 0.30;


-- CREATE VIEW `reddit-employee-datasets.david_bermejo.subclu_v0041_cluster_k100_tfidf_bm25` AS (
WITH
ngram_counts_per_subreddit AS (
    -- By default start with subreddit level, need to change this to get cluster-level
    SELECT
        -- Set the cluster grain here:
        m.k_0100_label_name AS cluster_id
        , n.ngram
        , SUM(ngram_count) AS ngram_count
    FROM `reddit-employee-datasets.david_bermejo.subreddit_ngram_test_20211215` AS n
        -- Map subreddits to cluster IDs
        LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full` AS a
            USING (subreddit_id)
        -- Map cluster IDs to new manual labels
        LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_manual_names` AS m
            -- Join at lowest level to prevent dupes
            ON a.k_0400_label = m.k_0400_label
        LEFT JOIN (
            SELECT DISTINCT TRIM(ngram) AS ngram
            FROM `reddit-employee-datasets.david_bermejo.ngram_stop_words`
        ) AS sw
            ON n.ngram = sw.ngram
    WHERE 1=1
        -- AND n.ngram_char_len >= 2
        AND ngram_count >= 80 -- MIN_NGRAM_COUNT, ~10 @subreddit level, 90+ @k=100 clusters
        -- Exclude stop words
        AND sw.ngram IS NULL

        -- Exclude some porn/NSFW sub-clusters that are mixed with SFW clusters
        --   e.g., some subs at cluster 53 @k=100
        AND COALESCE(m.use_for_global_tfidf, true) != false

    -- WE NEED TO GROUP BY because otherwise we'll get duplicate ngrams per cluster
    GROUP BY 1, 2
)

, ngram_total_words AS (
    -- Total words in each CLUSTER
    SELECT
        cluster_id  -- change this param to get a cluster or subreddit

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

        -- Formula should be:
        --   ngram_count / (
        --      ngram_count + (
        --          K1 *
        --          (1 - B + B * total_count / avg_ngramgs)
        --      )
        --   )
        , ngram_count / (
            ngram_count +
            24.0 * -- K1
            (
                -- 1.0 - B + ...
                1.0 - 0.35 +
                -- B * total_count ...
                0.35 * total_count / (SELECT avg FROM avg_ngrams_per_subreddit)
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
            AND df >= 0.0   -- MIN_DF, default = 0.0
            AND df <= 0.98  -- MAX_DF, default = 0.98
            AND n_docs_with_ngram >= 2  -- MIN_DOCS_WITH_NGRAM, default = 2
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

    WHERE 1=1
        AND (
            ngram_rank_bm25 <= 50       -- TOP_N_WORDS_FROM_BM25
            OR ngram_rank_tfidf <= 50   -- TOP_N_WORDS_FROM_TFIDF
        )

    -- Order by TFIDF to get the most frequent words first
    ORDER BY cluster_id, ngram_rank_tfidf
)
, tf_idf_single_row_per_cluster AS (
    SELECT
        tf.cluster_id   -- may need to rename: cluster_id AS k_0100_label_name
        , STRING_AGG(ngram, ', ') AS top_keywords
    FROM tf_idf_with_rank_and_limits tf

    GROUP BY 1
)

-- Save top 100 keywords per cluster in view
-- Then I can always select fewer keywords and aggregate them in a separate query
SELECT
    cluster_id AS k_0100_label_name
    , * EXCEPT(cluster_id)
FROM tf_idf_with_rank_and_limits


-- Preview the aggregated text (1 row = 1 cluster)
-- SELECT
--     cluster_id AS k_0100_label_name
--     , * EXCEPT(cluster_id)
-- FROM tf_idf_single_row_per_cluster
-- ;

-- );  -- CLOSE CREATE VIEW parens
