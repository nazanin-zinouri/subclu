-- Get TF-IDF & BM25 at CLUSTER level
--  The best strategy might be to get the top N from TF-IDF and top M from BM25

-- EXCLUDE rare words
DECLARE MIN_NGRAM_COUNT DEFAULT 200;
DECLARE MIN_DOCS_WITH_NGRAM NUMERIC DEFAULT 2;  -- 1= no filter
DECLARE MIN_DF NUMERIC DEFAULT 0.0;  -- 0= no filter. higher num -> exclude rare words

-- EXCLUDE common words
-- 1.0= no filter. lower num -> exclude MORE words
--    Example: 0.9 -> exclude words tht appear in 90% of documents (clusters)
DECLARE MAX_DF NUMERIC DEFAULT 0.98;

-- k1 = 1.2 term frequency saturation paramete.
--  [0,3] Could be higher than 3 | [0.5,2.0] "Optimal" starting range
--  High -> staturation is slower (books)
--  Low  -> downweight counts quickly (news articles)
DECLARE K1 NUMERIC DEFAULT 24.0;

-- b  = 0.75 doc length penalty.  0 -> no penalty
--  [0,1] MUST be between 0 & 1 | [0.3, 0.9] "optimal" starting range
--  High -> broad articles, penalize a lot
--  Low  -> detailed/focused technical articles (low penalty)
DECLARE B NUMERIC DEFAULT 0.50;

-- CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subreddit_top_tfidf_bm25_20211215`
-- AS (
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

        -- For testing, filter subreddit names here, otherwise the IDF will be wrong

        -- Exclude stop words
        AND sw.ngram IS NULL


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
            ngram_rank_bm25 <= 9
            OR ngram_rank_tfidf <= 9
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


-- Get 1 row per cluster
SELECT
    tf.cluster_id
    , prt.subreddits_in_cluster_count
    , top_topic
    , top_topic_percent
    , prr.top_rating
    , top_rating_percent
    , top_keywords
    , cluster_primary_topics
    , prr.* EXCEPT(k_0100_label, top_rating, subreddits_in_cluster_count, top_rating_percent)
FROM tf_idf_single_row_per_cluster tf
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_cluster_k100_top_topics` AS prt
        ON tf.cluster_id = prt.k_0100_label
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_cluster_k100_top_ratings` AS prr
        ON tf.cluster_id = prr.k_0100_label

ORDER BY cluster_id
;
