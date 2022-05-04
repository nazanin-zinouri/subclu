WITH

ngram_counts_per_subreddit AS (
    -- By default start with subreddit level, need to change this to get cluster-level
    SELECT *
    FROM `reddit-employee-datasets.david_bermejo.subreddit_ngram_test_20211214`
)

, ngram_total_words AS (
    -- Total words in each CLUSTER or SUBBREDDIT
    --  If we want to do it by cluster we'd need to join with a table that has cluster IDs
    SELECT
        subreddit_id  -- change this param to get a cluster grouping
        , COUNT(*) OVER() AS n_docs  -- how many subreddits/clusters (for idf)

        , SUM(ngram_count) AS total_count

    FROM ngram_counts_per_subreddit
    GROUP BY 1
)
, ngram_tf AS (
    -- Term-Frequency for ngram in cluster
    SELECT
        n.subreddit_id
        , n.ngram
        , n.ngram_count / t.total_count AS tf
        , ngram_count
    FROM ngram_counts_per_subreddit AS n
        INNER JOIN ngram_total_words AS t
            USING(subreddit_id)
)
, ngram_in_docs AS (
    -- How many "documents" have a word
    --  docs could be subreddits or clusters
    SELECT
        ngram
        , COUNT(DISTINCT subreddit_id) n_docs_with_ngram
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
    FROM ngram_in_docs AS n
        CROSS JOIN (
           SELECT DISTINCT
              n_docs
           FROM ngram_total_words
        ) AS t
)

-- Check totals
-- SELECT *
-- FROM ngram_total_words
-- ;


-- Check TF
-- SELECT
--     a.subreddit_name
--     , ntf.*
-- FROM ngram_tf AS ntf
--     LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` AS a
--       USING (subreddit_id)
-- ORDER BY tf DESC
-- LIMIT 3000


-- Check ngram in docs
-- SELECT
--     *
-- FROM ngram_in_docs AS nd
--     WHERE 1=1
--         -- no idea why there are some ngrams with whitespace b/c this doesn't get rid of them
--         AND ngram != '   '
-- ORDER BY n_docs_with_ngram DESC
-- LIMIT 3000
-- ;

-- Check IDF
SELECT *
FROM ngram_idf
ORDER BY idf DESC
LIMIT 3000
;
