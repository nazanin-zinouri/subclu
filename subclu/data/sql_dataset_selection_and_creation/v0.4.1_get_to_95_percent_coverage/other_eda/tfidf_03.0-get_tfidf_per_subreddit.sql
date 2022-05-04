-- Get TF-IDF at subreddit level
DECLARE MIN_NGRAM_COUNT DEFAULT 10;
DECLARE MIN_DF NUMERIC DEFAULT 0.06;
DECLARE MAX_DF NUMERIC DEFAULT 0.92;

WITH ngram_counts_per_subreddit AS (
    -- By default start with subreddit level, need to change this to get cluster-level
    SELECT
        * EXCEPT(ngram)
        , TRIM(ngram) as ngram
    FROM `reddit-employee-datasets.david_bermejo.subreddit_ngram_test_20211214`
    WHERE 1=1
        AND COALESCE(TRIM(ngram), '') NOT IN (
            -- German
            'eine', 'einen', 'für', 'nicht', 'der', 'wenn', 'dass', 'dann'
            , 'ich', 'und', 'zu', 'sich', 'von', 'als', 'meine', 'wird', 'sind'
            , 'jetzt', 'aber'

            -- English
            , 'i', 'the', 'they', 'and', 'that', 'you', 'to', 'to the'
            , 'she', 'shes', 'her', 'i was', 'she was', 'he was', 'that she', 'that he', 'that i'
            , 'her and', 'didnt', 'her to', 'she has', 'he has', 'what to', 'what to do', 'to do'
            , 'with him', 'with her', 'with me', 'and she', 'and he', 'and i', 'and you'
            , 'told her', 'told him', 'told me', 'at what', 'but at'
            , 'be sure to', 'be sure', 'where you can', 'an answer'
            , 'often as', 'if you didnt', 'if you', 'you can also'
            , 'for', 'this', 'get', 'the city'
            , 'when your', 'when my', 'when her', 'when his', 'when i'
            , 'made with', 'how do', 'what if', 'why do', 'here to'
            , 'view all comments', 'to sort', 'asked that', 'click here'
            , 'you can', 'i can', 'he can', 'she can', 'we can', 'they can'
            , 'to your', 'to my', 'to his', 'to her', 'to us', 'to them'
            , 'get a lot'

            -- French, Spanish, Others
            , 'de la', 'à', 'en la', 'en el', 'a', 'me', 'el', 'una', 'del'
            , 'je', 'por', 'a la', 'que se', 'que les'
        )
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
, tf_idf_raw AS (
    -- We can save this "raw" table
    --   and apply filters on demand like: min count, min & max df
    SELECT
        *
        , tf*idf AS tfidf
    FROM ngram_tf AS t
        LEFT JOIN ngram_idf AS i
            USING(ngram)
)
, tf_idf_with_rank AS (
    SELECT
        subreddit_id
        , ngram
        , ROW_NUMBER() OVER (PARTITION BY subreddit_id ORDER BY tfidf DESC, ngram_count DESC) as ngram_rank_in_subreddit
        , tfidf
        , ngram_count
        , * EXCEPT(subreddit_id, ngram, tfidf, ngram_count)
    FROM tf_idf_raw
    WHERE 1=1
        AND df >= MIN_DF
        AND df <= MAX_DF
    ORDER BY subreddit_id ASC, tfidf DESC
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
-- SELECT *
-- FROM ngram_idf
-- ORDER BY idf DESC
-- LIMIT 3000
-- ;


-- Check TF-IDF
SELECT a.subreddit_name, t.*
FROM tf_idf_with_rank AS t
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` AS a
        USING (subreddit_id)
WHERE ngram_rank_in_subreddit <= 40
ORDER BY subreddit_name, ngram_rank_in_subreddit
LIMIT 3000
;
