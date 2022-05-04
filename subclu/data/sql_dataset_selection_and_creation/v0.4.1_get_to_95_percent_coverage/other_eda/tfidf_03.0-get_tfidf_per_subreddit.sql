-- Get TF-IDF & BM25 at subreddit level
--  The best strategy might be to get the top 5 from TF-IDF and top 5 from BM25

DECLARE MIN_NGRAM_COUNT DEFAULT 11;
DECLARE MIN_DF NUMERIC DEFAULT 0.06;
DECLARE MAX_DF NUMERIC DEFAULT 0.98;

-- k1 = 1.2 term frequency saturation paramete.
--  [0,3] Could be higher than 3 | [0.5,2.0] "Optimal" starting range
--  High -> staturation is slower (books)
--  Low  -> downweight counts quickly (news articles)
DECLARE K1 NUMERIC DEFAULT 32.0;

-- b  = 0.75 doc length penalty.  0 -> no penalty
--  [0,1] MUST be between 0 & 1 | [0.3, 0.9] "optimal" starting range
--  High -> broad articles, penalize a lot
--  Low  -> detailed/focused technical articles (low penalty)
DECLARE B NUMERIC DEFAULT 0.30;


WITH ngram_counts_per_subreddit AS (
    -- By default start with subreddit level, need to change this to get cluster-level
    SELECT
        * EXCEPT(ngram)
        , TRIM(ngram) as ngram
    FROM `reddit-employee-datasets.david_bermejo.subreddit_ngram_test_20211214`
    WHERE 1=1
        AND COALESCE(TRIM(ngram), '') NOT IN (
            -- German
            'eine', 'einen', 'einem', 'für', 'nicht', 'der', 'wenn', 'dass', 'dann'
            , 'ich', 'und', 'zu', 'sich', 'von', 'als', 'meine', 'meines', 'meinen', 'wird', 'sind'
            , 'jetzt', 'aber', 'in der', 'mehr', 'zum', 'keine', 'keinen', 'wie', 'wir', 'haben'
            , 'ich dann', 'irgendwann', 'ist', 'auf'

            -- English
            , 'i', 'the', 'they', 'and', 'that', 'you', 'to', 'to the'
            , 'she', 'shes', 'him', 'her', 'i was', 'she was', 'he was', 'that she', 'that he', 'that i'
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
            , 'you to', 'me to', 'he to', 'her to', 'us to', 'them to'
            , 'get a lot', 'but she', 'but he', 'but i', 'but we', 'i dont'
            , 'i am', 'she is', 'he is', 'you are', 'they are', 'we are'
            , 'i was', 'she was', 'he was', 'you were', 'they were', 'we were'
            , 'my'

            -- French, Spanish, Others
            , 'de la', 'à', 'en la', 'en el', 'a', 'me', 'el', 'una', 'del'
            , 'je', 'por', 'a la', 'de la', 'de los', 'lo que'
            , 'que', 'que se', 'que les', 'qué', 'más', 'que no', 'nous'
            , 'pero', 'algo', 'muy', 'nada', 'hace', 'hacer', 'tengo', 'tiene'
            , 'hasta', 'de las', 'desde', 'no se', 'no me', 'no te', 'no le'
            , 'estaba', 'cuando', 'como', 'esta'
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
, avg_ngrams_per_subreddit AS (
    -- We need this average for BM25
    SELECT
        AVG(total_count) AS avg
    FROM ngram_total_words
)
, ngram_tf AS (
    -- Term-Frequency for ngram in cluster
    SELECT
        n.subreddit_id
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
        , tw.* EXCEPT(subreddit_id, n_docs)
        , tf * idf AS tfidf
        , tf_bm25 * idf_prob AS bm25

    FROM ngram_tf AS t
        LEFT JOIN ngram_idf AS i
            USING(ngram)
        LEFT JOIN ngram_total_words AS tw
            USING(subreddit_id)
)
, tf_idf_with_rank AS (
    SELECT
        subreddit_id
        , ngram
        , ROW_NUMBER() OVER (PARTITION BY subreddit_id ORDER BY bm25 DESC, ngram_count DESC) as ngram_rank_bm25
        , ROW_NUMBER() OVER (PARTITION BY subreddit_id ORDER BY tfidf DESC, ngram_count DESC) as ngram_rank_tfidf
        , bm25
        , tfidf
        , ngram_count
        , * EXCEPT(subreddit_id, ngram, tfidf, bm25, ngram_count)
    FROM tf_idf_and_bm25_raw
    WHERE 1=1
        AND df >= MIN_DF
        AND df <= MAX_DF
        AND ngram_count >= MIN_NGRAM_COUNT
    ORDER BY subreddit_id ASC, tfidf DESC
)


-- Check TF-IDF & BM25
SELECT
    t.subreddit_id
    , a.subreddit_name
    , t.* EXCEPT(subreddit_id)
FROM tf_idf_with_rank AS t
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` AS a
        USING (subreddit_id)
WHERE 1=1
    AND (
        ngram_rank_bm25 <= 5
        OR ngram_rank_tfidf <= 5
    )
ORDER BY subreddit_name, ngram_rank_bm25
LIMIT 3000
;


-- Check total words
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
-- ;


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
