-- TF-IDF per cluster
-- This query fails because  we get errors with "cannot query rows larger than 100MB limit"
--  I tried cleaning up the text into a single row
WITH
preprocessed_text AS (
    -- Clean up the text before concat to prevent memory errors

    SELECT
        sc.k_0085_label AS id
        , REGEXP_REPLACE(
            REGEXP_REPLACE(
                CONCAT(
                    COALESCE(flair_text, '')
                    , ' ', COALESCE(post_url_for_embeddings, '')
                    , ' ', COALESCE(text, '')
                    , ' ', COALESCE(ocr_inferred_text_agg_clean, '')
                ), '&amp;', '&'
            ), r'&[a-z]{2,4};', '*'
        )AS clean_text

    FROM `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20211214` AS p
        INNER JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` AS sc
            ON p.subreddit_id = sc.subreddit_id
    WHERE 1=1
        AND sc.k_0085_label IN (
            1, 2, 10, 20, 30, 40, 50, 60, 70, 80, 84, 85
            , 11, 22, 33, 44, 55, 66, 77
        )
),
text_agg AS (
    -- Combine the text from all subreddits in a cluster into a single row
    SELECT
        id
        , STRING_AGG(LOWER(clean_text)) AS text_concat

    FROM preprocessed_text
    GROUP BY 1
),
words_by_cluster AS (
    SELECT
        id
        -- TODO(djb): test google's ngram function
        , REGEXP_EXTRACT_ALL(
            text_concat
            , r"[a-z]{2,20}\'?[a-z]+"
        ) AS words
        , COUNT(DISTINCT id) OVER() AS docs_n
    FROM text_agg
),
words_tf AS (
    SELECT
        id
        , word
        , COUNT(*) AS frequency_in_doc
        , COUNT(*) / ARRAY_LENGTH(ANY_VALUE(words)) tf
        , ARRAY_LENGTH(ANY_VALUE(words)) words_in_doc
        , ANY_VALUE(docs_n) docs_n
    FROM words_by_cluster, UNNEST(words) word
    GROUP BY id, word
    HAVING words_in_doc>30
),
docs_idf AS (
  SELECT
    tf.id
    , word
    , frequency_in_doc
    , tf.tf
    , ARRAY_LENGTH(tfs)             AS docs_with_word
    , ARRAY_LENGTH(tfs)/docs_n      AS df
    , LOG(docs_n/ARRAY_LENGTH(tfs)) AS idf
  FROM (
    SELECT
        word
        , ARRAY_AGG(STRUCT(frequency_in_doc, tf, id, words_in_doc)) tfs
        , ANY_VALUE(docs_n) docs_n
    FROM words_tf
    GROUP BY 1
  ), UNNEST(tfs) tf
),
tf_idf AS (
    SELECT
        *
        , tf*idf AS tfidf
    FROM docs_idf
    WHERE docs_with_word >= 2

),
tf_idf_with_rank AS (
    SELECT
        *
        , ROW_NUMBER() OVER (PARTITION BY id ORDER BY tfidf desc) as ngram_rank_in_cluster
    FROM tf_idf
)

-- final query
SELECT
    id
    , word
    , ngram_rank_in_cluster
    , tfidf
    , * EXCEPT(id, word, ngram_rank_in_cluster, tfidf)
FROM tf_idf_with_rank
WHERE 1=1
    AND ngram_rank_in_cluster <= 15
ORDER BY id ASC, ngram_rank_in_cluster ASC
;

-- 01 Test word counts
-- SELECT
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT id)    AS id_unique
--     , MAX(docs_n)  AS docs_n_max
--     , MIN(docs_n)  AS docs_n_min
-- FROM words_by_cluster
-- ;

-- 02 test words tf
-- SELECT
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT id)    AS id_unique
--     , MAX(docs_n)  AS docs_n_max
--     , MIN(docs_n)  AS docs_n_min
-- FROM words_by_cluster
-- ;
