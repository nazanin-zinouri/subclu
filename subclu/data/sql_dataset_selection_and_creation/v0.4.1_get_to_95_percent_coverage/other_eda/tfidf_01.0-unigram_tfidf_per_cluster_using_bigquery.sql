-- USE TF-IDF to get rank of most common words per cluster
--  based on:
--  https://stackoverflow.com/questions/47028576/how-can-i-compute-tf-idf-with-sql-bigquery

WITH words_by_cluster AS (
  SELECT
    sc.k_0085_label AS id,
    -- TODO(djb): test google's ngram function
    REGEXP_EXTRACT_ALL(
        REGEXP_REPLACE(REGEXP_REPLACE(LOWER(CONCAT(
            COALESCE(flair_text, '')
            , ' ', COALESCE(post_url_for_embeddings, '')
            , ' ', COALESCE(text, '')
            , ' ', COALESCE(ocr_inferred_text_agg_clean, '')
        )), '&amp;', '&'), r'&[a-z]{2,4};', '*')
        , r"[a-z]{2,20}\'?[a-z]+") AS words
    , COUNT(*) OVER() docs_n
  FROM `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20211214` AS p
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` AS sc
        ON p.subreddit_id = sc.subreddit_id
  WHERE 1=1
    AND sc.k_0085_label IN (20, 69, 84)

),
words_tf AS (
  SELECT id, word, COUNT(*) / ARRAY_LENGTH(ANY_VALUE(words)) tf, ARRAY_LENGTH(ANY_VALUE(words)) words_in_doc
    , ANY_VALUE(docs_n) docs_n
  FROM words_by_cluster, UNNEST(words) word
  GROUP BY id, word
  HAVING words_in_doc>30
),
docs_idf AS (
  SELECT
    tf.id,
    word,
    tf.tf,
    ARRAY_LENGTH(tfs) docs_with_word,
    LOG(docs_n/ARRAY_LENGTH(tfs)) idf
  FROM (
    SELECT word, ARRAY_AGG(STRUCT(tf, id, words_in_doc)) tfs, ANY_VALUE(docs_n) docs_n
    FROM words_tf
    GROUP BY 1
  ), UNNEST(tfs) tf
),
tf_idf AS (
    SELECT
        *
        , tf*idf AS tfidf
    FROM docs_idf
    WHERE docs_with_word > 1
    ORDER BY id ASC, tfidf DESC
),
tf_idf_with_rank AS (
    SELECT
        *
        , ROW_NUMBER() OVER (PARTITION BY id ORDER BY tfidf desc) as ngram_rank_in_cluster
    FROM tf_idf
)

SELECT
    *
FROM tf_idf_with_rank
WHERE 1=1
    AND ngram_rank_in_cluster <= 10
;
