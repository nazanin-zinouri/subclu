WITH
preprocessed_text AS (
    -- Clean up the text before concat to prevent memory errors
    -- breaking it up doesn't work because we get errors with "cannot query rows larger than 100MB limit"
    SELECT
        sc.k_0118_label AS cluster_label
        , REGEXP_REPLACE(
            REGEXP_REPLACE(
                CONCAT(
                    COALESCE(flair_text, '')
                    , ' ', COALESCE(post_url_for_embeddings, '')
                    , ' ', COALESCE(text, '')
                    , ' ', COALESCE(ocr_inferred_text_agg_clean, '')
                ), r'&[a-z]{2,4};|https?:?|watch v|\w+\.[a-z]{2,3}', '' -- URL/UTM info
            ), r"(?i)&amp;|[\)!\('\.\"\]\[\*]+|[;%,-=_\+\$\?\<\>’~]+|^i\.|\|", ''
        )AS clean_text

    FROM `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20211214` AS p
        INNER JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` AS sc
            ON p.subreddit_id = sc.subreddit_id
    WHERE 1=1
        AND sc.k_0085_label IN (
            30, 40, 50
            -- ,  60, 70, 80, 84, 85
            -- , 11, 22, 33, 44, 55, 66, 77
            -- , 1, 2, 10, 20
        )
),
ngram_per_cluster AS (
    -- how many times each word is mentioned in a cluster
    SELECT
        cluster_label
        , ngram
        , count(1) AS ngram_count
    FROM preprocessed_text, UNNEST(
        ML.NGRAMS(
            SPLIT(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(LOWER(TRIM(clean_text)), r'(\pP)', r' '),
                    r"\s{2,}|\n *\n *\n*|\n", r" "
                )
                , ' '
            )
            , [1,2],
            ' '
        )
    ) as ngram
    WHERE ngram != ''
    GROUP BY cluster_label, ngram
),
ngram_total_per_cluster AS (
    -- Total words in each cluster
    SELECT
        cluster_label
        , SUM(ngram_count) AS total_count
        , COUNT(DISTINCT cluster_label) AS n_docs
    FROM ngram_per_cluster
    GROUP BY cluster_label
),
ngram_tf AS (
    -- Term-Frequency for ngram in cluster
    SELECT
        n.cluster_label
        , ngram
        , n.ngram_count / t.total_count AS tf
        , ngram_count
    FROM ngram_per_cluster AS n
        INNER JOIN ngram_total_per_cluster AS t
            USING(cluster_label)
)
, ngram_in_docs AS (
    -- how many "documents"/clusters have a word
    SELECT
        ngram
        , COUNT(DISTINCT cluster_label) n_docs_with_ngram
    FROM ngram_per_cluster
    GROUP BY 1
)
, total_docs AS (
    --total # of docs, need for idf
    SELECT COUNT(DISTINCT cluster_label) AS n_docs
    FROM preprocessed_text
)
, idf AS (
    -- df & idf for an ngram
    SELECT
        n.ngram
        , n.n_docs_with_ngram
        , n_docs
        , n_docs_with_ngram / t.n_docs         AS df
        , LOG(t.n_docs / n_docs_with_ngram)    AS idf
    FROM ngram_in_docs AS n
        CROSS JOIN total_docs AS t
)




-- Check ngram-counts (frequency)
-- SELECT
--     *
-- FROM ngram_per_cluster
-- WHERE ngram_count >=4500
-- ORDER BY id ASC, ngram_count DESC
-- ;

-- Check total ngrams per cluster
-- SELECT
--     *
-- FROM ngram_total_per_cluster
-- ;


-- Check Term frequency
-- SELECT *
-- FROM ngram_tf
-- WHERE ngram_count >=4500
-- LIMIT 1000
-- ;

-- Check idf (and doc frequency)
SELECT *
FROM idf
-- WHERE df >= 0.75
LIMIT 1000
;
