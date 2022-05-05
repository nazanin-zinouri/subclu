-- Create test table to count ngrams once and then reuse them as a separate step

CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subreddit_ngram_test_20211215`
AS (
WITH
    preprocessed_text AS (
        -- This text has already been cleand up
        -- breaking it up doesn't work because we get errors with "cannot query rows larger than 100MB limit"
        SELECT
            subreddit_id
            , post_id
            , clean_text

        FROM `reddit-employee-datasets.david_bermejo.subreddit_text_test_20211215` AS p

    )
    , ngram_per_subreddit_raw AS (
        -- how many times each word is mentioned in a cluster
        SELECT
            subreddit_id
            , TRIM(ngram) as ngram
            , count(1) AS ngram_count
        FROM preprocessed_text, UNNEST(
            ML.NGRAMS(
                SPLIT(
                    REGEXP_REPLACE(LOWER(TRIM(clean_text)), r'\pP+|\s+|\pZ+', " ")
                    , ' '
                )
                , [1,3],  -- trigrams are about the limit
                ' '  -- character(s) to separate n-grams
            )
        ) as ngram
        WHERE ngram IS NOT NULL
            AND ngram NOT IN(
                '', ' ', ' ', '   ', '    ', '     '
                -- Common English tokens

            )
        GROUP BY subreddit_id, TRIM(ngram)
    )
    , ngram_per_subreddit AS (
        SELECT
            *
            , (1 + array_length(regexp_extract_all(ngram, ' '))) AS ngram_type
            , CHAR_LENGTH(ngram) AS ngram_char_len
        FROM ngram_per_subreddit_raw
    )


-- Select n-grams for tf-idf & BM25
SELECT
    n.subreddit_id
    , sc.subreddit_name
    , n.* EXCEPT(subreddit_id)
FROM ngram_per_subreddit AS n
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` AS sc
                ON n.subreddit_id = sc.subreddit_id
WHERE ngram_count >= 3

ORDER BY subreddit_id, ngram_count DESC

);  -- close CREATE table parens
