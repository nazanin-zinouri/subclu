-- Create test table to count ngrams once and then reuse them as a separate step

CREATE TABLE `reddit-employee-datasets.david_bermejo.subreddit_ngram_test_20211214`
AS (
WITH
    preprocessed_text AS (
        -- Clean up the text before concat to prevent memory errors
        -- breaking it up doesn't work because we get errors with "cannot query rows larger than 100MB limit"
        SELECT
            p.subreddit_id
            , post_id
            , REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        CONCAT(
                            COALESCE(flair_text, '')
                            , ' ', COALESCE(post_url_for_embeddings, '')
                            , ' ', COALESCE(text, '')
                            , ' ', COALESCE(ocr_inferred_text_agg_clean, '')
                        ), r'&[a-z]{2,4};|https?:?|watch v|\w+\.[a-z]{2,3}', '' -- URL/UTM info
                    ), r"(?i)&amp;|[\)!\('\.\"\]\[\*]+|[;%,-=_\+\$\?\<\>’~]+|^i\.|\|", ''
                ), r"\s{2,}|\n *\n *\n*|\n", r" " -- extra spaces
            ) AS clean_text

        FROM `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20211214` AS p
            INNER JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` AS sc
                ON p.subreddit_id = sc.subreddit_id
        WHERE 1=1
            -- filter by sub name
            AND p.subreddit_name IN (
                'legaladvice', 'fatfire'
                , 'newparents', 'medicine',
                , 'netherlands', 'london'
                , 'lgbt'
                , 'cooking'
                , 'fuckcars'
                , 'ucla', 'maliciouscompliance'
                , 'writing', 'relationship_advice', 'fitness'
                , 'wallstreetbets', 'ethereum'
                , 'torontoraptors', 'formula1'
                , 'de', 'mexico', 'france', 'argentina', 'india'
                , 'me_irl', 'china_irl', 'newsg', 'ich_iel'
                , 'skyrim', 'breath_of_the_wild', 'gaming', 'steam'
            )
            -- filter by label
            -- AND sc.k_0085_label IN (
            --     30, 40, 50
                -- ,  60, 70, 80, 84, 85
                -- , 11, 22, 33, 44, 55, 66, 77
                -- , 1, 2, 10, 20
            -- )
    )
    , ngram_per_subreddit AS (
        -- how many times each word is mentioned in a cluster
        SELECT
            subreddit_id
            , ngram
            , count(1) AS ngram_count
        FROM preprocessed_text, UNNEST(
            ML.NGRAMS(
                SPLIT(
                    REGEXP_REPLACE(LOWER(TRIM(clean_text)), r'(\pP)', r' ')
                    , ' '
                )
                , [1,3],  -- trigrams are about the limit
                ' '  -- character(s) to separate n-grams
            )
        ) as ngram
        WHERE ngram IS NOT NULL
            AND ngram NOT IN('', ' ', ' ', '   ', '    ', '     ')
        GROUP BY subreddit_id, ngram
    )

SELECT *
FROM ngram_per_subreddit

ORDER BY subreddit_id, ngram_count DESC

)
