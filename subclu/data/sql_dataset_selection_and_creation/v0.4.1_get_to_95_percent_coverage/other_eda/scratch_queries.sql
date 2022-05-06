SELECT *
FROM `reddit-employee-datasets.david_bermejo.subreddit_text_test_20211215`
WHERE 1=1
    AND subreddit_name IN (
        ''
        -- , '1110asleepshower'
        -- , '12datesofchristmastv'
        -- , '10provinces'
        -- , '123swap' -- long URLs
        , '169'  -- lots of empty ngrams
        -- , '2islamist4you'
        -- , '30mais'
        -- , '1fcnuernberg'
        -- , 'askreddit'
        -- , 'baseball'
        -- , '0hthaatsjaay'
        -- , '0sanitymemes'
        , 'mexico'
        -- , 'argentina'
        -- , 'de'
        -- , 'breath_of_the_wild'
    )

    -- AND post_id IN (
    --     't3_qfnm6n'
    --     , 't3_rg0bcy'
    --     , 't3_qm1fgw'

    --     , 't3_qegz1x'
    --     , 't3_qcuf0c'
    --     , 't3_qcg6uk'

    --     -- ich_iel
    --     , 't3_rbkkus'

    --     , 't3_rezxjd'
    --     , 't3_qu40um'
    -- )

ORDER BY subreddit_name
;



SELECT *
FROM `reddit-employee-datasets.david_bermejo.subreddit_ngram_test_20211215`
WHERE 1=1
  AND subreddit_name IN (
    -- '1fcnuernberg'
    '169'
  )
  -- AND ngram = r'\s'

LIMIT 1000







-- Test n-grams on a single post to check whether/how punctuation makes a difference
WITH
ngram_per_subreddit_raw AS (
        -- how many times each word is mentioned in a cluster
        SELECT
            subreddit_id
            , TRIM(ngram) as ngram
            , count(1) AS ngram_count
        FROM `reddit-employee-datasets.david_bermejo.subreddit_text_test_20211215`, UNNEST(
            ML.NGRAMS(
                SPLIT(
                    REGEXP_REPLACE(LOWER(TRIM(clean_text)), r'\pP+|\s+|\pZ+', " ")
                    -- r"\1 " would return the period AND a space after it.
                    -- In our case, we want to throw out the periods, so don't include the capturing group
                    -- Also make sure to remove the `r` at the beginning
                    , ' '
                )
                , [1,3],  -- trigrams are about the limit
                ' '  -- character(s) to separate n-grams
            )
        ) as ngram
        WHERE 1=1
            AND post_id IN (
                't3_qfnm6n'
                , 't3_rg0bcy'
                , 't3_qm1fgw'

                , 't3_qegz1x'
                , 't3_qcuf0c'
                , 't3_qcg6uk'

                -- ich_iel
                , 't3_rbkkus'

                , 't3_rezxjd'
                , 't3_qu40um'
            )
            -- AND ngram IS NOT NULL
            -- AND ngram NOT IN(
            --     '', ' ', ' ', '   ', '    ', '     '
            --     -- Common English tokens
            --     , 'the'
            -- )
        GROUP BY subreddit_id, TRIM(ngram)
    )

SELECT *
FROM ngram_per_subreddit_raw
WHERE ngram_count >= 2

ORDER BY subreddit_id, ngram_count DESC
-- ORDER BY ngram, ngram_count DESC
;


-- Test n-grams on a single post to check whether/how punctuation makes a difference
WITH
ngram_per_subreddit_raw AS (
        -- how many times each word is mentioned in a cluster
        SELECT
            subreddit_id
            , TRIM(ngram) as ngram
            , count(1) AS ngram_count
        FROM `reddit-employee-datasets.david_bermejo.subreddit_text_test_20211215`, UNNEST(
            ML.NGRAMS(
                SPLIT(
                    REGEXP_REPLACE(LOWER(TRIM(text)), r'\pP+|\pZ+', " ")
                    , ' '
                )
                , [1,3],  -- trigrams are about the limit
                ' '  -- character(s) to separate n-grams
            )
        ) as ngram
        WHERE 1=1
            AND post_id IN (
                't3_qfnm6n'
                , 't3_rg0bcy'
                , 't3_qm1fgw'

                , 't3_qegz1x'
                , 't3_qcuf0c'
                , 't3_qcg6uk'

                -- ich_iel
                , 't3_rbkkus'

                , 't3_rezxjd'
                , 't3_qu40um'

            )
            -- AND ngram IS NOT NULL
            -- AND ngram NOT IN(
            --     '', ' ', ' ', '   ', '    ', '     '
            --     -- Common English tokens
            --     , 'the'
            -- )
        GROUP BY subreddit_id, TRIM(ngram)
    )

SELECT *
FROM ngram_per_subreddit_raw
WHERE ngram_count >= 2
ORDER BY subreddit_id, ngram_count DESC
-- ORDER BY ngram, ngram_count DESC
;
