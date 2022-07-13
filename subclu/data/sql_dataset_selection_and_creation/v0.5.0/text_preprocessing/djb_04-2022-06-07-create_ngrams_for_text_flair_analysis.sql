-- Create test table to count ngrams once and then reuse them as a separate step

DECLARE REGEX_REPLACE_WITH_SPACE_STR STRING DEFAULT
    r"(?i)\.|[:_-]|\(|\)|\[|\]|\/|\\|\|\"";
DECLARE REGEX_REMOVE_2ND_PASS_STR STRING DEFAULT
    r"\.";


CREATE TABLE `reddit-employee-datasets.david_bermejo.subclu_flair_ngrams_20220607`
AS (
WITH
    preprocessed_text AS (
        -- Clean up the text before concat to prevent memory errors
        -- breaking it up doesn't work because we get errors with "cannot query rows larger than 100MB limit"
        SELECT
            p.subreddit_id
            , post_id
            -- Need to coalesce in case the regexes return an empty string
            , COALESCE(TRIM(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            LOWER(TRIM(flair_text))
                            , REGEX_REPLACE_WITH_SPACE_STR, ' '
                        ), REGEX_REMOVE_2ND_PASS_STR, ' ' -- replace common items with space
                    ), r"\s{2,}", r" " -- remove extra spaces
                  )
            ), 'BLANK') AS clean_text

        FROM `reddit-relevance.tmp.subclu_posts_for_modeling_20220606` AS p

        WHERE 1=1
            AND flair_text IS NOT NULL

            -- filter by sub name
            -- AND p.subreddit_name IN (
            --     'formula1'
            --     , 'me_irl', 'china_irl'
            --     , 'newsg', 'ich_iel'
            --     , 'askreddit', 'fragreddit'
            --     , 'legaladvice', 'fatfire'
            --     , 'newparents', 'medicine'
            --     , 'netherlands', 'london'
                -- , 'lgbt'
                -- , 'cooking'
                -- , 'fuckcars', 'cars', 'cycling'
                -- , 'ucla', 'maliciouscompliance'
                -- , 'writing', 'relationship_advice', 'fitness'
                -- , 'wallstreetbets', 'ethereum'
                -- , 'foofighters', 'edm'
                -- , 'torontoraptors', 'baseball', 'nhl', 'nba', 'soccer', 'nfl', 'mma'
                -- , 'de', 'mexico', 'france', 'argentina', 'india', 'memexico'
                -- , 'explainlikeimfive', 'space', 'pics', 'economy'
                -- , 'worldnews', 'todayilearned'
                -- , 'skyrim', 'breath_of_the_wild', 'gaming', 'steam'
            -- )
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
                    clean_text
                    -- REGEXP_REPLACE(TRIM(clean_text), r'(\pP)', r" \1 ")  -- this helps split emoji, but might be overkill for flair
                    , ' '
                )
                , [1,1],  -- trigrams are about the limit
                ' '  -- character(s) to separate n-grams
            )
        ) as ngram
        WHERE ngram IS NOT NULL
            AND ngram NOT IN('', ' ', ' ', '   ', '    ', '     ')
        GROUP BY subreddit_id, TRIM(ngram)
    )
    , ngram_per_subreddit AS (
        SELECT
            *
            , (1 + array_length(regexp_extract_all(ngram, ' '))) AS ngram_type
            , CHAR_LENGTH(ngram) AS ngram_char_len
        FROM ngram_per_subreddit_raw
    )


-- Check clean text regexes
-- SELECT *
-- FROM preprocessed_text
-- ;

-- Select n-grams for tf-idf & BM25
SELECT *
FROM ngram_per_subreddit

ORDER BY subreddit_id, ngram_count DESC

)  -- close CREATE table parens
