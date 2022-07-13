-- Get ngrams from COMMENTS at a subreddit-level
--   1) Clean comment text
--   2) get n-gram counts
-- Calculate TF-IDF & BM25 scores separately because we might want to exclude other ngrams later

DECLARE REGEX_REPLACE_CLEAN_MEDIA_LINKS STRING DEFAULT
    -- This is mostly meant to catch images and videos hosted by reddit
    r"(\!?\[)(\w{3,})(\]\()(\w{6,}|[\w\s\|</>+;@#\?\!'_,.:\-=&%]{7,})(\s+\"[\w\s\|</>+;@#\?\!'_,{}\(\).:\-]+\"\s*|\s+'[\w\s\|</>+;@#\?\!\"_,{}\(\).:\-]+'\s*)?(\))";

DECLARE REGEX_STOPWORDS_TO_REMOVE STRING DEFAULT
    -- These are mostly English stop words with some Spanish mixed in.
    --  For other languages, we'll need to filter after the fact
    r"(?i)\bthe[ymn]?[\s:;,!\.\?]|\bto\s|\bi am\s|\bde la\s|\bEst[oae]?s?n?y?\b|\bpara\s|\ba? ?las?\s|\bIs\s|\bIt[\s,:;\?!]|\bIt[’']?s\s|\bA[st]\s|&nbsp;|&#x200B;|\bTh[ieo]se?[\s:;,!\.\?]|\bThat[’']?[s,!\.\?]?\s|\bFor[,!\.\?]?\s|\bOn\s|\bI[’'][md]\s|\bPorque\s|\bCuando\s|\bTodos?\s|\bWe[’']re\s|\bYou[’']?re?\s|\bI\s|\bAnd\s|\bYou\s|\bS?He\s|\bHers?\b|\bHi[ms]\s|\b[AW]e?re\s|\bWas\s|\bDo\s|\bDoes[,!\.\?]?\s|\ban?\s|\bBut\s|\bMy\s|\b[mB]e\s|\bT?Here[\s,\.:;\?!]|\bWith\s|\bCan[’']t\s|\bCan\s|\bG[eo]t\s|\bGotta\s|\bCould\b|\bWon[’']t\b|\btheir\b|\bThey[’']re\s|\bHave\b|\bHaven['’]t\s|\bHadn['’]t\s|\bHad\b|\bFrom\s|\bSuch\s|\bof\s|\sin\b|\bI?[’']ve\s|\bDoes\b|\bDo\s|\bDon['’]t\b|\bDoesn[’']t[\s,\.:;\?!]|\bor\s|\bWe\s|\bWill\s|\by[’']all\s|\sAll\b|\bAny\s|\bSome\s|\bNone\s|\bAnyone\s|\bSomeone\b|\bSomething\b|\banything\b|\beveryone\b|\bif\b|\bwould\b|\bso\b|\bnot?\b|\byes\s|\bBy\s|\bAuch\s|\b[’'][ds]\b|\b[’']re\b|\bI?[’']ll\b|\bI?[’']ve\b|\b&apos;s?\b";

DECLARE REGEX_REMOVE STRING DEFAULT
    -- URLs, UTMs, some punctuation
    r"(?i)\?[^vcid]\w*=[^\s\(\)]+|&\w+=[^\s\(\)]+|%3f\w+%3D[^\s\(\)]+|%3f\w+=[^\s\(\)]+|%26\w+%3D[^\s\(\)]+|[iv]\.redd\.it|reddit.com/gallery|reddit.com/[ru]\b|\bi\.|https?:?//?|www\.?|\.com/watch|\.mp4|\.jsp|&[a-z\\_%\d]+=[a-z\\_%\d\.\-_]+|\?[a-z\\_%\d]+=[a-z\\_%\d\.\-_]+|\.s?html?|\.com|\b[’']\b|¿|…\B|—\B|\.gif|.jpe?g|\.org|\.net|\.gg";

DECLARE REGEX_REPLACE_WITH_SPACE STRING DEFAULT
    r"(?i)\x60|&[a-z]{3,4};|[\-\/\.][a-z][\-\/\.]|[_/\\]+\d+[_/\\]|/search|/status/|/comments/|\b\d+\b|\sá\s|\sé\s|%[0-9a-f]{4}|%[0-9a-f]{2}|\b[wy]/o\b|\sdel?\s|\n&#x200B;|%\w{2}|[\^”–“·。;:%,\-=_\+\$\?\<\>’~#\\\/]+|\s?\| *:?-?:? *|[\)!\('\.\"\]\[\*\{\}]+|”|」|¬|\s&\s|\s@\s";


CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_comments_ngram_20211214`
AS (
WITH
preprocessed_text AS (
    -- Clean up the text before concat to prevent memory errors
    -- breaking it up doesn't work because we get errors with "cannot query rows larger than 100MB limit"
    SELECT
        p.subreddit_id
        , post_id
        , comment_id
        , p.subreddit_name
        -- Need to coalesce in case the regexes return an empty string
        , COALESCE(TRIM(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                comment_body_text, REGEX_REPLACE_CLEAN_MEDIA_LINKS, r"\5 " -- keep only the description
                            ), REGEX_STOPWORDS_TO_REMOVE, ''
                        ), REGEX_REMOVE, ''
                    ), REGEX_REPLACE_WITH_SPACE, ' ' -- replace common items with space
                ), r"\s{2,}|\n *\n *\n*|\n", " " -- remove extra spaces
              )
        ), '.') AS clean_text

    FROM `reddit-employee-datasets.david_bermejo.subclu_comments_top_no_geo_20211214` AS p
        INNER JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` AS sc
            ON p.subreddit_id = sc.subreddit_id
    WHERE 1=1
        -- TESTING: filter by sub name
        -- AND p.subreddit_name IN (
        --     'formula1'
        --     , 'me_irl', 'china_irl'
        --     , 'newsg', 'ich_iel'
        --     , 'askreddit', 'fragreddit'
        --     , 'legaladvice', 'fatfire'
        --     , 'newparents', 'medicine'
        --     , 'netherlands', 'london'
        --     , 'lgbt'
        --     , 'cooking'
        --     , 'fuckcars', 'cars', 'cycling'
        --     , 'ucla', 'maliciouscompliance'
        --     , 'writing', 'relationship_advice', 'fitness'
        --     , 'wallstreetbets', 'ethereum'
        --     , 'foofighters', 'edm'
        --     , 'torontoraptors', 'baseball', 'nhl', 'nba', 'soccer', 'nfl', 'mma'
        --     , 'de', 'mexico', 'france', 'argentina', 'india', 'memexico'
        --     , 'explainlikeimfive', 'space', 'pics', 'economy'
        --     , 'worldnews', 'todayilearned'
        --     , 'skyrim', 'breath_of_the_wild', 'gaming', 'steam'
        -- )
)
, ngram_per_subreddit_raw AS (
        -- how many times each word is mentioned in a cluster
        SELECT
            subreddit_id
            , subreddit_name
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
            AND COALESCE(TRIM(ngram), '') NOT IN(
                ''
                -- Common English tokens are now mostly in the REGEX to clean the text
                , 'she', 'youtu', 'the', 'not', 'my', 'by', 'you'
                , 't', 'r'
            )
        GROUP BY subreddit_id, subreddit_name, TRIM(ngram)
    )
    , ngram_per_subreddit AS (
        SELECT
            *
            , (1 + array_length(regexp_extract_all(ngram, ' '))) AS ngram_type
            , CHAR_LENGTH(ngram) AS ngram_char_len
        FROM ngram_per_subreddit_raw
    )


-- Select n-grams to calculate tf-idf & BM25
SELECT
    n.subreddit_id
    , n.subreddit_name
    , n.* EXCEPT(subreddit_id, subreddit_name)
FROM ngram_per_subreddit AS n

WHERE ngram_count >= 3

ORDER BY subreddit_name, ngram_count DESC

);  -- close CREATE table parens
