-- Create test table to count ngrams once and then reuse them as a separate step

DECLARE REGEX_REPLACE_CLEAN_MEDIA_LINKS STRING DEFAULT
    -- This is mostly meant to catch images and videos hosted by reddit
    r"(\!?\[)(\w{3,})(\]\()(\w{6,}|[\w\s\|</>+;@#\?\!'_,.:\-=&%]{7,})(\s+\"[\w\s\|</>+;@#\?\!'_,{}\(\).:\-]+\"\s*|\s+'[\w\s\|</>+;@#\?\!\"_,{}\(\).:\-]+'\s*)?(\))";

DECLARE REGEX_FLAIR_REPLACE_WITH_SPACE STRING DEFAULT
    r":post[\-_]|:snoo[\-_]";

DECLARE REGEX_STOPWORDS_TO_REMOVE STRING DEFAULT
    -- These are mostly English stop words with some Spanish mixed in.
    --  For other languages, we'll need to filter after the fact
    r"(?i)\bthe[ymn]?[\s:;,!\.\?]|\bto\s|\bi am\s|\bde la\s|\bEst[oae]?s?n?y?\b|\bpara\s|\ba? ?las?\s|\bIs\s|\bIt[\s,:;\?!]|\bIt[’']?s\s|\bA[st]\s|&nbsp;|&#x200B;|\bTh[ieo]se?[\s:;,!\.\?]|\bThat[’']?[s,!\.\?]?\s|\bFor[,!\.\?]?\s|\bOn\s|\bI[’'][md]\s|\bPorque\s|\bCuando\s|\bTodos?\s|\bWe[’']re\s|\bYou[’']?re?\s|\bI\s|\bAnd\s|\bYou\s|\bS?He\s|\bHers?\b|\bHi[ms]\s|\b[AW]e?re\s|\bWas\s|\bDo\s|\bDoes[,!\.\?]?\s|\ban?\s|\bBut\s|\bMy\s|\b[mB]e\s|\bT?Here[\s,\.:;\?!]|\bWith\s|\bCan[’']t\s|\bCan\s|\bG[eo]t\s|\bGotta\s|\bCould\b|\bWon[’']t\b|\btheir\b|\bThey[’']re\s|\bHave\b|\bHaven['’]t\s|\bHadn['’]t\s|\bHad\b|\bFrom\s|\bSuch\s|\bof\s|\sin\b|\bI?[’']ve\s|\bDoes\b|\bDo\s|\bDon['’]t\b|\bDoesn[’']t[\s,\.:;\?!]|\bor\s|\bWe\s|\bWill\s|\by[’']all\s|\sAll\b|\bAny\s|\bSome\s|\bNone\s|\bAnyone\s|\bSomeone\b|\bSomething\b|\banything\b|\beveryone\b|\bif\b|\bwould\b|\bso\b|\bnot?\b|\byes\s|\bBy\s|\bAuch\s|\b[’'][ds]\b|\b[’']re\b|\bI?[’']ll\b|\bI?[’']ve\b|\b&apos;s?\b";

DECLARE REGEX_REMOVE_FROM_OCR STRING DEFAULT
    -- OCR can introduce noise from repeated characters that we need to remove. Be more aggressive & remove anything too short
    r"(?i)\.[a-z]+\b|\b[_/\-\\’']?[a-z]{1,2}[_/\-\\]?\b|\bimg_|\b[dw]on'?t?\b|\bcom\b|\bnet\b|\bget\b|\blets?\b|\b[il]{2,}\b";

DECLARE REGEX_REMOVE STRING DEFAULT
    -- URLs, UTMs, some punctuation
    r"(?i)\?[^vcid]\w*=[^\s\(\)]+|&\w+=[^\s\(\)]+|%3f\w+%3D[^\s\(\)]+|%3f\w+=[^\s\(\)]+|%26\w+%3D[^\s\(\)]+|[iv]\.redd\.it|reddit.com/gallery|reddit.com/[ru]\b|\bi\.|https?:?//?|www\.?|\.com/watch|\.mp4|\.jsp|&[a-z\\_%\d]+=[a-z\\_%\d\.\-_]+|\?[a-z\\_%\d]+=[a-z\\_%\d\.\-_]+|\.s?html?|\.com|\b[’']\b|¿|…\B|—\B|\.gif|.jpe?g|\.org|\.net|\.gg";

DECLARE REGEX_REPLACE_WITH_SPACE STRING DEFAULT
    r"(?i)&[a-z]{3,4};|[\-\/\.][a-z][\-\/\.]|[_/\\]+\d+[_/\\]|/search|/status/|/comments/|\b\d+\b|\sá\s|\sé\s|%[0-9a-f]{4}|%[0-9a-f]{2}|\b[wy]/o\b|\sdel?\s|\n&#x200B;|%\w{2}|[\^”–“·。;:%,\-=_\+\$\?\<\>’~#\\\/]+|\s?\| *:?-?:? *|[\)!\('\.\"\]\[\*\{\}]+|”|」|¬|\s&\s|\s@\s";


CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subreddit_text_test_20211215`
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
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    CONCAT(
                                        COALESCE(REGEXP_REPLACE(flair_text, REGEX_FLAIR_REPLACE_WITH_SPACE, ' '), '')
                                        , ' ', COALESCE(text, '')
                                        -- Only extract URL's from non-reddit websites b/c reddit URL's mess with word clean up
                                        , ' ', IF(post_url_for_embeddings IS NULL, '', post_url)
                                        , ' ', COALESCE(REGEXP_REPLACE(ocr_inferred_text_agg_clean, REGEX_REMOVE_FROM_OCR, ''), '')
                                    ), REGEX_REPLACE_CLEAN_MEDIA_LINKS, r"\5 " -- keep only the description
                                ), REGEX_STOPWORDS_TO_REMOVE, ''
                            ), REGEX_REMOVE, ''
                        ), REGEX_REPLACE_WITH_SPACE, ' ' -- replace common items with space
                    ), r"\s{2,}|\n *\n *\n*|\n", " " -- remove extra spaces
                  )
            ), '.') AS clean_text

        FROM `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20211214` AS p
            INNER JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` AS sc
                ON p.subreddit_id = sc.subreddit_id
        WHERE 1=1
            -- filter by sub name
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
            -- filter by label
            -- AND sc.k_0085_label IN (
            --     30, 40, 50
                -- ,  60, 70, 80, 84, 85
                -- , 11, 22, 33, 44, 55, 66, 77
                -- , 1, 2, 10, 20
            -- )
    )


-- Check clean text regexes
SELECT
    t.subreddit_name
    , t.flair_text
    , t.text
    , p.*
    , t.post_url
    , t.ocr_inferred_text_agg_clean
FROM preprocessed_text AS p
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20211214` AS t
        ON p.post_id = t.post_id

);  -- close CREATE table parens
