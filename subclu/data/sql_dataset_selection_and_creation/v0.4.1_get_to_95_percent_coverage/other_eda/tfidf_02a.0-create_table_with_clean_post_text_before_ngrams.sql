-- Create test table to count ngrams once and then reuse them as a separate step


DECLARE REGEX_REPLACE_CLEAN_MEDIA_LINKS STRING DEFAULT
    -- This is mostly meant to catch images and videos hosted by reddit
    r"(\!?\[)(\w{3,})(\]\()(\w{6,}|[\w\s\|</>+;@#\?\!'_,.:\-=&%]{7,})(\s+\"[\w\s\|</>+;@#\?\!'_,{}\(\).:\-]+\"\s*|\s+'[\w\s\|</>+;@#\?\!\"_,{}\(\).:\-]+'\s*)?(\))";

DECLARE REGEX_FLAIR_REPLACE_WITH_SPACE STRING DEFAULT
    r":post[\-_]|:snoo[\-_]";

DECLARE REGEX_STOPWORDS_TO_REMOVE STRING DEFAULT
    -- These are mostly English stop words with some Spanish mixed in.
    --  For other languages, we'll need to filter after the fact
    r"\b[tT]he[ymn]?\s|\bto\s|\b[Ii] am\b|\bde la\s|\b[^\.]de[^/]\s|\b[Ee]st[oae]?s?n?y?\b|\bpara\b|\ba? ?las?\s|\b[Ii]s\s|\b[Ii]t[\s,]|\b[Ii]t[’']?s\s|\ba[st]\s|\b[Dd]oes\b|&nbsp;|&#x200B;|\b[Tt]h[ieo]se?\s|\b[Tt]hat\s|\b[Ff]or\s|\b[Oo]n\s|\b[’'][ds]\b|\b[Ii][’'][md]\s|\b[Ii][’']ll\s|\b[Pp]orque\s|\b[Cc]uando\s|\b[Tt]odos?\s|\b[Ww]e[’']re\s|\b[Yy]ou[’']?re?\s|\b[Ii]\s|\b[Aa]nd\s|\b[Yy]ou\s|\b[Ss]?[Hh]e\s|\b[Hh]ers?\b|\b[Hh]i[ms]\s|\b[AaWw]e?re\s|\b[Ww]as\s|\b[Dd]o\s|\b[Dd]oes\s|\ban?\s|\b[Bb]ut\s|\b[Mm]y\s|\b[mBb]e\s|\b[Tt]?[Hh]ere[\.\s]|\b[Ww]ith\s|\b[Cc]an\s|\b[Cc]ould\s|\b[Tt]heir\s|\b[Hh]ave\s|\b[Hh]ad\s|\b[Ff]rom\s|\b[Ss]uch\s|\bof\s|\sin\b|\bI?[’']ve";

DECLARE REGEX_REMOVE_FROM_OCR STRING DEFAULT
    -- OCR can introduce noise from repeated characters that we need to remove
    r"(?i)\s[ms]+\s+[ms]+\b|\bx\sx\s?x?\b";

DECLARE REGEX_REMOVE STRING DEFAULT
    -- URL/UTM, S & M's from OCR, some punctuation
    r"(?i)[iv]\.redd\.it|\bi\.|https?:?//?|www\.?|\.com/watch|\.mp4|&[a-z\\_%\d]+=[a-z\\_%\d\.\-_]+|\?[a-z\\_%\d]+=[a-z\\_%\d\.\-_]+|\.s?html?|\.com|\ss+\s+s+\b|\sm\s+s\b|\s+m\s+m\b|\b[’']\b|¿|…\B|—\B|\.gif|.jpe?g|\.org";

DECLARE REGEX_REPLACE_WITH_SPACE STRING DEFAULT
    r"(?i)/search|/status/|&nbsp;|\bá\b|%[0-9a-f]{4}|%[0-9a-f]{2}|\n&#x200B;|%\w{2}|[\^”–“·。;:%,\-=_\+\$\?\<\>’~#\\\/]+|\s?\| *:?-?:? *|&amp;|[\)!\('\.\"\]\[\*\{\}]+|\b\d+\b|”|」|\s&\s|\s@\s";


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
