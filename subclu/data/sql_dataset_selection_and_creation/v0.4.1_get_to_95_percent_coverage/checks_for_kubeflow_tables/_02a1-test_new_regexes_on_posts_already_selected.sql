-- Test new functions to parse post-URL
-- 3. Post URL for embedding (raw path, but no search params)
-- 1. Post URL domain (only the first part of raw path)
-- 2. Post URL path for text (the path w/o IDs that we can add to post_combined_text)

-- Process URL
--   URL domain. This needs to also capture subreddit name for cross-posts that start with /r/subreddit_name/
DECLARE REGEX_GET_URL_DOMAIN STRING DEFAULT r"(?i)^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.|i\.)?(?:/r/)?([^:\/?\n]+)";

--  URL path raw
DECLARE REGEX_GET_URL_PATH_AND_PARAMS STRING DEFAULT r"(?i)^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.|i\.)?(?:/r/)?(?:[^:\/?\n]+/)(.+)";
-- Add %3F b/c it's encoded as `?`
DECLARE REGEX_REMOVE_URL_PARAMS STRING DEFAULT r"(?i)\?[^vcid]\w*=.+|&\w+=.+|#\w*|%3f\w+%3D.+|%3f\w+=.+|%26\w+%3D.+";
DECLARE REGEX_URL_PATH_REPLACE_WITH_SPACE1 STRING DEFAULT r"(?i)%[0-9a-f]{4}|%[0-9a-f]{2}";

--  Clean path text to concat as part of post
DECLARE REGEX_URL_PATH_CLEAN_REMOVE_IDS STRING DEFAULT
  r"(?i)https?|:\/\/|www\.|index\.\w{3,4}|\.\w{3,4}$|\b\d?\.?\d{5,}_?\b|\!|\s\d{6,}|_\d{6,}|\b_?\d{3}\b|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|-\d[a-z0-9]{8,}|_?-?[0-9a-f]{24}\b|\b\.?[a-z0-9]{25,}\b|\b[a-z0-9]{20,}_[a-z0-9]{20,}\b|\b[a-z]+\d+[a-z]+\b|\b\d+[a-z]+\d+[a-z]+\d*[a-z]*\b";
DECLARE REGEX_URL_PATH_CLEAN_REPLACE_WITH_SPACE2 STRING DEFAULT
  r"-|_|\+|=|\?";


-- POST title & body
DECLARE REGEX_REPLACE_INIT_PARENS_WITH_COMMA STRING DEFAULT
    r"(^\(|^\[)(\w+\s*[\|</>+;@#'_\",.:\-]*\s*\w+\s*[\|</>+;@#'_\",.:\-]*\s*\w*)(\)|\])";
DECLARE REGEX_REPLACE_CLEAN_MEDIA_LINKS STRING DEFAULT
    r"(\!\[)(\w{3,6})(\]\()(\w{6,}|[\w\s\|</>+;@#\?\!'_,.:\-]{7,80})(\s+\"[\w\s\|</>+;@#\?\!'_,{}\(\).:\-]+\"\s*|\s+'[\w\s\|</>+;@#\?\!\"_,{}\(\).:\-]+'\s*)?(\))";

DECLARE REGEX_POST_REMOVE_1ST STRING DEFAULT
    r"(?i)https?://|\w{1,3}\.reddit|redd.it|\.gg|goo.gl|bit.ly|search\?\w+=|www\.|\br/|/r/|\.html?|\.com|\.org|/index\.\w{3,4}|\n-{2,}|\< ";
DECLARE REGEX_REPLACE_WITH_SPACE_STR STRING DEFAULT
    r"(?i)/?wiki/|#wiki_|\b/\w+\?\w+=|&\w{1,}=|\?\w+=|\)\|?\[|\]\|?\(| *\| *|&nbsp;| {2,}|flair%3A|%3A|%2B|%\w{2}|/+|-+|_|%20|\n&#x200B;|\n +\n";
DECLARE REGEX_REMOVE_2ND_PASS_STR STRING DEFAULT
    r"\|?:?-+:?\|{1,}|\(|\)|\!?\[|\]|\>|\^| +\| +|: +:\||\|{2,}|\n\|+ {0,9}\|{0,9}|\n ?: +:|\x60|~|={2,}|:{2,}|#|\\|\*{2,}";


WITH
    posts_lang_and_meta_top AS (
        SELECT * EXCEPT(
          post_url_path_raw
          , post_url_domain
          , post_url_to_concat
          , post_url_for_standalone_embedding
          , post_title_and_body_text_clean
          , post_title_and_body_text_clean_word_count
          , subreddit_name
        )
      FROM `reddit-relevance.tmp.subclu_posts_for_modeling_20220430`
    )
    , post_url_domain_and_raw_paths AS (
        -- Apply filters to only get domain URLs for some posts and prevent having to run regexes on all
        --   URLs (we don't use all of them for the embeddings)
        SELECT
            post_id
            , post_url_domain
            , CASE
                WHEN post_url_domain IS NULL THEN NULL
                ELSE REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_EXTRACT(post_url, REGEX_GET_URL_PATH_AND_PARAMS),
                                REGEX_REMOVE_URL_PARAMS, ""
                            ), r"\/_\/|\?\w=|\/", ' ' -- replace slashes with spaces
                        ), REGEX_URL_PATH_REPLACE_WITH_SPACE1, ' '
                    ), r' {2,}', ' '
                )
            END AS post_url_path_raw
        FROM (
            SELECT
                post_id
                , post_url
                , CASE
                    WHEN post_url IS NULL THEN NULL

                    -- Exclude images & videos hosted at Reddit (these are all UUIDs w/o semantic meaning)
                    WHEN STARTS_WITH(post_url, 'https://i.redd.it') THEN NULL
                    WHEN STARTS_WITH(post_url, 'https://v.redd.it') THEN NULL

                    -- Exclude URL if it's the same as the post (if the post ID is in the post_url)
                    --  This would be repeating/leaking subreddit name information that we'll add separately
                    WHEN REGEXP_INSTR(
                        post_url,
                        ARRAY_REVERSE(SPLIT(post_id, "_"))[SAFE_OFFSET(0)]
                        ) > 0 THEN NULL

                    -- Get the domain from the post URL
                    ELSE TRIM(
                        REGEXP_EXTRACT(post_url, REGEX_GET_URL_DOMAIN)
                    )
                END AS post_url_domain

            FROM posts_lang_and_meta_top
        )
    )
    , clean_post_urls AS (
        SELECT
            post_id
            , post_url_domain
            , post_url_path_raw -- TODO(djb) remove this raw col from final table once regexes are complete

            -- In the simple case we keep everything to create an embedding
            --  the caveat is that this embedding might be useless b/c it'll include meaningless IDs & UUIDs
            , CASE
                WHEN post_url_domain IS NULL THEN NULL
                ELSE TRIM(
                    CONCAT(
                        post_url_domain, ' ',
                        COALESCE(
                          REGEXP_REPLACE(
                            REGEXP_REPLACE(post_url_path_raw, r"-", " ")
                            , r" {2,}", " "
                          )
                          , ''
                        )
                    )
                )
            END AS post_url_for_standalone_embedding

            -- In the more complex case, we'll filter out UUIDs and other info so that the text we'll concat with
            --  body text is more meaningful
            , CASE
                WHEN post_url_domain IS NULL THEN NULL

                -- Domains where paths don't include semantic paths (only IDs)
                WHEN post_url_domain IN (
                    'gfycat.com'
                    , 'youtube.com', 'youtu.be', 'm.youtube.com'
                    , 'imgur.com', 'i.imgur.com', 'm.imgur.com'
                    , 'redgifs.com'
                    , 'discord.gg'
                    , 'open.spotify.com'
                    , 'playoutube.com '
                    , 'clips.twitch.tv'
                    , 'joinfambase.com'
                    , 'instagram.com'
                ) THEN post_url_domain

                -- Domains that usually have names and page titles in URL, even if they're short
                WHEN post_url_domain IN (
                    'twitch.tv', 'theguardian.com', 'nytimes.com'
                    , 'twitter.com'
                ) THEN TRIM(
                    CONCAT(
                        post_url_domain, ' ',
                        COALESCE(post_url_path_to_concat_text, '')
                    )
                )

                -- Add path data if it's likely to have words (usually 2 or fewer "words" => only IDs in path)
                WHEN post_url_path_to_concat_word_count >= 3 THEN TRIM(
                    CONCAT(
                        post_url_domain, ' ',
                        COALESCE(post_url_path_to_concat_text, '')
                    )
                )

                -- If path doesn't meet criteria, only pass the domain
                ELSE post_url_domain

            END AS post_url_to_concat

          , post_url_path_to_concat_word_count
          , post_url_path_to_concat_text

        -- Use subqueries to prevent having a bunch more CTEs & prevent having to compute REGEX over URLs we won't use
        FROM (
            SELECT
                -- subquery to count the words in the text to concat, we use this len to determine whether to concat the path
                *
                , (
                  1 +
                  ARRAY_LENGTH(
                    REGEXP_EXTRACT_ALL(TRIM(COALESCE(post_url_path_to_concat_text, ''))," ")
                  )
                ) AS post_url_path_to_concat_word_count

            FROM (
              SELECT
                  -- Subquery to get the text we can concat
                  *

                  -- Remove most IDs (UUIDs, base36, etc)
                  , REGEXP_REPLACE(
                      REGEXP_REPLACE(
                          REGEXP_REPLACE(post_url_path_raw, REGEX_URL_PATH_CLEAN_REMOVE_IDS, ''),
                          REGEX_URL_PATH_CLEAN_REPLACE_WITH_SPACE2, ' '
                      ), r' {2,}', ' '  -- remove dupe spaces to save overhead & b/c we use it to count words
                  ) AS post_url_path_to_concat_text

              FROM post_url_domain_and_raw_paths
            )
        )
    )
    , posts_final_clean_top AS (
        -- This is the final, de-duped table used for modeling
        SELECT
            * EXCEPT(
                post_title_and_body_text
                , flair_text
                , post_title_and_body_text_clean
                , ocr_inferred_text_agg_clean
            )

            , (post_title_and_body_text_clean = post_title_and_body_text) AS post_title_and_body_text_raw_same_as_clean
            , CHAR_LENGTH(post_title_and_body_text_clean) AS post_title_and_body_text_clean_len
            , array_length(regexp_extract_all(post_title_and_body_text_clean, r"\b\w+\b")) post_title_and_body_text_clean_word_count
            , flair_text -- TODO(djb): Do I clean flair text?
            , post_title_and_body_text
            , post_title_and_body_text_clean
            , TRIM(
                    CONCAT(
                        COALESCE(flair_text, '')
                        , "\n", COALESCE(post_title_and_body_text_clean, '')
                        , "\n", COALESCE(ocr_inferred_text_agg_clean, '')
                        , "\n", COALESCE(post_url_to_concat, '')
                    )
            ) AS post_flair_title_body_ocr_url_text_clean
            , ocr_inferred_text_agg_clean

        FROM (
            SELECT
                pl.* EXCEPT(post_url)
                , CHAR_LENGTH(ocr_inferred_text_agg_clean) AS ocr_text_len
                , array_length(regexp_extract_all(ocr_inferred_text_agg_clean, r"\b\w+\b")) ocr_text_word_count

                , post_url
                -- URL cols from new table:
                , post_url_path_raw -- TODO(djb): Remove col after testing
                , post_url_domain
                , post_url_path_to_concat_word_count
                , post_url_to_concat
                , post_url_for_standalone_embedding

                -- apply text preprocessing for POST text
                , TRIM(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        REGEXP_REPLACE(
                                            REGEXP_REPLACE(post_title_and_body_text, REGEX_REPLACE_INIT_PARENS_WITH_COMMA, r'\2,')
                                            , REGEX_REPLACE_CLEAN_MEDIA_LINKS, r"\2 \5"
                                        )
                                        , REGEX_POST_REMOVE_1ST, ""),
                                    REGEX_REPLACE_WITH_SPACE_STR, " "
                                ),
                                REGEX_REMOVE_2ND_PASS_STR, ""
                            ), " {2,}", " " -- Remove multiple spaces next to each other
                        ), r"\n\s*\n\s*\n+", "\n\n"  -- replace repeated newlines
                    )
                ) AS post_title_and_body_text_clean

            FROM posts_lang_and_meta_top AS pl
                LEFT JOIN clean_post_urls AS pu
                    ON pl.post_id = pu.post_id
        )
    )


SELECT
  sel.post_id
  -- , sel.post_url
  -- , cpu.* EXCEPT(post_url_to_concat)
  , subreddit_name
  , sel.post_type
  , sel.flair_text
  , cpu.post_url_to_concat
  , cpu.post_title_and_body_text_clean
  , sel.post_title_and_body_text

FROM posts_final_clean_top AS cpu
  LEFT JOIN `reddit-relevance.tmp.subclu_posts_for_modeling_20220430` AS sel
    ON cpu.post_id = sel.post_id

WHERE 1=1
    -- AND cpu.post_url_domain IS NOT NULL
    -- AND sel.post_title_and_body_text_raw_same_as_clean = false
    -- AND cpu.post_title_and_body_text_clean_word_count >= 9

    -- use to check cleaning up markdown links
    AND REGEXP_CONTAINS(sel.post_title_and_body_text, r'\!\[')

  -- AND post_url_domain_new IN (
  --   'streamable.com', 'soundcloud.com', 'pbs.twimg.com', 'vm.tiktok.com', 'instagram.com', 'm.youtube.com', 'twitch.tv'
  -- )
  -- AND LENGTH(post_url_domain) <= 7
  -- AND weighted_language != 'en'

LIMIT 4000
;
