-- Select POSTS + detected language for topic modeling
-- Turns out that the language_detect_v2 table doesn't have unique posts/comments
--    so we have to create an intermediary table to remove duplicates
-- Update checklist:
-- * start date
-- * end date
-- * max posts per sub
-- * name of new created table (i.e., update date)
-- * table with latest selected subreddits

DECLARE MAX_POSTS_PER_SUB NUMERIC DEFAULT 8400;
DECLARE END_DATE DATE DEFAULT ${end_date};
DECLARE START_DATE DATE DEFAULT END_DATE - ${post_lookback_days};
-- Smaller vals for testing
-- DECLARE END_DATE DATE DEFAULT CURRENT_DATE() - 2;
-- DECLARE START_DATE DATE DEFAULT END_DATE - 4;


-- Regexes for OCR text
DECLARE REGEX_REPLACE_WITH_SPACE_OCR1 STRING DEFAULT
    r"(?i)\d+[-:,\.]\d+([-:,\.]\d{2,4}){0,1}|\d|[\+\#]|[ur]/|https?://|www\d?\.|\bcdn\.|<\\?\w{1,2}>|/r/|\.html|reddit|\.\w{3}\b|\s\w\s\w?\b|\s\w\s|^\w\s|\s\w$|\s[\!\.,]\s[\!\.,]*|\(|\)|\[|\]|\{|\}|\||\<|\>|/|\=|\~|[\.,\-\_%:;\+=\!‒]{2,}|\bi{2,}\b|\b''?l{2,}\b|\bj{2,}\b|\^|\bs{3,}\b|\bm{3,}\b|\bx{4,}\b|[fl]{4,}\b|[●←»«]|www\d?|&#?\w{3,5};|&nbsp\s";
--  sometimes, after removing some numbers we end up with stray letters (e.g., s=seconds, h=hours)
DECLARE REGEX_REPLACE_WITH_SPACE_OCR2 STRING DEFAULT
    r"(?i)\s\w\s\w?\b|\s\w\s|^\w\s|\s\w$";

-- Process URL
--   URL domain. This needs to also capture subreddit name for cross-posts that start with /r/subreddit_name/
DECLARE REGEX_GET_URL_DOMAIN STRING DEFAULT r"(?i)^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\d?\.|\bcdn\.|[mi]\.|mobile\.)?(?:/r/)?([^:\/?\n]+)";
DECLARE RX_REMOVE_FROM_COMMON_URLS STRING DEFAULT r"\.com$|\.tv$|\.gg$|\.co\b|^[mi]\.|^mobile\.|^v[mt]\.|^clips\.|^open\.|/clip/[\w-]+";

--  URL path raw
DECLARE REGEX_GET_URL_PATH_AND_PARAMS STRING DEFAULT r"(?i)^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\d?\.|i\.)?(?:/r/)?(?:[^:\/?\n]+/)(.+)";
-- Add %3F b/c it's encoded as `?`
DECLARE REGEX_REMOVE_URL_PARAMS STRING DEFAULT r"(?i)\?[^vcid]\w*=.+|&\w+=.+|#\w*|%3f\w+%3D.+|%3f\w+=.+|%26\w+%3D.+";
DECLARE REGEX_URL_PATH_REPLACE_WITH_SPACE1 STRING DEFAULT r"(?i)%[0-9a-f]{4}|%[0-9a-f]{2}";

-- Clean path text to concat as part of post
--  Also remove IDs from post body (replace with space)
DECLARE REGEX_URL_PATH_CLEAN_REMOVE_IDS STRING DEFAULT
  r"(?i)https?|:\/\/|www\d?\.|index\.\w{3,4}|\.\w{3,4}$|\b\d?\.?\d{5,}_?\b|\!{2,}|&\w{3,4};|&#\w{4,5};|\s\d{6,}|_\d{6,}|\b_?\d{3}\b|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[\-\"]\d[a-z0-9]{8,}|_?-?[0-9a-f]{24}\b|\b\.?[a-z0-9]{25,}\b|\b[a-z0-9]{20,}_[a-z0-9]{20,}\b|\b[a-z]+\d+[a-z]+\b|\b\d+[a-z]+\d+[a-z]+\d*[a-z]*\b";
DECLARE REGEX_URL_PATH_CLEAN_REPLACE_WITH_SPACE2 STRING DEFAULT
  r"-|_|\+|=|\?";

-- Test flair text pre-processing
DECLARE REGEX_REPLACE_WITH_SPACE_FLAIR STRING DEFAULT
    r"(?i)\"|\||\.com|:snoo[_\-]?|post[_\-]flair|post[_\-]|the[_\-]|[:_-]|\(|\)|\[|\]|\/|\^|\*|\\|&\w{3,4};";
DECLARE REGEX_REMOVE_2ND_PASS_FLAIR STRING DEFAULT
    r"- ";

-- POST title & body
DECLARE REGEX_REPLACE_INIT_PARENS_WITH_COMMA STRING DEFAULT
    r"(^\(|^\[)(\w+\s*[\|</>+;@#'_\",.:\-]*\s*\w+\s*[\|</>+;@#'_\",.:\-]*\s*\w*)(\)|\])";
DECLARE REGEX_REPLACE_CLEAN_MEDIA_LINKS STRING DEFAULT
    r"(\!\[)(\w{3,6})(\]\()(\w{6,}|[\w\s\|</>+;@#\?\!'_,.:\-]{7,80})(\s+\"[\w\s\|</>+;@#\?\!'_,{}\(\).:\-]+\"\s*|\s+'[\w\s\|</>+;@#\?\!\"_,{}\(\).:\-]+'\s*)?(\))";

DECLARE REGEX_POST_REMOVE_SOME_IDS STRING DEFAULT
  r"(?i)\/index\.\w{3,4}|&\w{3,4};|&#\w{4,5};|[_\-\"\/]\d[a-f0-9]{7,}\b|[_\-\"\/][0-9a-f]{24}\b|\b\.?[a-z0-9]{25,}\b|\b[a-z0-9]{20,}_[a-z0-9]{20,}\b|\b\d{5,}\b";

DECLARE REGEX_POST_REMOVE_1ST STRING DEFAULT
    r"(?i)https?://|\w{1,3}\.reddit|redd.it|\.gg|goo.gl|bit.ly|search\?\w+=|www\.|\.html?|\.com|\.org|/index\.\w{3,4}|\n-{2,}|\< ";
DECLARE REGEX_REPLACE_WITH_SPACE_STR STRING DEFAULT
    r"(?i)\br/|/r/|/?wiki/|#wiki_|\b/\w+\?\w+=|&\w{1,}=|\?\w+=|\)\|?\[|\]\|?\(| *\| *|&\w{3,4};|&nbsp\s|&nbsp$|#| {2,}|\>!|![\<\\]|flair%3A|%3A|%2B|%\w{2}|/+|-+|_|%20|\n&#x200B;|\n +\n|[\"'']id[\"'']:[\"''][a-z0-9]{9,}[\"'']|,?[\"'']\w[\"'']:[\"'']\w{0,3}[\"'']|,?[\"'']\w[\"'']:|,?[\"'']id[\"'']:[\"''] ?[\"'']|{[\"'']document[\"'']:[\{\[]|},{|{{|[}\]]}|[\"'']link[\"''],|[\"'']text[\"''],|[\[,]{";
DECLARE REGEX_REMOVE_2ND_PASS_STR STRING DEFAULT
    r"\|?:?-+:?\|{1,}|\(|\)|\!?\[|\]|\>|\^| +\| +|: +:\||\|{2,}|\n\|+ {0,9}\|{0,9}|\n ?: +:|\x60|~|={2,}|:{2,}|\\|\*{2,}|\b\d{5,}|\d+,\d+,\d+|[⠁-⣿]{3,}|[\.,\!\?]{4,}\b";

-- output if image is flagged as potential nudity or sexually explicit
DECLARE IMAGE_EXPLICIT_MODEL_STR STRING DEFAULT "Nudity or sex image.";


CREATE OR REPLACE TABLE `reddit-relevance.${dataset}.subclu_posts_for_modeling_${run_id}`

AS (

    WITH
    post_language AS (
        -- This table has duplicates for (at least) 2 reasons:
        --  * When OP comments on their post it triggers a 2nd "post" event
        --  * When someone makes a comment it can trigger a "post" event but with the user_id of the commenter (instead of OP)
        --  * Unclear if edits by OP create a new row

        -- In this CTE, we remove *some* duplicates by using row_number
        -- We get final UNIQUE post-ids by JOINING on: user_id (OP), post_id, and subreddit_id
        -- Example for 1 day:
        --  total rows  | row_num()=1 rows | unique post IDs
        --  9.4 million | 6.7 million      | 1.7 million

        SELECT
            -- Rank by post-ID + user_id + thing_type (one user can post AND comment)
            ROW_NUMBER() OVER(
                PARTITION BY post_id, user_id
                ORDER BY created_timestamp DESC, weighted_probability DESC
            ) AS post_thing_user_row_num
            , post_id
            , pl.subreddit_id
            , user_id
            , weighted_language
            , weighted_probability

        FROM `reddit-relevance.${dataset}.subclu_subreddits_for_modeling_${run_id}` AS sel
            LEFT JOIN `data-prod-165221.language_detection.post_language_detection_cld3` AS pl
                ON sel.subreddit_id = pl.subreddit_id
        WHERE DATE(_PARTITIONTIME) BETWEEN START_DATE AND END_DATE
            -- Only posts from seed subreddits (optional)
            -- AND COALESCE(sel.subreddit_seed_for_clusters, FALSE) = TRUE
        QUALIFY post_thing_user_row_num = 1
    )
    , posts_not_removed AS (
        SELECT
            -- Use row_number to get the latest edit as row=1
            ROW_NUMBER() OVER (
                PARTITION BY post_id
                ORDER BY endpoint_timestamp DESC, removal_timestamp DESC
            ) AS row_num
            -- Keep sub_name from subs_for_modeling table b/c it's less likely
            --  to have dupes in case the subreddit name changed in the last month
            , sel.subreddit_name
            , sp.subreddit_id
            , sp.post_id
            , sp.user_id  -- we need it to remove dupes in post-language

            -- Meta content
            , sp.submit_date
            , sp.endpoint_timestamp
            , sp.geo_country_code
            , sp.is_deleted
            , sp.removed

            , sp.upvotes
            , sp.comments
            , sp.successful
            , sp.app_name
            , sp.post_type
            , sp.post_url
            , sp.post_nsfw

            , sp.post_title
            , sp.post_body_text

        FROM `reddit-relevance.${dataset}.subclu_subreddits_for_modeling_${run_id}` AS sel
            LEFT JOIN `data-prod-165221.cnc.successful_posts` AS sp
                ON sel.subreddit_id = sp.subreddit_id

        WHERE sp.dt BETWEEN START_DATE AND END_DATE
            AND sp.removed = 0

            -- TODO(djb): Fix removed logic! A post can be removed, but then added back later by mods/admins!
            -- Only posts from seed subreddits (optional)
            -- AND COALESCE(sel.subreddit_seed_for_clusters, FALSE) = TRUE

        -- Remove dupes with row_num
        --   Example: We can get multiple rows when a post is removed or edited multiple times
        QUALIFY row_num = 1
    )
    , ranked_posts AS (
        -- Rank posts after removing some spam posts
        SELECT
            pn.*
            , plo.net_upvotes_lookup
            , ROW_NUMBER() OVER(
                PARTITION BY pn.subreddit_id
                ORDER BY net_upvotes_lookup DESC, pn.comments DESC, pn.upvotes DESC
            ) AS rank_post_in_sub
        FROM posts_not_removed AS pn
            INNER JOIN (
                SELECT
                    *
                    , (upvotes - downvotes) AS net_upvotes_lookup
                FROM `data-prod-165221.ds_v2_postgres_tables.post_lookup`
                WHERE DATE(_PARTITIONTIME) = end_date
            ) AS plo
                ON pn.subreddit_id = plo.subreddit_id AND pn.post_id = plo.post_id
                    AND pn.user_id = plo.author_id
        WHERE 1=1
            AND (
                -- Filter out spam posts
                COALESCE(plo.neutered, false) = false

                -- Keep posts that were flagged/neutered, but then approved
                OR (
                    COALESCE(plo.neutered, false) = true
                    AND COALESCE(plo.verdict, '') IN ('mod-approved', 'admin-approved')
                )
            )
        QUALIFY rank_post_in_sub <= MAX_POSTS_PER_SUB
    )
    , posts_lang_and_meta AS (
        SELECT
            sp.subreddit_id
            , sp.subreddit_name
            , sp.post_id
            , sp.user_id

            , sp.submit_date
            , sp.endpoint_timestamp
            , sp.geo_country_code
            , sp.is_deleted
            , sp.removed

            -- NOTE: in `post_lookup` some values are only updated w/in 24 hours of post creation
            , plo.neutered
            , plo.content_category
            , plo.upvotes    AS upvotes_lookup
            , plo.downvotes  AS downvotes_lookup
            --  Use post-lookup for net upvotes because it's more consistent, even if it doesn't always match the UI
            , sp.net_upvotes_lookup
            , sp.rank_post_in_sub

            , sp.upvotes AS upvotes
            , sp.comments
            , sp.successful
            , sp.app_name
            , sp.post_type
            , sp.post_url
            , sp.post_nsfw

            -- Language info
            , COALESCE(tl.weighted_language, 'UNPROCESSED') AS weighted_language
            , tl.weighted_probability AS weighted_language_probability
            , plo.language_preference AS post_language_preference

            -- Text
            -- Wait to do expensive string manipulation AFTER:
            --   - removing duplicates & keeping only the top posts per sub
            , plo.flair_text
            , CASE
                WHEN sp.post_body_text IS NULL THEN TRIM(COALESCE(sp.post_title), '.')
                ELSE TRIM(
                    CONCAT(COALESCE(sp.post_title, ''), "\n", COALESCE(sp.post_body_text, ''))
                )
            END AS post_title_and_body_text

        FROM ranked_posts AS sp
            LEFT JOIN post_language AS tl
                ON tl.subreddit_id = sp.subreddit_id
                    AND tl.post_id = sp.post_id
                    AND tl.user_id = sp.user_id

            INNER JOIN (
                SELECT * FROM `data-prod-165221.ds_v2_postgres_tables.post_lookup`
                WHERE DATE(_PARTITIONTIME) = end_date
            ) AS plo
                ON tl.subreddit_id = plo.subreddit_id AND tl.post_id = plo.post_id
                    AND tl.user_id = plo.author_id

        -- Test REGEXES keep only some subreddits
        -- WHERE 1=1
            -- AND subreddit_name IN (
            --     "redditsessions", "blursedimages", "hmmm", "hmm", "bollywood", "bollyblindsngossip", "bollyarm", "bollywoodmemes", "twitter", "eyebleach", "makenewfriendshere", "meetpeople", "berlinsocialclub", "nycmeetups", "news"
            --     , "antiwork", "damnthatsinteresting", "publicfreakout", "lifeprotips", "jedi", "jamesbond", "clonewars", "archerfx", "loveislandtv", "residentevil", "mortalkombat", "ukrainewarvideoreport", "sfx", "formula1", "gonewild", "minecraft", "china_irl", "lebanon", "hottiesoftvandyt", "mdma"
            --     , "de", "mexico", "france", "rance_iel", "relationship_advice", "gonewildstories", "sex", "nsfwiama", "lgbt", "askgaybros", "asexuality", "me_irlgbt", "blackladies", "asianamerican", "mixedrace", "askreddit", "nostupidquestions", "yahooqr", "tinder", "ama", "wallstreetbets"
            --     , "cryptocurrency", "soccer", "nba", "nfl", "fifa", "sports", "baseball", "fuckcars", "texas", "canada", "australia", "worldnews", "ukraine", "europe", "todayilearned", "india", "conspiracy", "space", "nasa", "explainlikeimfive", "eldenring", "apexlegends", "hiphopheads", "music", "listentothis", "halo"
            --     , "marvelstudios", "starwars", "movies", "deuxmoi", "aww", "interestingasfuck", "anime", "genshin_impact", "sweden", "denmark", "romania", "ufc", "mma", "kpop", "kpopde", "freekarma4u", "fitness", "trees", "cocktails", "vegan"
            --     , "cooking", "food", "amitheasshole", "showerthoughts", "memexico", "mujico", "gardening", "humansaremetal", "anormaldayinrussia"
            -- )
    )
    , ocr_text_agg AS (
        -- We need to agg the OCR text because one post could have multiple images
        --  DO NOT group by pt because it's possible for some images to be posted on one day and other images on the next day
        SELECT
            ocr.post_id
            , TRIM(STRING_AGG(TRIM(inferred_text), "\n" ORDER BY ocr.endpoint_timestamp ASC)) AS ocr_inferred_text_agg
            , TRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            STRING_AGG(TRIM(inferred_text), "\n" ORDER BY ocr.endpoint_timestamp ASC)
                            , REGEX_REPLACE_WITH_SPACE_OCR1, " "
                        )
                        , REGEX_REPLACE_WITH_SPACE_OCR2, " "
                    )
                    , r" {2,}", " " -- replace extra spaces with single space
                )
            )  AS ocr_inferred_text_agg_clean
            , COUNT(media_url) AS ocr_images_in_post_count

        FROM posts_lang_and_meta as pl
            LEFT JOIN `data-prod-165221.swat_tables.image_ocr_text` AS ocr
                ON pl.post_id = ocr.post_id

        WHERE DATE(ocr.pt) BETWEEN start_date AND end_date
            AND (
                COALESCE(inferred_text, '') != ''
                OR inferred_text IS NOT NULL
            )

        GROUP BY 1
    )
    , posts_lang_and_meta_top AS (
        -- Here we rank each post in the sub by upvotes & comments
        --   (might try screenviews later, but that wasn't trivial)
        -- ALSO add OCR text here
        SELECT
            pl.* EXCEPT(flair_text, post_title_and_body_text)
            , ocr_images_in_post_count
            , pl.flair_text
            -- Need to coalesce in case the regexes return an empty string
            , COALESCE(TRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(flair_text)
                            , REGEX_REPLACE_WITH_SPACE_FLAIR, ' '
                        ), REGEX_REMOVE_2ND_PASS_FLAIR, ' ' -- replace common items with space
                    ), r"\s{2,}", r" " -- remove extra spaces
                )
            ), NULL) AS flair_text_clean
            , pl.post_title_and_body_text
            , ocr_inferred_text_agg_clean
            , ocr_inferred_text_agg

        FROM posts_lang_and_meta AS pl
            -- There's something unexpected with votes in both `post_lookup` & `successful_posts`
            --  (usually votes are missing when compared to the UI)
            --  So use the combined metric to hedge our bets

            LEFT JOIN (
                SELECT * FROM ocr_text_agg
                WHERE COALESCE(ocr_inferred_text_agg_clean, "") != ""
            ) AS ocr
                ON pl.post_id = ocr.post_id

        WHERE 1=1
            AND pl.rank_post_in_sub <= MAX_POSTS_PER_SUB
    )
    , post_image_explicit_model_agg AS (
        -- 2022-11-02: Pull from FACT table instead of raw analytics_v2.events!
        --   When pulling frome events the query takes 20+ minutes or errors out b/c of resource overload

        -- Each image gets a rating and a post can have multiple images,
        --   so we'll need to aggregate output multiple images into a single field
        SELECT
            post_id
            -- If multiple images in post are flagged, keep only one instance of it
            , IF(
                SUM(sexually_expicit_image_pred) >= 1.0
                , IMAGE_EXPLICIT_MODEL_STR
                , NULL
            ) AS sexually_explicit_image_pred_text

        FROM (
            SELECT
                t.post_id
                , CASE
                    WHEN
                        COALESCE(
                            CAST(JSON_EXTRACT(ml_model_prediction_scores, "$.SexuallyExplicit[0]") AS FLOAT64)
                            , 0.0
                        ) >= 0.9
                        THEN 1
                    ELSE 0
                END AS sexually_expicit_image_pred
            FROM (
                SELECT
                    post_id
                    , ml_model_prediction_scores
                FROM `data-prod-165221.fact_tables.content_classification_record_response_image`

                WHERE DATE(pt) between START_DATE and END_DATE
                    AND source = "content_classification"
                    AND action = "record_response"
                    AND ml_model_name in ('safety_media_x_model', 'safety_media_x_video_model')
            ) AS a
            INNER JOIN posts_lang_and_meta_top AS t
                ON a.post_id = t.post_id

        ) AS sm

        GROUP BY 1

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

                -- youtube aliases
                WHEN post_url_domain IN (
                    'youtube.com', 'youtu.be', 'm.youtube.com'
                ) THEN 'youtube'

                -- For spotify we care about the type of item is is (e.g., track, album, episode, playlist, user)
                WHEN post_url_domain IN (
                    'spotify.com', 'spoti.fi', 'open.spotify.com'
                ) THEN TRIM(
                    CONCAT(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(post_url_domain, r"spoti\.fi", "spotify")
                            , RX_REMOVE_FROM_COMMON_URLS, ""
                        )
                        , ' '
                        , COALESCE(SPLIT(post_url_path_to_concat_text, " ")[SAFE_OFFSET(0)], '')
                    )
                )

                -- reddit link (not x-posts)
                --  Sometimes people link to subreddits or reddit posts (but not as a x-post)
                WHEN post_url_domain IN (
                    'reddit.com', 'old.reddit.com'
                ) THEN COALESCE(
                    TRIM(REGEXP_REPLACE(post_url_path_to_concat_text, r"^r | comments\b", ""))
                    , ''
                )

                -- Domains where paths DON'T include semantic paths (only IDs)
                WHEN post_url_domain IN (
                    'gfycat.com'
                    , 'imgur.com', 'i.imgur.com', 'm.imgur.com'
                    , 'redgifs.com'
                    , 'discord.gg'
                    , 'open.spotify.com'
                    , 'playoutube.com '
                    , 'clips.twitch.tv'
                    , 'joinfambase.com'
                    , 'instagram.com'
                    , 'vm.tiktok.com', 'vt.tiktok.com'
                ) THEN REGEXP_REPLACE(post_url_domain, RX_REMOVE_FROM_COMMON_URLS, "")

                -- Domains that usually have names and page titles in URL, even if they're short
                WHEN post_url_domain IN (
                    'twitch.tv', 'theguardian.com', 'nytimes.com'
                    , 'twitter.com', 'mobile.twitter.com'
                ) THEN TRIM(
                    CONCAT(
                        REGEXP_REPLACE(post_url_domain, RX_REMOVE_FROM_COMMON_URLS, "")
                        , ' '
                        , COALESCE(post_url_path_to_concat_text, '')
                    )
                )

                -- Add path data if it's likely to have words (usually 2 or fewer "words" => only IDs in path)
                WHEN post_url_path_to_concat_word_count >= 3 THEN TRIM(
                    CONCAT(
                        post_url_domain, ' ',
                        COALESCE(post_url_path_to_concat_text, '')
                    )
                )

                -- If path doesn't meet any criteria, only pass the domain
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
                , flair_text_clean
                , post_title_and_body_text_clean
                , ocr_inferred_text_agg_clean, ocr_inferred_text_agg
                , sexually_explicit_image_pred_text
            )

            , (post_title_and_body_text_clean = post_title_and_body_text) AS post_title_and_body_text_raw_same_as_clean
            , CHAR_LENGTH(post_title_and_body_text_clean) AS post_title_and_body_text_clean_len
            , array_length(regexp_extract_all(post_title_and_body_text_clean, r"\b\w+\b")) post_title_and_body_text_clean_word_count
            , flair_text
            , flair_text_clean
            , post_title_and_body_text
            , post_title_and_body_text_clean
            , sexually_explicit_image_pred_text
            , TRIM(
                    CONCAT(
                        COALESCE(flair_text_clean, '')
                        , IF(post_nsfw, '\nNSFW or porn.', '')
                        , COALESCE(CONCAT('\n', sexually_explicit_image_pred_text), '')
                        , COALESCE(CONCAT("\n", post_url_to_concat), '')
                        , "\n", IF(
                            CHAR_LENGTH(post_title_and_body_text_clean) >= 1010
                            , COALESCE(
                                CONCAT(LEFT(post_title_and_body_text_clean, 720), ".\n\n", RIGHT(post_title_and_body_text_clean, 290))
                                , ''
                            )
                            , COALESCE(post_title_and_body_text_clean, '')
                        )
                        -- OCR text can be looong, so cap it to ~80th percentile
                        , "\n", COALESCE(LEFT(ocr_inferred_text_agg_clean, 300), '')
                    )
            ) AS post_text_for_embeddings
            , ocr_inferred_text_agg_clean
            , ocr_inferred_text_agg

        FROM (
            SELECT
                pl.* EXCEPT(post_url)
                , CHAR_LENGTH(ocr_inferred_text_agg_clean) AS ocr_text_clean_len
                , array_length(regexp_extract_all(ocr_inferred_text_agg_clean, r"\b\w+\b")) ocr_text_word_count

                , post_url
                -- URL cols from new table:
                , post_url_domain
                , post_url_path_to_concat_word_count
                , post_url_to_concat
                , post_url_for_standalone_embedding
                , sexually_explicit_image_pred_text

                -- apply text preprocessing for POST text
                , TRIM(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        REGEXP_REPLACE(
                                            REGEXP_REPLACE(
                                                REGEXP_REPLACE(post_title_and_body_text, REGEX_REPLACE_INIT_PARENS_WITH_COMMA, r'\2, ')
                                                , REGEX_REPLACE_CLEAN_MEDIA_LINKS, r"\2 \5"
                                            )
                                            , REGEX_POST_REMOVE_SOME_IDS, " "
                                        )
                                        , REGEX_POST_REMOVE_1ST, ""
                                    )
                                    , REGEX_REPLACE_WITH_SPACE_STR, " "
                                )
                                , REGEX_REMOVE_2ND_PASS_STR, " "
                            )
                            , " {2,}", " "),  -- Remove multiple spaces next to each other
                        r"\n\s*\n\s*\n+", "\n\n"  -- Replace repeated newlines
                    )
                ) AS post_title_and_body_text_clean

            FROM posts_lang_and_meta_top AS pl
                LEFT JOIN clean_post_urls AS pu
                    ON pl.post_id = pu.post_id
                LEFT JOIN post_image_explicit_model_agg AS sm
                    ON pl.post_id = sm.post_id
        )
    )

-- Select for table creation
SELECT
    *
    , CHAR_LENGTH(post_text_for_embeddings) AS post_text_for_embeddings_len
    , END_DATE AS end_date
FROM posts_final_clean_top
ORDER BY subreddit_name, endpoint_timestamp
);  -- close CREATE TABLE parens


-- Scan size by CTE:
--     3.4 GB  post_language
--     3.1 GB  posts_not_removed
--   112.1 GB  posts_lang_and_meta
--    69.7 GB  ocr_text_agg
--   112.7 GB  posts_lang_and_meta_top
-- * 988.0 GB  post_image_explicit_model_agg ** big scan b/c it needs to check multiple models
--             but it's MUCH BETTER than 20+ TB when scanning events_v2.analytics
--    98.0 GB  post_url_domain_and_raw_paths
--    98.0 GB  clean_post_urls
--     1.2 TB  posts_final_clean_top
