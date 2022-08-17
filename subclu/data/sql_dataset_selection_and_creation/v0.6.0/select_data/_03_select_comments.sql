-- Select COMMENTS for the subreddits we've already selected so that we can combine
--  posts + coomments to create topic models (instead of only posts)

-- Turns out that the language_detect_v2 table doesn't have unique posts/comments
--    so we have to create an intermediary table to remove duplicates

-- Update checklist:
-- * start date
-- * end date
-- * min comment len
-- * max comments per post
-- * name of new created table (update date)
-- * table with latest selected posts (e.g., subclu_posts_top_no_geo_20211214)
-- * name of newly created table for exporting
-- * new GCS folder for new table

DECLARE END_DATE DATE DEFAULT ${end_date};
DECLARE START_DATE DATE DEFAULT END_DATE - ${post_lookback_days};
-- smaller vals for testing
-- DECLARE END_DATE DATE DEFAULT CURRENT_DATE() - 2;
-- DECLARE START_DATE DATE DEFAULT END_DATE - 7;


DECLARE MIN_COMMENT_LEN NUMERIC DEFAULT 11;
DECLARE MAX_COMMENTS_PER_POST NUMERIC DEFAULT 8;

-- POST title & body
DECLARE REGEX_REPLACE_CLEAN_MEDIA_LINKS STRING DEFAULT
    r"(\!\[)(\w{3,6})(\]\()(\w{6,}|[\w\s\|</>+;@#\?\!'_,.:\-]{7,80})(\s+\"[\w\s\|</>+;@#\?\!'_,{}\(\).:\-]+\"\s*|\s+'[\w\s\|</>+;@#\?\!\"_,{}\(\).:\-]+'\s*)?(\))";

DECLARE REGEX_POST_REMOVE_1ST STRING DEFAULT
    r"(?i)https?://|\w{1,3}\.reddit|redd.it|\.gg|goo.gl|bit.ly|search\?\w+=|www\.|\.html?|\.com|\.org|/index\.\w{3,4}|\n-{2,}|\< |!remindme \d+";
DECLARE REGEX_REPLACE_WITH_SPACE_STR STRING DEFAULT
    r"(?i)\br/|/r/|/?wiki/|#wiki_|\b/\w+\?\w+=|&\w{1,}=|\?\w+=|\)\|?\[|\]\|?\(| *\| *|&nbsp;|&#\w{3,5};| {2,}|\>!|![\<\\]|flair%3A|%3A|%2B|%\w{2}|/+|-+|_|%20|\n +\n|[\"']id[\"']:[\"'][a-z0-9]{9,}[\"']|,?[\"']\w[\"']:[\"']\w{0,3}[\"']|,?[\"']\w[\"']:|,?[\"']id[\"']:[\"'] ?[\"']|{[\"']document[\"']:[\{\[]|},{|{{|[}\]]}|[\"']link[\"'],|[\"']text[\"'],|[\[,]{";

-- There are ~250 points for braille patterns, this covers the whole range: [⠁-⣿]
--  In the Reddit context, it's most often used to make UNICODE-art (ascii-art)
DECLARE REGEX_REMOVE_2ND_PASS_STR STRING DEFAULT
    r"\|?:?-+:?\|{1,}|\(|\)|\!?\[|\]|\>|\^| +\| +|: +:\||\|{2,}|\n\|+ {0,9}\|{0,9}|\n ?: +:|\x60|~|={2,}|:{2,}|#|\\|\*{2,}|\b\d{5,}|\d+,\d+,\d+|[⠁-⣿]{6,}|[\.,\!\?]{4,}|\n\.+\s*\n|\n\d{2,}\s*\n\d{2,}|•";


CREATE OR REPLACE TABLE `reddit-relevance.${dataset}.subclu_comments_for_modeling_${run_id}`
AS (

WITH
    selected_posts AS (
        -- Start with selected posts to reduce orphan comments
        SELECT
            subreddit_id
            , post_id
            , subreddit_name

        FROM `reddit-relevance.${dataset}.subclu_posts_for_modeling_${run_id}`
        -- Use where-clause for testing
        -- WHERE 1=1
        --     AND (
        --         geo_relevant_subreddit_all = TRUE
        --     )
    )
    , comments_not_removed AS (
        -- also exclude comments from some known bots + start limiting by length
        SELECT
            -- Keys & IDS
            gs.subreddit_name
            , sp.subreddit_id
            , sp.post_id
            , sp.comment_id
            , sp.user_id

            -- Meta content
            , sp.submit_date
            , sp.endpoint_timestamp
            , sp.noun
            , sp.removed
            , sp.upvotes
            , sp.app_name
            , sp.post_nsfw
            , sp.geo_country_code

            -- Text
            , sp.comment_body_text
            , CHAR_LENGTH(comment_body_text) AS comment_body_text_len

            -- Use row_num to remove dupes
            , ROW_NUMBER() OVER (
                PARTITION BY sp.comment_id
                ORDER BY sp.endpoint_timestamp DESC, removal_timestamp DESC
            ) AS row_num_comment_dupes

        -- Start with selected posts to reduce orphan comments
        FROM selected_posts AS gs
            LEFT JOIN `data-prod-165221.cnc.successful_comments` AS sp
                ON gs.subreddit_id = sp.subreddit_id
                    AND gs.subreddit_name = sp.subreddit_name
                    AND gs.post_id = sp.post_id

        WHERE sp.dt BETWEEN start_date AND end_date
            AND sp.removed = 0
            -- Keep only subs above the min length threshold to reduce computation overhead for text-preprocessing
            AND CHAR_LENGTH(TRIM(comment_body_text)) >= MIN_COMMENT_LEN

            -- Exclude some known bots that add noise
            AND sp.user_id NOT IN (
                "t2_4kh8rj3k"

                -- ModBots. e.g., asking for upvote & downvote + list of rules/links
                , 't2_7jj8uhqq'
                , 't2_2717bc9o'  -- Reasons why a post was removed
                , 't2_39hp5rq2'
                , 't2_hi92lspq'
                , 't2_hvku6ntu'  -- flair reminders

                -- saveVideo & similar bots
                , 't2_8gveco3a', 't2_9153zxld'
                , 't2_brd1xvt2'
                , 't2_1q5xnz7f'
                , 't2_6iso5iga'

                -- porn spam?
                , 't2_lmb8dru4', 't2_f7pr0m1g', 't2_n2s3oxjl'
                , 't2_1q5xnz7f'

                -- Spam bot detector
                , 't2_1qa7819l'

                -- grammar bots, exclaim bots, alphabetical order, etc.
                , 't2_92rt3uzb', 't2_ko79fwmf', 't2_3yl3wf07', 't2_co52o6va'
                , 't2_i4spg4l9', 't2_bmrlx9pm'

                -- Profanity check bot
                , 't2_4sk81sqg'

                -- Spotify tracks
                , 't2_1i0n633d'

                -- ranking bots
                , 't2_1z1g03sv'

                -- shadowban bots
                , 't2_2aprhayx', 't2_3afvt5vz'

                -- timer/remind me bots
                , 't2_3vultc71'

                -- bot converting celsius & farenheit
                , 't2_ekocoqou'
            )

            -- for some common comments, easier to list them and exclude than write regexes
            AND TRIM(LOWER(sp.comment_body_text)) NOT IN (
                'u/savevideo', '/u/savevideo', '/u_savevideo', '/u savevideo'
                , 'u/savevideobot', '/u/savevideobot'
                , 'this great!'
                , '!translated'
                , 'beautiful!!', 'beautiful !', 'beautiful  !'
                , 'beautiful x', 'beautiful :)'
                , 'be my guest'
                , 'i know that', 'i know that.', 'i know that!'
                , 'i have some', 'i have that', 'i have this'
                , 'i do indeed', 'i do it too', 'i do it lol'
                , 'i feel that', 'i do agree.'
                , 'i can do it'
                , 'i dm’ed you'
                , 'i bet it is'
                , 'i am indeed'
                , 'i always do'
                , "how’d it go", "how’d it go?"
                , 'how was it', 'how was it?'
                , 'hello there'
                , 'hehe thanks', 'haha thanks'
                , 'hey thanks!', 'hey thanks.'
                , 'got a link?'
                , 'good stuff.', 'good stuff!'
                , 'good point.', 'good point!', 'good points'
                , 'hard agree.', 'hard agree!'
                , 'back at ya.', 'back at ya!', 'back at you'
                , 'awesomeness', 'awesome bro'
                , 'autocorrect', 'astonishing'
                , 'appreciated', 'anytime man'
                , 'another one'
                , 'and so do i', 'and thanks!'
                , 'thank you:)', 'thank you :)'
            )
    )

    , most_frequent_commenters AS (
        -- Get IDs for users with most comments so we can filter them out
        -- usually either spammers or bots that don't add to topic understanding
        SELECT
            user_id
            , COUNT(DISTINCT comment_id) AS comment_count_total
        FROM comments_not_removed
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 1000
    )
    , comment_language AS (
        -- This table has duplicates. One reason is that people can edit their post
        --  We'll keep the most recent detected language.
        SELECT
            -- rank to remove dupes
            ROW_NUMBER() OVER(
                PARTITION BY thing_id, cl.user_id
                ORDER BY cl.created_timestamp DESC, cl.weighted_probability DESC
            ) AS post_thing_user_row_num
            , cl.subreddit_id
            , cl.post_id
            , cl.thing_id AS comment_id
            , cl.user_id
            , COALESCE(cl.weighted_language, "UNPROCESSED") AS weighted_language
            , cl.weighted_probability
            , text

        FROM comments_not_removed AS sel
            LEFT JOIN `data-prod-165221.language_detection.comment_language_detection_cld3` AS cl
                ON sel.subreddit_id = cl.subreddit_id
                    AND sel.post_id = cl.post_id
                    AND sel.comment_id = cl.thing_id
        WHERE DATE(cl._PARTITIONTIME) BETWEEN START_DATE AND END_DATE
    )

    -- TL = thing_language. In this case thing=comment
    , tl_with_meta AS (
        SELECT
            -- Keys/IDs to join
            sel.subreddit_name
            , sel.subreddit_id
            , sel.post_id
            , sel.comment_id
            , sel.user_id

            -- Metadata
            , sel.endpoint_timestamp
            , sel.submit_date
            , sel.removed
            , sel.upvotes
            , sel.app_name
            , sel.geo_country_code AS geolocation_country_code

            -- Language predictions
            , tl.weighted_language
            , tl.weighted_probability AS weighted_language_probability

            -- Text
            --  Start text pre-processing now that we removed duplicates
            , comment_body_text_len
            , sel.comment_body_text
            , TRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        COALESCE(sel.comment_body_text, text)
                                        , REGEX_REPLACE_CLEAN_MEDIA_LINKS, r"\2 \5"
                                    )
                                    , REGEX_POST_REMOVE_1ST, " "
                                )
                                , REGEX_REPLACE_WITH_SPACE_STR, " "
                            )
                            , REGEX_REMOVE_2ND_PASS_STR, " "
                        )
                        , " {2,}", " "  -- Remove multiple spaces next to each other
                    )
                    , r"\n\s*\n\s*\n+", "\n\n"  -- Replace repeated newlines
                )
            ) AS comment_text_clean

        -- Start with full geo data & append language data if it exists
        FROM (
            SELECT * FROM comments_not_removed
            WHERE row_num_comment_dupes = 1
        ) AS sel
            LEFT JOIN (
                SELECT *
                FROM comment_language
                WHERE post_thing_user_row_num = 1
            ) AS tl
                ON tl.subreddit_id = sel.subreddit_id
                    AND tl.post_id = sel.post_id
                    AND tl.user_id = sel.user_id
                    AND tl.comment_id = sel.comment_id

    )

    , comments_ranked AS (
        -- Rank comments so we only keep the top N comments per post
        SELECT
            tl.subreddit_id
            , tl.post_id
            , tl.comment_id
            , comment_text_clean_len

            , ROW_NUMBER() OVER (
                PARTITION BY tl.post_id
                ORDER BY tl.upvotes DESC, tl.comment_text_clean_len DESC, endpoint_timestamp ASC
            ) AS comment_rank_by_post_id

        FROM (
            SELECT
                *
                , CHAR_LENGTH(comment_text_clean) AS comment_text_clean_len
            FROM tl_with_meta
            -- Filter to keep only comments that are long enough
            WHERE CHAR_LENGTH(comment_text_clean) >= MIN_COMMENT_LEN
        ) AS tl
    )
    , selected_comments AS (
        -- Pick only comments that meet ALL ranking/filtering criteria
        SELECT
            subreddit_id
            , post_id
            , comment_id
            , comment_text_clean_len
            , comment_rank_by_post_id
        FROM comments_ranked AS cr
        -- filter out comments above threshold
        WHERE comment_rank_by_post_id <= MAX_COMMENTS_PER_POST
    )

    , tl_unique_with_meta_top_comments AS (
        SELECT
            tl.* EXCEPT (comment_body_text, comment_text_clean, comment_body_text_len)
            , comment_rank_by_post_id
            , array_length(regexp_extract_all(comment_text_clean, r"\b[\p{L}\w]+\b")) comment_text_clean_word_count
            , tl.comment_body_text_len
            , sc.comment_text_clean_len
            , comment_text_clean
            , comment_body_text

        FROM selected_comments AS sc
        INNER JOIN tl_with_meta AS tl
            ON sc.subreddit_id = tl.subreddit_id
                AND sc.post_id = tl.post_id
                AND sc.comment_id = tl.comment_id
    )

    -- This is the final table used for modeling
    --   Comment this section out if you want to preview with queries below
    SELECT * FROM  tl_unique_with_meta_top_comments
); -- close create table parens

