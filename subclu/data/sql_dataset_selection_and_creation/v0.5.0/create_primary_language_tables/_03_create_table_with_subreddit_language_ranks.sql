-- For this view, exclude removed posts & comments
--  Sometimes they're removed because they're spam (language pred might be bad)
--  Sometimes posts are removed because they're in an unexpected language
--  So keeping removed posts might bias our primary language in a bad way

-- Get subreddit language using both posts and comments
-- shape is long, e.g., 1 row = 1 subreddit + language + type(comment/post)

CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subreddit_language_rank_20220808` AS (
    WITH comments_lang AS (
        SELECT
            co.subreddit_id
            , co.subreddit_name
            , co.post_id
            , co.comment_id
            , co.comment_text_length
            , co.weighted_language_code
            , co.weighted_language_name AS language_name

        FROM `reddit-employee-datasets.david_bermejo.comment_language_detection_cld3_clean` AS co
        WHERE comment_text_length >= 20
            AND COALESCE(removed, 0) = 0

    ),
    subreddit_lang_comments AS (
        SELECT
            subreddit_id
            , subreddit_name
            , language_name
            , 'comment' AS thing_type
            , STRING_AGG(DISTINCT(weighted_language_code), ',') AS language_codes

            , SUM(COUNT(comment_id)) OVER (PARTITION BY subreddit_id) AS total_count
            , COUNT(comment_id) AS language_count
            , ((0.0 + COUNT(comment_id)) / (SUM(COUNT(comment_id)) OVER (PARTITION BY subreddit_id))) as language_percent
        FROM comments_lang
        GROUP BY subreddit_id, subreddit_name, language_name, thing_type
    ),
    subreddit_lang_comments_rank AS (
        SELECT
            *
            , ROW_NUMBER() OVER (
                PARTITION BY subreddit_id
                ORDER BY language_percent DESC, language_name
            ) AS language_rank
        FROM subreddit_lang_comments
    ),
    posts_lang AS (
        SELECT
            p.subreddit_id
            , p.subreddit_name
            , p.post_id
            , p.post_title_and_body_text_length
            , p.weighted_language_code
            , p.weighted_language_name AS language_name

        FROM `reddit-employee-datasets.david_bermejo.post_language_detection_cld3_clean` AS p
        WHERE post_title_and_body_text_length >= 20
            AND COALESCE(removed, 0) = 0
    ),
    subreddit_lang_posts AS (
        SELECT
            subreddit_id
            , subreddit_name
            , language_name
            , 'post' AS thing_type
            , STRING_AGG(DISTINCT(weighted_language_code), ',') AS language_codes

            , SUM(COUNT(post_id)) OVER (PARTITION BY subreddit_id) AS total_count
            , COUNT(post_id) AS language_count
            , ((0.0 + COUNT(post_id)) / (SUM(COUNT(post_id)) OVER (PARTITION BY subreddit_id))) as language_percent
        FROM posts_lang
        GROUP BY subreddit_id, subreddit_name, language_name,  thing_type
    ),
    subreddit_lang_posts_rank AS (
        SELECT
            *
            , ROW_NUMBER() OVER (
                PARTITION BY subreddit_id
                ORDER BY language_percent DESC, language_name
            ) AS language_rank
        FROM subreddit_lang_posts
    ),
    -- Create new table that gets percent and rank for BOTH posts and comments
    subreddit_lang_things AS (
        SELECT
            COALESCE(pl.subreddit_id, cl.subreddit_id)          AS subreddit_id
            , COALESCE(pl.subreddit_name, cl.subreddit_name)    AS subreddit_name
            , COALESCE(pl.language_name, cl.language_name)      AS language_name
            , 'post_and_comment' AS thing_type
            , COALESCE(cl.language_codes, pl.language_codes) AS language_codes

            , SUM(COALESCE(pl.total_count, 0) + COALESCE(cl.total_count, 0)) AS total_count
            , SUM(COALESCE(pl.language_count, 0) + COALESCE(cl.language_count, 0)) AS language_count
        FROM subreddit_lang_posts AS pl
            FULL OUTER JOIN subreddit_lang_comments AS cl
                ON cl.subreddit_id = pl.subreddit_id
                    AND cl.subreddit_name = pl.subreddit_name
                    AND cl.language_name = pl.language_name
        GROUP BY 1, 2, 3, 4, 5
    ),
    subreddit_lang_things_rank AS (
        SELECT
            *
            , ((0.0 + language_count) / total_count) as language_percent
            , ROW_NUMBER() OVER (
                PARTITION BY subreddit_id
                ORDER BY language_count DESC, language_name
            ) AS language_rank
        FROM subreddit_lang_things
    )



-- Get all: posts+comments, posts, & comments in a single table
-- Do UNION because it's cleaner/easier now that all temp tables have the same columns
(
    SELECT *
    FROM subreddit_lang_posts_rank
    WHERE language_rank <= 20
        -- AND language_percent >= 0.01
        -- AND total_count >= 2
    -- ORDER BY subreddit_name, language_rank
    -- LIMIT 10
)
UNION DISTINCT
(
    SELECT *
    FROM subreddit_lang_comments_rank
    WHERE language_rank <= 20
        -- AND language_percent >= 0.01
        -- AND total_count >= 2
    ORDER BY subreddit_name, language_rank
    -- LIMIT 10
)
UNION DISTINCT
(
    SELECT *
    FROM subreddit_lang_things_rank
    WHERE language_rank <= 20
        -- AND language_percent >= 0.005
        -- AND total_count >= 2
    ORDER BY subreddit_name, language_rank
    -- LIMIT 10
)

ORDER BY subreddit_name ASC, thing_type, language_rank ASC
);  -- close CREATE TABLE parens
