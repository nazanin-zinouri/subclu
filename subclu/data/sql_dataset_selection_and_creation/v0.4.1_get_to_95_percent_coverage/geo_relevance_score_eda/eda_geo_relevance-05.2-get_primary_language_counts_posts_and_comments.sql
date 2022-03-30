-- Language-based filters
-- Original query in Mode for v0.4.0
--   https://app.mode.com/editor/reddit/reports/9c7601f78d3c/queries/db3c439930f2
-- TODO(djb): update to:
--  - pull ALL comments & posts (not just the top)
DECLARE partition_date DATE DEFAULT '2021-12-13';
DECLARE rating_date DATE DEFAULT '2022-01-11';

DECLARE MIN_LANGUAGE_PERCENT NUMERIC DEFAULT 0.1;
DECLARE MIN_LANGUAGE_RANK NUMERIC DEFAULT 3;

-- Get subreddit language using both posts and comments
-- shape is long, e.g., 1 row = 1 subreddit + language + type(comment/post)
WITH
    comments_lang AS (
        SELECT
            co.subreddit_id
            , co.subreddit_name
            , co.post_id
            , co.comment_id
            , co.comment_text_len
            , co.weighted_language      AS language_weigthed_code
            , ll.language_name_top_only AS language_name

        FROM `reddit-employee-datasets.david_bermejo.subclu_comments_top_no_geo_20211214` AS co
            LEFT JOIN `reddit-employee-datasets.david_bermejo.language_detection_code_to_name_lookup_cld3` AS ll
                ON co.weighted_language = ll.language_code
    ),
    subreddit_lang_comments AS (
        SELECT
            subreddit_id
            , subreddit_name
            , language_name
            , 'comment' AS thing_type
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
                ORDER BY language_percent DESC
            ) AS language_rank
        FROM subreddit_lang_comments
    ),
    posts_lang AS (
        SELECT
            p.subreddit_id
            , p.subreddit_name
            , p.post_id
            , p.text_len
            , p.weighted_language      AS language_weigthed_code
            , ll.language_name_top_only AS language_name

        FROM `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20211214` AS p
            LEFT JOIN `reddit-employee-datasets.david_bermejo.language_detection_code_to_name_lookup_cld3` AS ll
                ON p.weighted_language = ll.language_code
    ),
    subreddit_lang_posts AS (
        SELECT
            subreddit_id
            , subreddit_name
            , language_name
            , 'post' AS thing_type
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
                ORDER BY language_percent DESC
            ) AS language_rank
        FROM subreddit_lang_posts
    ),
    subreddit_lang_things AS (
        SELECT
            COALESCE(pl.subreddit_id, cl.subreddit_id)          AS subreddit_id
            , COALESCE(pl.subreddit_name, cl.subreddit_name)    AS subreddit_name
            , COALESCE(pl.language_name, cl.language_name)      AS language_name
            , 'posts_and_comments' AS thing_type

            , SUM(0 + pl.total_count + cl.total_count) AS total_count
            , SUM(0 + pl.language_count + cl.language_count) AS language_count
        FROM subreddit_lang_posts AS pl
            FULL OUTER JOIN subreddit_lang_comments AS cl
                ON cl.subreddit_id = pl.subreddit_id
                    AND cl.subreddit_name = pl.subreddit_name
                    AND cl.language_name = pl.language_name
        GROUP BY 1, 2, 3, 4
    ),
    subreddit_lang_things_rank AS (
        SELECT
            *
            , ((0.0 + language_count) / total_count) as language_percent
            , ROW_NUMBER() OVER (
                PARTITION BY subreddit_id
                ORDER BY language_count DESC
            ) AS language_rank
        FROM subreddit_lang_things
    ),
    subreddit_lang_union_all AS (
        -- Get all: posts+comments, posts, & comments in a single table
        -- Do UNION because it's cleaner/easier now that all temp tables have the same columns
        (
            SELECT *
            FROM subreddit_lang_posts_rank
            WHERE language_rank <= 5
                AND language_percent >= .04
                AND total_count >= 1
            -- LIMIT 10
        )
        UNION DISTINCT
        (
            SELECT *
            FROM subreddit_lang_comments_rank
            WHERE language_rank <= 5
                AND language_percent >= 0.04
                AND total_count >= 1
            ORDER BY subreddit_name, language_rank
        )
        UNION DISTINCT
        (
            SELECT *
            FROM subreddit_lang_things_rank
            WHERE language_rank <= 5
                AND language_percent >= 0.04
                AND total_count >= 2
            ORDER BY subreddit_name, language_rank
            -- LIMIT 10
        )
    )

SELECT
    nt.rating_short
    , nt.rating_name
    , nt.primary_topic
    , a.*
FROM subreddit_lang_union_all AS a
    LEFT JOIN (
        SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
        WHERE pt = rating_date
    ) AS nt
        ON a.subreddit_id = nt.subreddit_id

WHERE 1=1
    -- AND subreddit_name IN ('100pushupsmumbai', '14yearoldofreddit')
    AND language_percent >= MIN_LANGUAGE_PERCENT
    AND language_rank <= MIN_LANGUAGE_RANK

ORDER BY subreddit_name ASC, language_percent DESC
;

