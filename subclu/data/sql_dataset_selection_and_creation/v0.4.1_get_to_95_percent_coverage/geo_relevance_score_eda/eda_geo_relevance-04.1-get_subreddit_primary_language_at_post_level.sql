-- Get primary language for a subreddit based only POSTS
-- Shape is long, e.g., 1 row = 1 subreddit + language + type(comment/post)
-- Original query in Mode for v0.4.0
--   https://app.mode.com/editor/reddit/reports/9c7601f78d3c/queries/db3c439930f2

DECLARE rating_date DATE DEFAULT '2022-01-22';

-- We can make the filters more strict later
DECLARE MIN_LANGUAGE_RANK NUMERIC DEFAULT 10;

CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_posts_primary_language_by_subreddit_20220122`
AS (
    WITH
    posts_lang AS (
        SELECT
            p.subreddit_id
            , p.subreddit_name
            , p.post_id
            , p.text_len
            , p.weighted_language
            , p.language_name
            , p.language_name_top_only

        FROM `reddit-employee-datasets.david_bermejo.subclu_posts_primary_language_check_20220122` AS p
    ),
    subreddit_lang_posts AS (
        SELECT
            subreddit_id
            , subreddit_name
            , weighted_language
            , 'post' AS thing_type
            , SUM(COUNT(post_id)) OVER (PARTITION BY subreddit_id) AS total_count
            , COUNT(post_id) AS language_count
            , ((0.0 + COUNT(post_id)) / (SUM(COUNT(post_id)) OVER (PARTITION BY subreddit_id))) as language_percent
        FROM posts_lang
        GROUP BY subreddit_id, subreddit_name, weighted_language,  thing_type
    ),
    subreddit_lang_posts_rank AS (
        SELECT
            *
            , ROW_NUMBER() OVER (
                PARTITION BY subreddit_id
                ORDER BY language_percent DESC
            ) AS language_rank
        FROM subreddit_lang_posts
    )

SELECT
    nt.rating_short
    , nt.rating_name
    , nt.primary_topic
    , a.*
    , ll.language_name
    , ll.language_name_top_only
    , ll.language_in_use_multilingual

FROM subreddit_lang_posts_rank AS a
    LEFT JOIN (
        SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
        WHERE pt = rating_date
    ) AS nt
        ON a.subreddit_id = nt.subreddit_id
    LEFT JOIN `reddit-employee-datasets.david_bermejo.language_detection_code_to_name_lookup_cld3` AS ll
        ON a.weighted_language = ll.language_code

WHERE 1=1
    -- AND subreddit_name IN ('100pushupsmumbai', '14yearoldofreddit')
    AND language_rank <= MIN_LANGUAGE_RANK

ORDER BY subreddit_name ASC, language_rank ASC
);  -- Close CREATE TABLE statement
