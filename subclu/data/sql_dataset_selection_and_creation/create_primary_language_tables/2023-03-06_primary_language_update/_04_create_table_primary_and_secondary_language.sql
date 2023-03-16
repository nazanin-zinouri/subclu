-- Get primary & secondary languages for all subreddits

-- What "thing" do you want to check?
--  'post', 'comment', 'posts_and_comments'
DECLARE THING_TYPE_TO_COUNT STRING DEFAULT 'posts_and_comments';

CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subreddit_language_primary_posts_and_comments_20220808` AS (
WITH
sub_primary_language AS (
    SELECT
        lr.subreddit_id
        , lr.subreddit_name
        , lr.language_name AS primary_language
        , lr.language_percent AS primary_language_pct
        , lr.language_count AS primary_language_count
        , lr.thing_type
        , lr.total_count

    FROM `reddit-employee-datasets.david_bermejo.subreddit_language_rank_20220808` lr
    WHERE 1=1
        AND thing_type = THING_TYPE_TO_COUNT
        AND language_rank = 1
)
, sub_2nd_language AS (
    SELECT
        lr.subreddit_id
        , lr.language_name AS secondary_language
        , lr.language_percent AS secondary_language_pct
        , lr.language_count AS secondary_language_count

    FROM `reddit-employee-datasets.david_bermejo.subreddit_language_rank_20220808` lr
    WHERE 1=1
        AND thing_type = THING_TYPE_TO_COUNT
        AND language_rank = 2
)
, sub_primary_and_2nd_lang  AS (
    -- Use a self-join to get wide format (primary & secondary languages as columns instead of rows)
    SELECT
        l1.subreddit_id
        , l1.subreddit_name

        , l1. primary_language
        , l1.primary_language_pct
        -- , ROUND(l1.primary_language_pct, 9) AS primary_language_pct

        , l2.secondary_language
        , l2.secondary_language_pct
        -- , ROUND(l2.secondary_language_pct, 9) AS secondary_language_pct

        , l1.thing_type
        , primary_language_count, secondary_language_count
        , l1.total_count
        , (SELECT MIN(dt_start) FROM `reddit-employee-datasets.david_bermejo.subreddit_language_rank_20220808`) AS dt_start
        , (SELECT MAX(dt_end) FROM `reddit-employee-datasets.david_bermejo.subreddit_language_rank_20220808`) AS dt_end

    FROM sub_primary_language AS l1
        LEFT JOIN sub_2nd_language AS l2
            ON l1.subreddit_id = l2.subreddit_id

    ORDER BY subreddit_name
)

-- Append date range so it's easier to debug & check
SELECT
    *
FROM sub_primary_and_2nd_lang
);  -- close CREATE table parens


-- Tests & checks:
-- WHERE 1=1
--     -- ~200 with rounding diff
--     -- AND ROUND(primary_language_pct + secondary_language_pct, 3) != ROUND((primary_language_count + secondary_language_count) / total_count, 3)

--     -- Should be zero:
--     AND (secondary_language_pct != (secondary_language_count / total_count))

--     -- AND subreddit_name LIKE "ask%"
--     -- subs where primary & secondary don't add up to 100%
--     -- AND subreddit_name IN (
--     --     '000203992183188138818'
--     --     , '0011cant'
--     --     , '00sdesign'
--     -- )
--     -- AND subreddit_name IN (
--     --     'antiwork', 'de', 'france', 'askreddit', 'india'
--     --     , 'mexico', 'spain'
--     --     , 'brazil', 'brasil'
--     -- )
--     AND total_count >= 4
-- ;
