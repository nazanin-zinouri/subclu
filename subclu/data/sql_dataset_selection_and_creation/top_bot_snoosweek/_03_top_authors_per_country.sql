-- Get leaderboard at country-level
DECLARE PT_DATE DATE DEFAULT "2022-08-23";  -- Change when/if we create an ETL
DECLARE TARGET_COUNTRY DEFAULT 'Mexico';

WITH
subs_in_target_country AS (
    SELECT
        c.subreddit_id
        , m.country_name
        , activity_7_day
        , slo.over_18
    FROM `data-prod-165221.i18n.community_local_scores` AS c
        LEFT JOIN `reddit-employee-datasets.david_bermejo.countrycode_name_mapping` AS m
            ON c.geo_country_code = m.country_code
        -- Get latest subreddit_name & status
            LEFT JOIN (
                SELECT
                    subreddit_id
                    , over_18
                FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
                WHERE dt = PT_DATE
                    AND NOT REGEXP_CONTAINS(LOWER(name), r'^u_.*')
            ) AS slo
                ON c.subreddit_id = slo.subreddit_id
    WHERE DATE(c.pt) = PT_DATE
        AND sub_dau_perc_l28 >= 0.20
        AND m.country_name = TARGET_COUNTRY
)

, user_comment_counts AS (
    SELECT
        t.time_frame
        , t.user_id

        , SUM(comment_count) AS comment_count
        , SUM(comment_karma) AS comment_karma
        , COUNT(DISTINCT t.subreddit_id) AS comment_subreddit_count
        , STRING_AGG(subreddit_name, ", " ORDER BY c.activity_7_day DESC LIMIT 5) AS comment_subreddit_names
    FROM `reddit-employee-datasets.david_bermejo.top_bot_author_comments` AS t
        INNER JOIN subs_in_target_country AS c
            ON t.subreddit_id = c.subreddit_id
    WHERE pt = PT_DATE
        -- AND author_rank <= 5
    GROUP BY 1, 2
)
, user_comment_rank AS (
    SELECT
        ROW_NUMBER() OVER(
            PARTITION BY time_frame
            ORDER BY comment_karma DESC, comment_count DESC, comment_subreddit_count DESC, user_id
        ) AS comment_author_rank
        , *
    FROM user_comment_counts
    -- QUALIFY comment_author_rank <= 10
    -- ORDER BY comment_author_rank
)

, user_post_counts AS (
    SELECT
        t.time_frame
        , t.user_id

        , SUM(post_count) AS post_count
        , SUM(post_karma) AS post_karma
        , COUNT(DISTINCT t.subreddit_id) AS post_subreddit_count
        , STRING_AGG(subreddit_name, ", " ORDER BY c.activity_7_day DESC LIMIT 5) AS post_subreddit_names
    FROM `reddit-employee-datasets.david_bermejo.top_bot_author_posts` AS t
        INNER JOIN subs_in_target_country AS c
            ON t.subreddit_id = c.subreddit_id
    WHERE pt = PT_DATE
        AND author_rank <= 5
    -- ORDER BY activity_7_day DESC, subreddit_name, time_frame, author_rank
    GROUP BY 1, 2
)
, user_post_rank AS (
    SELECT
        ROW_NUMBER() OVER(
            PARTITION BY time_frame
            ORDER BY post_karma DESC, post_count DESC, post_subreddit_count DESC, user_id
        ) AS post_author_rank
        , *
    FROM user_post_counts
    -- QUALIFY post_author_rank <= 10
    -- ORDER BY post_author_rank
)

SELECT
    COALESCE(c.time_frame, p.time_frame) AS time_frame
    , COALESCE(c.user_id, p.user_id) AS user_id
    , c.comment_author_rank
    , p.post_author_rank
    , c.comment_karma
    , p.post_karma
    , comment_count
    , post_count
    , c.* EXCEPT(time_frame, user_id, comment_author_rank, comment_karma, comment_count)
    , p.* EXCEPT(time_frame, user_id, post_author_rank, post_karma, post_count)
FROM user_comment_rank AS c
    FULL OUTER JOIN user_post_rank AS p
        ON c.user_id = p.user_id AND c.time_frame = p.time_frame
WHERE 1=1
    AND (
        post_author_rank <= 10
        OR comment_author_rank <= 10
    )
ORDER BY time_frame, comment_author_rank, post_author_rank
;
