-- Subreddit candidates
--  Use this table as basis to prefilter which subreddits to analyze
--  We'll use criteria from this table a lot, so better to have a temp table to speed
--  up downstream queries and to also keep things constant (removed posts might change over time)
-- Create candidate subreddits table
--  Downstream we can filter:
--   - target date range
--   - over 100 views
--   - over 4 non-removed posts
-- Companion table to: subclu_geo_subreddit_candidates_20211214

DECLARE PARTITION_DATE DATE DEFAULT '2022-01-22';

DECLARE MIN_USERS_L7 NUMERIC DEFAULT 1;
DECLARE MIN_POSTS_L28_NOT_REMOVED NUMERIC DEFAULT 1;

CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_geo_subreddit_candidates_posts_no_removed_20220122`
AS (
    WITH
        subs_with_views_and_posts_raw AS (
            SELECT
                asr.subreddit_name
                , sp.subreddit_id
                , asr.users_l7
                , COUNT(DISTINCT sp.post_id) as posts_not_removed_l28
            FROM (
                    SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
                    WHERE DATE(pt) = partition_date
                ) AS asr
                LEFT JOIN (
                        SELECT *
                        FROM `data-prod-165221.cnc.successful_posts`
                        WHERE (dt) BETWEEN (PARTITION_DATE - 29) AND partition_date
                            AND removed = 0
                    ) AS sp
                        ON LOWER(asr.subreddit_name) = (sp.subreddit_name)
            GROUP BY 1, 2, 3
        ),
        subs_above_view_and_post_threshold AS (
            SELECT *
            FROM subs_with_views_and_posts_raw
            WHERE
                users_l7 >=MIN_USERS_L7
                AND posts_not_removed_l28 >= MIN_POSTS_L28_NOT_REMOVED
        )

    SELECT
        *
        , partition_date
        , (partition_date - 29) AS successful_post_start_date
    FROM subs_above_view_and_post_threshold
    ORDER BY users_l7, posts_not_removed_l28
)
;
