-- Get top contributors per subreddit POSTS over L7 days and L28 days
DECLARE DT_END DATE DEFAULT CURRENT_DATE() - 2;
DECLARE DT_START_WEEK DATE DEFAULT DT_END - 6;
DECLARE DT_START_MONTH DATE DEFAULT DT_END - 27;

DECLARE TOP_N_USERS_WEEK NUMERIC DEFAULT 1500;
DECLARE TOP_N_USERS_MONTH NUMERIC DEFAULT 5000;

-- ==================
-- Only need to create the first time we run it
-- ===
-- CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.top_bot_author_posts`
-- CREATE TABLE IF NOT EXISTS `reddit-employee-datasets.david_bermejo.top_bot_author_posts`
-- PARTITION BY pt AS (

-- ==================
-- After table is created, we can delete a partition & update it
-- ===
DELETE
    `reddit-employee-datasets.david_bermejo.top_bot_author_posts`
WHERE
    pt = DT_END
;

-- Insert latest data
INSERT INTO `reddit-employee-datasets.david_bermejo.top_bot_author_posts`
(

WITH
authors_posts_week AS (
    SELECT
        DT_END AS pt
        , "week" AS time_frame
        , *
        , ROW_NUMBER() OVER(
          partition by subreddit_id ORDER BY post_karma DESC, post_count DESC, user_id
        ) author_rank
    FROM (
        SELECT
            sp.subreddit_id
            , slo.subreddit_name
            , sp.user_id

            , SUM(upvote_users) - SUM(downvote_users) - SUM(clearvote_users) AS post_karma
            , COUNT(DISTINCT sp.post_id) AS post_count

        FROM (
            SELECT *
            FROM `data-prod-165221.ds_v2_aggregate_tables.post_daily_reporting`
            WHERE DATE(_PARTITIONTIME) BETWEEN DT_START_WEEK  AND DT_END
                AND COALESCE(subreddit_name, "") != ""
                AND NOT REGEXP_CONTAINS(subreddit_name, r'^u_.*')
        ) AS p
            -- Merge to get post_type & subreddit_id (instead of name)
            INNER JOIN (
                SELECT
                    -- Use row_number to get the latest edit as row=1
                    ROW_NUMBER() OVER (
                        PARTITION BY post_id
                        ORDER BY endpoint_timestamp DESC, removal_timestamp DESC
                    ) AS row_num
                    , post_id
                    , user_id
                    , post_type
                    , post_title
                    , subreddit_id
                FROM `data-prod-165221.cnc.successful_posts`
                WHERE 1=1
                    -- Only posts that were created in the target date range
                    AND dt BETWEEN DT_START_WEEK AND DT_END
                    -- Exclude posts that have been removed. NOTE: some posts appear INCORRECTLY removed
                    --  Lots of examples in r/amItheAsshole. Maybe b/c of automod?
                    AND (
                        COALESCE(removed, 0) = 0
                        OR (
                            COALESCE(removed, 0) = 1
                            AND upvotes >= 5
                        )
                    )

                    -- Exclude user profiles
                    AND NOT REGEXP_CONTAINS(subreddit_name, r'^u_.*')
                QUALIFY row_num = 1
            ) as sp
                ON p.post_id = sp.post_id
            -- Exclude posts that have been neutered (flagged as spam)
            LEFT JOIN (
                SELECT
                    post_id
                    , neutered
                    , verdict
                FROM `data-prod-165221.ds_v2_postgres_tables.post_lookup`
                WHERE DATE(_PARTITIONTIME) = DT_END
            ) AS pl
                ON p.post_id = pl.post_id

            -- Get latest subreddit_name & status
            LEFT JOIN (
                SELECT
                    subreddit_id
                    , name AS subreddit_name
                    , verdict
                    , quarantine
                    , is_spam
                    , is_deleted
                    , deleted
                    , over_18
                FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
                WHERE dt = DT_END
                    AND NOT REGEXP_CONTAINS(LOWER(name), r'^u_.*')
            ) AS slo
                ON sp.subreddit_id = slo.subreddit_id
        WHERE 1=1
            -- Exclude subs that are quarantined, removed, deleted, or marked as spam
            AND COALESCE(slo.verdict, "") != 'admin-removed'
            AND COALESCE(slo.is_spam, FALSE) = FALSE
            AND COALESCE(slo.is_deleted, FALSE) = FALSE
            AND slo.deleted IS NULL
            -- AND COALESCE(slo.quarantine, FALSE) = FALSE

            AND (
                -- Filter out spam posts
                COALESCE(pl.neutered, false) = false

                -- Keep posts that were flagged/neutered, but then approved
                OR (
                    COALESCE(pl.neutered, false) = true
                    AND COALESCE(pl.verdict, '') IN ('mod-approved', 'admin-approved')
                )
            )

            -- Test by limiting subreddits
            -- AND LOWER(slo.subreddit_name) IN (
            --     "amitheasshole"
            --     , "formula1", "askreddit", "de", "mexico"
            -- )
        GROUP BY 1, 2, 3
    )
    QUALIFY author_rank <= TOP_N_USERS_WEEK
    ORDER BY subreddit_name, author_rank
)
, authors_posts_month AS (
    SELECT
        DT_END AS pt
        , "month" AS time_frame
        , *
        , ROW_NUMBER() OVER(
          partition by subreddit_id ORDER BY post_karma DESC, post_count DESC, user_id
        ) author_rank
    FROM (
        SELECT
            sp.subreddit_id
            , slo.subreddit_name
            , sp.user_id

            , SUM(upvote_users) - SUM(downvote_users) - SUM(clearvote_users) AS post_karma
            , COUNT(DISTINCT sp.post_id) AS post_count

        FROM (
            SELECT *
            FROM `data-prod-165221.ds_v2_aggregate_tables.post_daily_reporting`
            WHERE DATE(_PARTITIONTIME) BETWEEN DT_START_MONTH  AND DT_END
                AND COALESCE(subreddit_name, "") != ""
                AND NOT REGEXP_CONTAINS(subreddit_name, r'^u_.*')
        ) AS p
            -- Merge to get post_type & subreddit_id (instead of name)
            INNER JOIN (
                SELECT
                    -- Use row_number to get the latest edit as row=1
                    ROW_NUMBER() OVER (
                        PARTITION BY post_id
                        ORDER BY endpoint_timestamp DESC, removal_timestamp DESC
                    ) AS row_num
                    , post_id
                    , user_id
                    , post_type
                    , post_title
                    , subreddit_id
                FROM `data-prod-165221.cnc.successful_posts`
                WHERE 1=1
                    -- Only posts that were created in the target date range
                    AND dt BETWEEN DT_START_MONTH AND DT_END
                    -- Exclude posts that have been removed. NOTE: some posts appear INCORRECTLY removed
                    --  Lots of examples in r/amItheAsshole. Maybe b/c of automod?
                    AND (
                        COALESCE(removed, 0) = 0
                        OR (
                            COALESCE(removed, 0) = 1
                            AND upvotes >= 5
                        )
                    )

                    -- Exclude user profiles
                    AND NOT REGEXP_CONTAINS(subreddit_name, r'^u_.*')
                QUALIFY row_num = 1
            ) as sp
                ON p.post_id = sp.post_id
            -- Exclude posts that have been neutered (flagged as spam)
            LEFT JOIN (
                SELECT
                    post_id
                    , neutered
                    , verdict
                FROM `data-prod-165221.ds_v2_postgres_tables.post_lookup`
                WHERE DATE(_PARTITIONTIME) = DT_END
            ) AS pl
                ON p.post_id = pl.post_id

            -- Get latest subreddit_name & status
            LEFT JOIN (
                SELECT
                    subreddit_id
                    , name AS subreddit_name
                    , verdict
                    , quarantine
                    , is_spam
                    , is_deleted
                    , deleted
                    , over_18
                FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
                WHERE dt = DT_END
                    AND NOT REGEXP_CONTAINS(LOWER(name), r'^u_.*')
            ) AS slo
                ON sp.subreddit_id = slo.subreddit_id
        WHERE 1=1
            -- Exclude subs that are quarantined, removed, deleted, or marked as spam
            AND COALESCE(slo.verdict, "") != 'admin-removed'
            AND COALESCE(slo.is_spam, FALSE) = FALSE
            AND COALESCE(slo.is_deleted, FALSE) = FALSE
            AND slo.deleted IS NULL
            -- AND COALESCE(slo.quarantine, FALSE) = FALSE

            AND (
                -- Filter out spam posts
                COALESCE(pl.neutered, false) = false

                -- Keep posts that were flagged/neutered, but then approved
                OR (
                    COALESCE(pl.neutered, false) = true
                    AND COALESCE(pl.verdict, '') IN ('mod-approved', 'admin-approved')
                )
            )
        GROUP BY 1, 2, 3
    )
    QUALIFY author_rank <= TOP_N_USERS_MONTH
)


-- Table with counts (intermediate counts)
SELECT * FROM authors_posts_week
UNION ALL
SELECT * FROM authors_posts_month
);
