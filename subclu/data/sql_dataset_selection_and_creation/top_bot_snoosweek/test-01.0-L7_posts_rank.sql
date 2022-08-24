-- Get posts for L7 days in a sub so we can compare with UI
-- See results in:
-- https://docs.google.com/spreadsheets/d/1WmV9JMELdWjqlXdKu-KNbbHf-CRhkOPeX1Y1J3XxV1k/edit#gid=1738669588

DECLARE DT_END DATE DEFAULT "2022-08-22";  -- CURRENT_DATE() - 2;
DECLARE DT_START_7_DAY DATE DEFAULT DT_END - 6;

DECLARE N_POSTS_L7 NUMERIC DEFAULT 20;

-- Aggregate counts all time per post
WITH
-- TODO: authors_l7 CTE
posts_l7 AS (
    -- We need to exclude type=predictions b/c they can get a lot of upvotes but rewarding mods
    --  isn't in the spirit of top-Bot, is it?
    SELECT
        ROW_NUMBER() OVER(partition by subreddit_id ORDER BY karma_max DESC) post_rank_max_l7
        -- , ROW_NUMBER() OVER(partition by subreddit_id ORDER BY karma_mid DESC) post_rank_mid_l7
        , ROW_NUMBER() OVER(partition by subreddit_id ORDER BY score_raw DESC) post_rank_raw_l7
        , ROW_NUMBER() OVER(partition by subreddit_id ORDER BY karma DESC) post_rank_l7
        , karma
        , * EXCEPT(karma)
    FROM (
        SELECT
            slo.subreddit_name
            , sp.post_type
            , sp.post_title
            , sp.user_id
            , sp.subreddit_id
            , p.post_id

            , SUM(upvote_users) - SUM(downvote_users) - SUM(clearvote_users) AS karma
            , CAST(
                SUM(upvote_users) - SUM(downvote_users) - ROUND(0.5 * SUM(clearvote_users), 0)
                AS INT64
            ) AS karma_mid
            , SUM(upvote_users) - SUM(downvote_users) AS karma_max
            , SUM(upvotes) - SUM(downvotes) AS score_raw

            , SUM(upvotes) AS upvotes
            , SUM(upvote_users) AS upvote_users

            , SUM(downvotes) AS downvotes
            , SUM(downvote_users) AS downvote_users

        FROM (
            SELECT *
            FROM `data-prod-165221.ds_v2_aggregate_tables.post_daily_reporting`
            WHERE DATE(_PARTITIONTIME) BETWEEN DT_START_7_DAY  AND DT_END
                AND COALESCE(subreddit_name, "") != ""
                AND NOT REGEXP_CONTAINS(subreddit_name, r'^u_.*')
        ) AS p
            -- Merge to get post_type & subreddit_id (instead of name)
            LEFT JOIN (
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
                QUALIFY row_num = 1
            ) as sp
                ON p.post_id = sp.post_id

            -- Get latest subreddit_name
            LEFT JOIN (
                SELECT
                    subreddit_id
                    , name AS subreddit_name
                FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
                WHERE dt = DT_END
            ) AS slo
                ON sp.subreddit_id = slo.subreddit_id
        WHERE 1=1
            AND LOWER(slo.subreddit_name) = "formula1" -- askreddit
            -- AND post_id IN ("t3_wclubp", "t3_wn8soz")
        GROUP BY 1, 2, 3, 4, 5, 6
    )
    QUALIFY post_rank_l7 <= N_POSTS_L7
)

, post_with_meta AS (
    -- TODO(djb): Remove? maybe we don't need this extra join because we can get most of this info from sucessful_post
    SELECT
        p7.subreddit_id
        , p7.post_id
        , author_id
        , slo.subreddit_name
        , title
        -- , selftext
        -- , flair_text
        , p7.* EXCEPT(post_id)
    FROM posts_l7 AS p7
        LEFT JOIN `data-prod-165221.ds_v2_postgres_tables.post_lookup` AS plo
            ON p7.post_id = plo.post_id
        LEFT JOIN (
            SELECT
                subreddit_id
                , LOWER(name) as subreddit_name
                , dt
            FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
            WHERE dt = DT_END
        ) AS slo
            ON p7.subreddit_id = slo.subreddit_id
    WHERE 1=1
        AND DATE(plo._PARTITIONTIME) = DT_END
)


-- Final table
-- SELECT *
-- FROM post_with_meta

-- ORDER BY post_rank_l7
-- ;

-- Table with counts (intermediate counts)
SELECT *
FROM posts_l7
ORDER BY post_rank_l7
;
