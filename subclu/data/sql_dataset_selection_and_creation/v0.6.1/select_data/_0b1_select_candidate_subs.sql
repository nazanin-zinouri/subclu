-- Subreddit candidates
--  Use this table as basis to prefilter which subreddits to analyze
--  We'll use criteria from this table a lot, so better to have a temp table to speed
--  up downstream queries and to also keep things constant (removed posts might change over time)
-- Downstream we can filter:
--   - target date range
--   - over 100 views
--   - over 4 non-removed posts

DECLARE PARTITION_DATE DATE DEFAULT ${end_date};
DECLARE PT_POSTS_START DATE DEFAULT PARTITION_DATE - ${post_lookback_days};

-- Remove the min users requirement b/c for embeddings we'll want some defunct subs
-- DECLARE MIN_USERS_L7 NUMERIC DEFAULT 1;
DECLARE MIN_POSTS_L28_NOT_REMOVED NUMERIC DEFAULT 1;


CREATE OR REPLACE TABLE `reddit-relevance.${dataset}.subclu_subreddit_candidates_${run_id}` AS (
    WITH subs_with_views_and_posts_raw AS (
        SELECT
            asr.subreddit_name
            , asr.users_l7
            , COUNT(DISTINCT sp.post_id) as posts_not_removed_l28
        FROM (
            SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
            WHERE DATE(pt) = PARTITION_DATE
        ) AS asr
            -- Use INNER JOIN to reduce overhead from subs w/o recent posts
            --  can't trust `successful_posts` for the correct subreddit_id, because of
            --  subreddit name changes, so need to join with SLO to get the latest ID
            INNER JOIN (
                SELECT s.*
                FROM `data-prod-165221.cnc.successful_posts` AS s
                    LEFT JOIN (
                        SELECT * FROM `data-prod-165221.ds_v2_postgres_tables.post_lookup`
                        WHERE DATE(_PARTITIONTIME) = PARTITION_DATE
                    ) AS plo
                    ON s.subreddit_id = plo.subreddit_id AND s.post_id = plo.post_id
                        AND s.user_id = plo.author_id
            WHERE (dt) BETWEEN PT_POSTS_START AND PARTITION_DATE
                -- removed = actively removed by mods or automod (maybe admins too?)
                -- NOTE: some posts appear INCORRECTLY removed
                --  Example: Lots of examples in r/amItheAsshole. Maybe b/c of automod or karma threshold?
                AND (
                    COALESCE(s.removed, 0) = 0
                    OR (
                        COALESCE(s.removed, 0) = 1
                        AND s.upvotes >= 20
                    )
                )

                -- neutered = marked as spam;
                --  For v0.6.0, let's include neutered posts in case they're flagged incorrectly
                -- AND COALESCE(neutered, FALSE) = FALSE
            ) AS sp
                ON LOWER(asr.subreddit_name) = (sp.subreddit_name)
        GROUP BY 1, 2
    ),
    subs_above_view_and_post_threshold AS (
        SELECT
            s.subreddit_id
            , base.*
            , s.subscribers
            , s.type
            , s.over_18
            , nt.rating_short
            , primary_topic
            , nt.rating_name

            , s.allow_discovery
            , s.allow_top
            , s.allow_trending
            , s.video_whitelisted
            , s.whitelist_status
            , s.lang      AS subreddit_language

            , s.verdict
            , s.is_spam
            , s.is_deleted
            , s.deleted

        FROM subs_with_views_and_posts_raw AS base
            -- Join with SLO b/c that might have more recent delete & banned info
            --  Also, we can trust its subreddit_id values more
            INNER JOIN (
                SELECT *
                FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
                WHERE dt = PARTITION_DATE
            ) AS s
                ON LOWER(base.subreddit_name) = LOWER(s.name)

            LEFT JOIN (
                SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
                WHERE pt = PARTITION_DATE
            ) AS nt
                ON s.subreddit_id = nt.subreddit_id
        WHERE 1=1
            -- Remove the min users requirement b/c for embeddings we'll want some defunct subs
            -- AND users_l7 >=MIN_USERS_L7
            AND posts_not_removed_l28 >= MIN_POSTS_L28_NOT_REMOVED

            AND s.subreddit_id != 't5_4vo55w'  -- "r/profile" subreddit generates weird results

            -- Exclude user-profiles + spam & sketchy subs [optional]
            -- For v0.6.0 we want to include subs even if they're spam so that we can rate & classify them
            -- AND COALESCE(s.verdict, 'f') <> 'admin-removed'
            -- AND COALESCE(s.is_spam, FALSE) = FALSE
            -- AND COALESCE(s.is_deleted, FALSE) = FALSE
            -- AND s.deleted IS NULL
            -- AND type IN ('public', 'private', 'restricted')
            AND NOT REGEXP_CONTAINS(LOWER(base.subreddit_name), r'^u_.*')
    ),
    unique_posters_per_subreddit AS (
        -- This could be slightly off if a subreddit changed names recently
        --  sometimes there could be a discrepancy b/n sub name & ID
        SELECT
            subreddit_id
            , COUNT(*) as posts_l7_submitted
            , COUNT(DISTINCT user_id) as unique_posters_l7_submitted
        FROM
            -- Pull from cnc's table because it's more consistent with activity table
            -- `data-prod-165221.andrelytics_ez_schema.post_submit` as comment
            `data-prod-165221.cnc.successful_posts` AS sp
        WHERE
            DATE(dt) BETWEEN (PARTITION_DATE - 7) AND PARTITION_DATE
            AND noun = "post"
        GROUP BY
            subreddit_id
    )
    , final_meta AS (
        SELECT
            s.* EXCEPT( verdict, is_spam, is_deleted, deleted, type)
            , ups.posts_l7_submitted
            , ups.unique_posters_l7_submitted

            , acs.activity_7_day
            , acs.submits_7_day
            , acs.comments_7_day
            , acs.active
            , acs.highly_active

            , cc.weekly_consumes
            , cc.weekly_consumes_bucket

            , CASE WHEN amb.subreddit_id IS NOT NULL THEN 'organic_or_community_builder'
                ELSE NULL
            END AS i18n_type

            , s.verdict
            , s.is_spam
            , s.is_deleted
            , s.deleted
            , s.type

            , PARTITION_DATE AS pt
            , PT_POSTS_START AS successful_post_start_date

        FROM subs_above_view_and_post_threshold AS s
            -- acs won't have some defunct or inactive subs, so we need a left join
            LEFT JOIN (
                SELECT * FROM `data-prod-165221.ds_subreddit_whitelist_tables.active_subreddits`
                WHERE DATE(_PARTITIONTIME) = PARTITION_DATE
            ) AS acs
                -- NOTE: active_subreddits can have duplicate subreddit_id, so let's join on sub_id AND sub_name
                ON acs.subreddit_id = s.subreddit_id
                    AND acs.subreddit_name = s.subreddit_name

            LEFT JOIN unique_posters_per_subreddit AS ups
                ON s.subreddit_id = ups.subreddit_id

            LEFT JOIN (
                SELECT * FROM `data-prod-165221.ds_v2_gold_tables.econ_community_consumes`
                WHERE DATE(pt) = PARTITION_DATE
            ) AS cc
                ON s.subreddit_id = cc.subreddit_id

            LEFT JOIN (
                SELECT DISTINCT
                    subreddit_id
                FROM `data-prod-165221.i18n.amb_prog_communities_partitioned`
                WHERE DATE(pt) = PARTITION_DATE
                    AND subreddit_id IS NOT NULL
            ) AS amb
                ON s.subreddit_id = amb.subreddit_id
    )

SELECT
    *
FROM final_meta
ORDER BY users_l7 DESC, posts_not_removed_l28 DESC
); -- close create table parens
