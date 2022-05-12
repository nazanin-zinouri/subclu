-- Get activity for all subs in the topic model
-- Use it to get "seed" communities for each topic + get activity per cluster
--  we'll filter/match to country separately

SELECT
    t.subreddit_id
    , t.k_0100_label
    , t.k_0400_label
    , t.subreddit_name
    -- , t.posts_for_modeling_count
    , ars.users_l7
    , ars.users_l28
    , ROW_NUMBER() OVER (PARTITION BY k_0100_label ORDER BY users_l7 DESC, users_l28 DESC) as users_l7_rank_100
    , ROW_NUMBER() OVER (PARTITION BY k_0400_label ORDER BY users_l7 DESC, users_l28 DESC) as users_l7_rank_400
    , ars.posts_l7
    , ars.posts_l28
    , ROW_NUMBER() OVER (PARTITION BY k_0100_label ORDER BY posts_l7 DESC, posts_l28 DESC) as posts_l7_rank_100
    , ROW_NUMBER() OVER (PARTITION BY k_0400_label ORDER BY posts_l7 DESC, posts_l28 DESC) as posts_l7_rank_400

    -- cuts by user user type/platform
    --  Note: only calculate cluster percentages in a GROUPBY, otherwise we'd calculate average of percents
    , ars.seo_users_l7
    , ars.loggedin_users_l7
    , (ars.ios_users_l7 + ars.android_users_l7) AS app_users_l7
    , ars.mweb_users_l7
    , ars.ios_users_l7
    , ars.android_users_l7

    , ars.seo_users_l28
    , ars.loggedin_users_l28
    , ars.mweb_users_l28
    , (ars.ios_users_l28 + ars.android_users_l28) AS app_users_l28
    , ars.ios_users_l28
    , ars.android_users_l28

FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full` AS t
    -- Add latests tags so we can get filter out subs marked as spam
    LEFT JOIN (
        SELECT *
        FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
        -- Get latest partition
        WHERE dt = (CURRENT_DATE() - 2)
    ) AS slo
        ON t.subreddit_id = slo.subreddit_id
    LEFT JOIN (
        SELECT *
        FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
        WHERE DATE(pt) = (CURRENT_DATE() - 2)
    ) AS ars
      ON t.subreddit_name = LOWER(ars.subreddit_name)
WHERE 1=1
    AND COALESCE(verdict, 'f') <> 'admin_removed'
    AND COALESCE(is_spam, FALSE) = FALSE
    -- AND COALESCE(over_18, 'f') = 'f'
    AND COALESCE(is_deleted, FALSE) = FALSE
    AND deleted IS NULL

-- ORDER BY k_0100_label, users_l7_rank_400 ASC, users_l7 DESC
;
