-- Get ratings & other flags for all subs in the topic model
-- Use it to get "seed" communities for each topic + get activity per cluster
--  we'll filter/match to country in python
DECLARE PARTITION_DATE DATE DEFAULT (CURRENT_DATE() - 1);

SELECT
    t.subreddit_id
    , t.posts_for_modeling_count
    , t.subreddit_name
    , CASE
        WHEN (
            nt.rating_short = 'X'
            OR COALESCE(slo.over_18, '') = 't'
         ) THEN 1
        ELSE 0
    END AS over_18_or_X
    , slo.over_18
    , slo.allow_discovery
    , nt.rating_short
    , slo.type
    , nt.primary_topic
    , nt.rating_name

    , ars.users_l7
    , ars.users_l28

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
    -- Add rating so we can get filter out subs not rated as E
    LEFT JOIN (
        SELECT *
        FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
        -- Get latest partition
        WHERE dt = PARTITION_DATE
    ) AS slo
        ON t.subreddit_id = slo.subreddit_id
    LEFT JOIN (
        SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
        WHERE pt = PARTITION_DATE
    ) AS nt
        ON t.subreddit_id = nt.subreddit_id
    LEFT JOIN (
      SELECT *
      FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
      WHERE DATE(pt) = PARTITION_DATE
    ) AS ars
      ON t.subreddit_name = ars.subreddit_name


WHERE 1=1
--     AND model_sort_order >= 20000
--     AND posts_for_modeling_count >= 500

ORDER BY model_sort_order
;
