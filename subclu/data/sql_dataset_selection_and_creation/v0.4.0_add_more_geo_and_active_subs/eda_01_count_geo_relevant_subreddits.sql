-- We have an OKR for topic model to cover 95% of i18n geo-relevant subreddits
--  So we need to actually count and define what this means
-- Colab notebook:
--  https://colab.research.google.com/drive/1ut0VvzRUkFpjSuVP0h88m0aT5LHUM8gK#scrollTo=Z2VDEgdL5IPX

-- Select all subreddits & get stats to check topic-cluster coverage
DECLARE partition_date DATE DEFAULT '2021-10-25';

WITH
    unique_posts_per_subreddit AS (
        SELECT
            subreddit_id
            , subreddit_name
            , COUNT(*) as posts_l7_submitted
            , COUNT(DISTINCT user_id) as unique_posters_l7_submitted
        FROM
            -- Pull from cnc's table because it's more concistent with activity table
            -- `data-prod-165221.andrelytics_ez_schema.post_submit` as comment
            `data-prod-165221.cnc.successful_posts` AS sp
        WHERE
            DATE(dt) BETWEEN (partition_date - 7) AND partition_date
            AND noun = "post"
        GROUP BY
            subreddit_id, subreddit_name
    )


SELECT
    COALESCE(slo.subreddit_id, acs.subreddit_id)  AS subreddit_id
    , COALESCE(acs.subreddit_name, LOWER(slo.name)) AS subreddit_name
    , slo.subscribers
    -- , DATE_DIFF(CURRENT_DATE(), DATE(slo.created_date), DAY) AS subreddit_age_days
    , DATE_DIFF(CURRENT_DATE(), DATE(slo.created_date), MONTH) AS subreddit_age_months
    , DATE_DIFF(CURRENT_DATE(), DATE(slo.created_date), YEAR) AS subreddit_age_years

    -- Sub activity
    , CASE
        WHEN COALESCE(users_l7, 0) >= 100 THEN true
        ELSE false
    END AS over_100_users_l7
    , asr.users_l7
    , asr.users_l28
    , acs.active
    , acs.activity_7_day

    , CASE
        WHEN (
            (COALESCE(posts_l7, 0) = 0)
            OR (unique_posters_l7_submitted IS NULL) ) THEN "0_posters"
        WHEN COALESCE(unique_posters_l7_submitted, 0) = 1 THEN "1_poster"
        ELSE "2_or_more_posters"
    END AS unique_posters_l7_bin
    , u.unique_posters_l7_submitted

    , asr.posts_l7
    , asr.comments_l7
    , asr.posts_l28
    , asr.comments_l28

    -- Rating info
    , slo.whitelist_status AS ads_allow_list_status
    , slo.over_18
    , rating_short
    , rating_name
    , primary_topic
    , rating_weight
    -- , array_to_string(mature_themes,", ") as mature_themes

    , slo.verdict
    , slo.quarantine
    , slo.allow_discovery
    , slo.is_deleted
    , slo.is_spam


FROM (
    SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
    WHERE DATE(pt) = partition_date
) AS asr
    INNER JOIN (
        -- subreddit_lookup includes pages for users, so we need LEFT JOIN
        --  or INNER JOIN with active_subreddits or all_reddit_subreddits
        SELECT * FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
        WHERE dt = partition_date
            AND COALESCE(type, '') != 'user'
    ) AS slo
        ON asr.subreddit_name = LOWER(slo.name)
    LEFT JOIN unique_posts_per_subreddit AS u
        ON asr.subreddit_name = u.subreddit_name

    LEFT JOIN (
        SELECT * FROM `data-prod-165221.ds_subreddit_whitelist_tables.active_subreddits`
        WHERE DATE(_PARTITIONTIME) = partition_date
            -- AND activity_7_day IS NOT NULL
    ) AS acs
        ON slo.subreddit_id = acs.subreddit_id

    LEFT JOIN (
        -- New view should be visible to all, but still comes from cnc_taxonomy_cassandra_sync
        SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
        WHERE pt = partition_date
    ) AS nt
        ON acs.subreddit_id = nt.subreddit_id

WHERE 1=1
    AND asr.users_l7 >= 100
    AND COALESCE(is_spam, false) = false
    AND COALESCE(is_deleted, false) = false

    -- AND posts_l7_andre > COALESCE(posts_l7, 0)

ORDER BY users_l7 DESC, posts_l7 ASC, activity_7_day ASC # subreddit_name ASC

-- LIMIT 5000
;
