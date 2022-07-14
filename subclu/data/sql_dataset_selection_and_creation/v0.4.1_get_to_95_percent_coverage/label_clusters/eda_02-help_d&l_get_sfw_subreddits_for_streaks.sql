-- Use topic model + other sources to exclue NSFW subreddits

WITH topic_cluster AS (
SELECT
    s.subreddit_id
    -- , nt.rating_short
    -- , nt.rating_name
    , COALESCE(tm.k_0100_label_name, nt.primary_topic) AS cluster_name_or_primary_topic
    , LOWER(name) AS subreddit_name
    , nt.primary_topic
    , tm.k_0100_label_name AS cluster_name
    , tm.k_0400_label_name AS cluster_subtopic
    -- , s.over_18
    -- , s.whitelist_status

FROM (
    SELECT *
    FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
    WHERE DATE(dt) = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
) AS s
    -- JOIN topic model IDs
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full` AS ti
        ON s.subreddit_id = ti.subreddit_id
    -- Join manual topic model labels
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_manual_names` AS tm
        ON ti.k_0400_label = tm.k_0400_label

    LEFT JOIN (
        SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
        WHERE pt =  DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    ) AS nt
        ON s.subreddit_id = nt.subreddit_id

WHERE 1=1
    -- Exclude user profiles. These are not rated
    AND NOT REGEXP_CONTAINS(LOWER(s.name), r'^u_.*')

    -- [optional] Only include subs in the topic model
    AND ti.subreddit_id IS NOT NULL

    -- OPTIONAL: Exclude spam, removed, & sketchy subs
    AND COALESCE(s.verdict, 'f') <> 'admin-removed'
    AND COALESCE(s.is_spam, FALSE) = FALSE
    AND COALESCE(s.is_deleted, FALSE) = FALSE
    AND s.deleted IS NULL
    AND type IN ('public', 'private', 'restricted')

ORDER BY cluster_name, cluster_subtopic
)


SELECT
    b.cluster_name
    , b.cluster_subtopic
    , nt.rating_short
    , nt.rating_name
    , s.over_18
    , s.whitelist_status
    , a.*
FROM `reddit-employee-datasets.dhwani_bosamiya.streak_level_2_7d` a
    LEFT JOIN topic_cluster b
        ON a.subreddit_id = b.subreddit_id

    -- add rating & subreddit lookup to check ratings for the non-topic cluster table
    LEFT JOIN (
        SELECT *
        FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
        WHERE DATE(dt) = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    ) AS s
        ON a.subreddit_id = s.subreddit_id
    LEFT JOIN (
        SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
        WHERE pt =  DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    ) AS nt
        ON a.subreddit_id = nt.subreddit_id

WHERE 1=1
    -- Exclude subs that have a non-E rating (coalesce 'E' will keep unrated subs)
    AND COALESCE(nt.rating_short, 'E') = 'E'

    -- Exclude subs marked as over_18 by moderators
    AND COALESCE(s.over_18, '') != 't'

    -- Exclude subreddits that have been excluded for ads
    AND COALESCE(s.whitelist_status, '') NOT IN ('no_ads', 'promo_adult_nsfw')

    -- Cluster topic NSFW, **NOTE**: some subreddits may be incorrectly flagged as NSFW
    AND (
        COALESCE(cluster_name, '') != 'Porn & Celebrity'
        AND COALESCE(cluster_subtopic, '') NOT IN (
            'Memes & Adult content', 'Questions, Mature Themes and Adult Content'
            , 'Mature Themes and Adult Content', 'Porn & Celebrity'
        )
        OR (
            -- allow rating + whitelist_status to over-ride the topic model
            COALESCE(s.whitelist_status, '') = 'all_ads'
            AND COALESCE(nt.rating_short, '') = 'E'
            AND COALESCE(cluster_name, '') = 'Porn & Celebrity'
        )
    )

    AND a.subreddit_name NOT IN (
        'sugarmamma', 'snapsexting1', 'vgb', 'mallubabes', 'replikatown', 'celebbattles', 'celebswithpetitetits', 'supermodelindia', 'yanetgracia'
        , 'indianmodelsactress', 'rc_healthchecks', 'instagramlivensfw18', 'earnyourkeep', 'wrestlewiththejoshis', 'oldschoolcelebs', 'jacas'
    )

ORDER BY num_total_posts DESC
;
