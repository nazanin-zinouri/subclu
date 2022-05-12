WITH
    cluster_primary_topics AS (
        SELECT
            m.k_0100_label_name
            , COALESCE(nt.primary_topic, 'UNRATED') AS primary_topic

            , SUM(COUNT(t.subreddit_id)) OVER (PARTITION BY m.k_0100_label_name) AS subreddits_in_cluster_count
            , COUNT(t.subreddit_id) AS topic_count
            , ((0.0 + COUNT(t.subreddit_id)) / (SUM(COUNT(t.subreddit_id)) OVER (PARTITION BY m.k_0100_label_name))) as topic_percent
        FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full` AS t
            LEFT JOIN (
                SELECT
                    * EXCEPT(primary_topic, rating_short)
                    , COALESCE(primary_topic, 'UNRATED') AS primary_topic
                    , COALESCE(rating_short, 'UNRATED') AS rating_short
                FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
                WHERE pt = (CURRENT_DATE() - 2)
            ) AS nt
                ON t.subreddit_id = nt.subreddit_id
            -- Merge with the new manual labels table so that we can get the rank by new cluster name
            LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_manual_names` AS m
                ON t.k_0400_label = m.k_0400_label

        GROUP BY 1, 2
    )
    , cluster_primary_topics_rank AS (
        SELECT
            *
            , ROW_NUMBER() OVER (
                PARTITION BY k_0100_label_name
                ORDER BY topic_percent DESC
            ) AS topic_rank
        FROM cluster_primary_topics
    )
    , primary_topics_per_cluster AS (
        SELECT
            k_0100_label_name
            , STRING_AGG(topic_and_percent, ' | ') AS cluster_primary_topics
        FROM (
            SELECT
              k_0100_label_name
              , FORMAT(
                  '%s%% %s'
                  , CAST(COALESCE(ROUND(100.0 * topic_percent, 0), 0.) AS STRING)
                  , primary_topic
              ) AS topic_and_percent
          FROM cluster_primary_topics_rank
          WHERE topic_rank <= 5
        )
        GROUP BY 1
    )


SELECT
    r.k_0100_label_name
    , r.subreddits_in_cluster_count
    , primary_topic AS top_topic
    , topic_percent AS top_topic_percent
    , p.* EXCEPT(k_0100_label_name)
FROM cluster_primary_topics_rank AS r
    LEFT JOIN primary_topics_per_cluster AS p
        ON r.k_0100_label_name = p.k_0100_label_name
WHERE 1=1
    AND topic_rank = 1

ORDER BY 1
;
