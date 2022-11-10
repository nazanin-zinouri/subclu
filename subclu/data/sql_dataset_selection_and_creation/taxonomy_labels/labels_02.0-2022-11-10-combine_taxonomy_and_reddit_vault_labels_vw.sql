-- Get all label sources for a subreddit
-- NOTE: as of 2022-08 we only have snapshots of the Reddit curator (taxonomy) labels

-- CREATE VIEW `reddit-employee-datasets.david_bermejo.reddit_vault_predictions_and_overrides_vw`
-- OPTIONS(
--     description="View that combines all sources for topic & rating labels: curator, crowd, & CA model. Wiki: https://reddit.atlassian.net/wiki/spaces/DataScience/pages/2389278980/Content+Analytics+Key+Tables"
-- )

WITH all_subreddit_labels AS (
    SELECT
        (CURRENT_DATE() - 2) AS dt
        , slo.subreddit_id
        , LOWER(name) AS subreddit_name
        , DATE(verification_timestamp) AS crowd_verification_dt
        , c.date_retrieved AS curator_dt

        -- Booleans to indicate source for RATING
        , IF(c.curator_rating IS NULL, 0, 1) AS curator_rating_tag
        , IF(t.rating_short IS NULL, 0, 1) AS crowd_rating_tag
        , IF(p.predicted_rating IS NULL, 0, 1) AS model_rating_tag
        -- When deciding the subreddit rating we take the first available rating in this order:
        --  curator > crowd > predicted
         , CASE
            WHEN c.curator_rating IS NOT NULL THEN c.curator_rating
            WHEN t.rating_short IS NOT NULL THEN t.rating_short
            ELSE p.predicted_rating
        END AS subreddit_rating
        , c.curator_rating
        , t.rating_short AS crowd_rating
        , p.predicted_rating
        , rating_prediction_score
        -- We assign the rating score as 1 (the maximum) if the curator or crowd label is available
        , CASE
            WHEN (c.curator_rating IS NOT NULL) OR (t.rating_short IS NOT NULL) THEN 1.0
            ELSE rating_prediction_score
        END AS rating_score

        -- Booleans to indicate source for TOPIC
        , IF(c.curator_topic IS NULL, 0, 1) AS curator_topic_tag
        , IF(t.primary_topic IS NULL, 0, 1) AS crowd_topic_tag
        , IF(p.predicted_topic IS NULL, 0, 1) AS model_topic_tag
        -- This is the order for deciding the "true" topic:
        --   curator > crowd > predicted
        -- ("true" because topics can be subjective AND not mutually exclusive)
        , CASE
            WHEN c.curator_topic IS NOT NULL THEN c.curator_topic
            WHEN t.primary_topic IS NOT NULL THEN t.primary_topic
            ELSE p.predicted_topic
        END AS subreddit_topic
        , c.curator_topic AS curator_topic
        , t.primary_topic AS crowd_topic
        , predicted_topic
        , topic_prediction_score
        -- We assign the topic score as 1 (the maximum) if the curator or crowd label is available
        , CASE
            WHEN (c.curator_topic IS NOT NULL) OR (t.primary_topic IS NOT NULL) THEN 1.0
            ELSE topic_prediction_score
        END AS topic_score

        , slo.subscribers

    FROM (
        SELECT
            subreddit_id
            , name
            , subscribers
        FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
        WHERE dt = (CURRENT_DATE() - 2)
            AND NOT REGEXP_CONTAINS(LOWER(name), r'^u_.*')
    ) AS slo
        LEFT JOIN `data-prod-165221.cnc.subreddit_metadata_lookup` AS t
            ON slo.subreddit_id = t.subreddit_id
        LEFT JOIN `reddit-employee-datasets.anna_scaramuzza.reddit_vault_predictions_20220411` AS p
            ON slo.subreddit_id = p.subreddit_id
        LEFT JOIN `reddit-employee-datasets.david_bermejo.taxonomy_curated_labels` AS c
            ON slo.subreddit_id = c.subreddit_id

    WHERE 1=1
        AND t.pt = CURRENT_DATE() - 2
)

SELECT *
FROM all_subreddit_labels
WHERE
    -- Only display subs that have at least one tag
    (
        curator_rating_tag + crowd_rating_tag + model_rating_tag
        + curator_topic_tag + crowd_topic_tag + model_topic_tag
    ) >= 1
ORDER BY subscribers DESC
;
