-- Get all label sources for a subreddit
-- NOTES
-- - as of 2022-11-10
--   - the curator (taxonomy) labels are updated manually (not automated yet)
--   - the v6 rating is out with M1 & M2 replacing M (mature)

-- Predictions below these thresholds will be taken as null/unlabeled
--   For the i18n QA process we only want to keep predictions over the thresholds where the models
--   are confident enough.
-- DECLARE PRED_RATING_THRESHOLD NUMERIC DEFAULT 0.5;
-- DECLARE PRED_TOPIC_THRESHOLD NUMERIC DEFAULT 0.5;


-- CREATE VIEW `reddit-employee-datasets.david_bermejo.reddit_vault_predictions_and_overrides_vw`
-- OPTIONS(
--     description="View that combines all sources for topic & rating labels: curator, crowd, & CA model. Wiki: https://reddit.atlassian.net/wiki/spaces/DataScience/pages/2389278980/Content+Analytics+Key+Tables"
-- )
-- AS (



WITH all_subreddit_labels AS (
    SELECT
        (CURRENT_DATE() - 2) AS dt
        , slo.subreddit_id
        , LOWER(name) AS subreddit_name
        -- , DATE(verification_timestamp) AS crowd_verification_dt
        , c.date_retrieved AS curator_dt

        -- Booleans to indicate source for RATING
        , IF(c.curator_rating IS NULL, 0, 1) AS curator_rating_tag
        , IF(t.rating_short IS NULL, 0, 1) AS crowd_rating_tag
        , IF(pr.rating_1 IS NULL, 0, 1) AS model_rating_tag
        -- When deciding the subreddit rating we take the first available rating in this order:
        --  curator > crowd > predicted
        , CASE
            WHEN c.curator_rating_short IS NOT NULL THEN c.curator_rating_short
            WHEN t.rating_short IS NOT NULL THEN t.rating_short
            ELSE pr.rating_1
        END AS subreddit_rating
        -- Taxonomy rating -> curated or crowd. Use it as labels for modeling
        , CASE
            WHEN c.curator_rating_short IS NOT NULL THEN c.curator_rating_short
            WHEN t.rating_short IS NOT NULL THEN t.rating_short
            ELSE NULL
        END AS taxonomy_rating
        , CASE
            WHEN c.curator_rating_name IS NOT NULL THEN c.curator_rating_name
            WHEN t.rating_name IS NOT NULL THEN t.rating_name
            ELSE NULL
        END AS taxonomy_rating_name
        , c.curator_rating_name
        , c.curator_rating_short AS curator_rating
        , t.rating_short AS crowd_rating
        , pr.rating_1 AS predicted_rating
        , pr.rating_score_1 AS rating_prediction_score
        -- We assign the rating score as 1 (the maximum) if the curator or crowd label is available
        , CASE
            WHEN (c.curator_rating IS NOT NULL) OR (t.rating_short IS NOT NULL) THEN 1.0
            ELSE pr.rating_score_1
        END AS rating_score

        -- Booleans to indicate source for TOPIC
        , IF(c.curator_topic IS NULL, 0, 1) AS curator_topic_tag
        , IF(c.curator_topic_v2 IS NULL, 0, 1) AS curator_topic_v2_tag
        , IF(t.primary_topic IS NULL, 0, 1) AS crowd_topic_tag
        , IF(pt.topic_1 IS NULL, 0, 1) AS model_topic_tag
        -- This is the order for deciding the "true" topic:
        --   curator > crowd > predicted
        -- ("true" because topics can be subjective AND not mutually exclusive)
        , CASE
            WHEN c.curator_topic IS NOT NULL THEN c.curator_topic
            WHEN t.primary_topic IS NOT NULL THEN t.primary_topic
            ELSE pt.topic_1
        END AS subreddit_topic
        -- Taxonomy rating -> curated or crowd. Use it as labels for modeling
        , CASE
            WHEN c.curator_topic IS NOT NULL THEN c.curator_topic
            WHEN t.primary_topic IS NOT NULL THEN t.primary_topic
            ELSE NULL
        END AS taxonomy_topic
        , c.curator_topic AS curator_topic
        , c.curator_topic_v2
        , t.primary_topic AS crowd_topic
        , pt.topic_1 AS predicted_topic
        , pt.topic_score_1 AS topic_prediction_score
        -- We assign the topic score as 1 (the maximum) if the curator or crowd label is available
        , CASE
            WHEN (c.curator_topic IS NOT NULL) OR (t.primary_topic IS NOT NULL) THEN 1.0
            ELSE pt.topic_score_1
        END AS topic_score

        , t.survey_version
        , CASE
            WHEN (c.curator_topic IS NOT NULL) THEN 'curator'
            WHEN (t.primary_topic IS NOT NULL) THEN 'crowd'
            ELSE NULL
        END AS taxonomy_topic_source
        , CASE
            WHEN (c.curator_rating_short IS NOT NULL) THEN 'curator'
            WHEN (t.rating_short IS NOT NULL) THEN 'crowd'
            ELSE NULL
        END AS taxonomy_rating_source

        , (pr.rating_score_1 < 0.5) AS pred_rating_low_confidence
        , (pt.topic_score_1 < 0.5) AS pred_topic_low_confidence

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
        -- pull from .shredded_crowdsource_topic_and_rating because it seems to include more
        --  labels than .subreddit_metadata_lookup
        LEFT JOIN (
            SELECT *
            FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
            WHERE pt = CURRENT_DATE() - 2
                AND survey_version IN ('v3', 'v5')
        ) AS t
            ON slo.subreddit_id = t.subreddit_id
        LEFT JOIN `reddit-employee-datasets.anna_scaramuzza.reddit_vault_all_topics_inference_20220929` AS pt
            ON slo.subreddit_id = pt.subreddit_id
        LEFT JOIN `reddit-employee-datasets.anna_scaramuzza.reddit_vault_all_ratings_inference_20220929` AS pr
            ON slo.subreddit_id = pr.subreddit_id
        LEFT JOIN `reddit-employee-datasets.david_bermejo.taxonomy_curated_labels` AS c
            ON slo.subreddit_id = c.subreddit_id

    -- WHERE 1=1
)

SELECT
    *
    , CASE
        WHEN taxonomy_topic_source IS NULL THEN NULL
        ELSE (curator_topic = crowd_topic)
    END AS topic_crowd_curator_agree
    , CASE
        WHEN taxonomy_rating_source IS NULL THEN NULL
        ELSE (curator_rating = crowd_rating)
    END AS rating_crowd_curator_agree

    , CASE
        WHEN (predicted_rating IS NULL) AND (taxonomy_rating IS NULL) THEN 'missing-pred_and_label'
        WHEN (pred_rating_low_confidence = TRUE) THEN 'pred_confidence_low'
        WHEN (predicted_rating IS NULL) THEN 'missing-pred'
        WHEN (taxonomy_rating IS NULL) THEN 'missing-label'
        WHEN (predicted_rating = taxonomy_rating) THEN 'agree'
        ELSE 'disagree'
    END AS rating_taxn_model_agree
    , CASE
        WHEN (predicted_topic IS NULL) AND (taxonomy_topic IS NULL) THEN 'missing-pred_and_label'
        WHEN (pred_topic_low_confidence = TRUE) THEN 'pred_confidence_low'
        WHEN (predicted_topic IS NULL) THEN 'missing-pred'
        WHEN (taxonomy_topic IS NULL) THEN 'missing-label'
        WHEN (predicted_topic = taxonomy_topic) THEN 'agree'
        ELSE 'disagree'
    END AS topic_taxn_model_agree

FROM all_subreddit_labels
WHERE
    -- Only display subs that have at least one tag
    (
        curator_rating_tag + crowd_rating_tag + model_rating_tag
        + curator_topic_tag + crowd_topic_tag + model_topic_tag
        + curator_topic_v2_tag
    ) >= 1
ORDER BY subscribers DESC
;
