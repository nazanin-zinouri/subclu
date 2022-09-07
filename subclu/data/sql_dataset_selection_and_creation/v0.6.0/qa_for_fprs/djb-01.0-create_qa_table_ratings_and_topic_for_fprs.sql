-- CREATE TABLE OR UPDATE query to apply QA to filter out subreddits for FPRs (add latest partition date)
-- Each dt in this table is meant to include all subreddits that have
--  a rating or topic label (curator, crowd, or predicted)
--  OR 1+ visits, posts, comments recently

-- Update: This table now uses a snapshot of curator labels!

DECLARE PARTITION_DATE DATE DEFAULT (CURRENT_DATE() - 2);

-- Define sensitive topics (actual & predicted) to filter out
DECLARE SENSITIVE_TOPICS DEFAULT [
    'Addiction Support'
    , 'Activism'
    , 'Culture, Race, and Ethnicity'
    , 'Fitness and Nutrition'
    , 'Gender'
    , 'Mature Themes and Adult Content'
    , 'Medical and Mental Health'
    , 'Military'
    , "Men's Health"
    , 'Politics'
    , "Religion and Spirituality"
    , 'Sexual Orientation'
    , 'Trauma Support'
    , "Women's Health"
];


-- Delete data from partition, if it exists
DELETE
    `reddit-employee-datasets.david_bermejo.subreddit_qa_flags`
WHERE
    pt = PARTITION_DATE
;

-- Create table (IF NOT EXISTS) or REPLACE
-- CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subreddit_qa_flags`
-- PARTITION BY pt
-- AS (

-- Insert latest partition
INSERT INTO `reddit-employee-datasets.david_bermejo.subreddit_qa_flags`
(

WITH
rating_and_topic_curator_and_crowd AS (
    -- If available, pick the override, otherwise fill with crowdsourced label
    -- NOTE: It will return some subs even if they have no topic or rating
    SELECT
        o.date_retrieved AS dt_curator
        , o.blocklist_dt AS dt_blocklist
        , blocklist_status
        , blocklist_reason
        , c.whitelist_status
        , COALESCE(o.subreddit_id, c.subreddit_id) AS subreddit_id
        , COALESCE(o.curator_topic, c.primary_topic) AS primary_topic
        , COALESCE(o.curator_rating, c.rating_short) AS rating_short
        , o.curator_topic
        , o.curator_rating
        , c.primary_topic AS crowd_topic
        , c.rating_short AS crowd_rating
        , (o.curator_topic IS NOT NULL) AS topic_by_curator
        , (o.curator_rating IS NOT NULL) AS rating_by_curator
        , CASE
            WHEN (o.curator_topic IS NULL) OR (c.primary_topic IS NULL) THEN NULL
            ELSE (o.curator_topic = c.primary_topic)
        END AS topic_crowd_curator_agree
        , CASE
            WHEN (o.curator_rating IS NULL) OR (c.rating_name IS NULL) THEN NULL
            ELSE (o.curator_rating = c.rating_short)
        END AS rating_crowd_curator_agree
    FROM (
        SELECT *
        FROM `data-prod-165221.cnc.subreddit_metadata_lookup`
        WHERE pt = PARTITION_DATE
    ) AS c
        FULL OUTER JOIN `reddit-employee-datasets.david_bermejo.taxonomy_curated_labels` AS o
            ON c.subreddit_id = o.subreddit_id
)
, base_filters AS (
    -- Use subquery so we don't have to define detailed & generic filter twice
    SELECT
        *
        , SPLIT(taxonomy_filter_detail, '-')[SAFE_OFFSET(0)] AS taxonomy_filter
        , SPLIT(taxonomy_filter_detail, '-')[SAFE_OFFSET(1)] AS taxonomy_filter_reason
        , SPLIT(predictions_filter_detail, '-')[SAFE_OFFSET(0)] AS predictions_filter
        , SPLIT(predictions_filter_detail, '-')[SAFE_OFFSET(1)] AS predictions_filter_reason

    FROM (
        SELECT
            nt.dt_curator
            , nt.dt_blocklist
            , s.subreddit_id
            , blocklist_status
            , blocklist_reason
            , whitelist_status
            , s.over_18
            , s.allow_discovery

            , s.subreddit_name

            , nt.rating_short
            , nt.curator_rating
            , nt.crowd_rating
            , vr.predicted_rating

            , nt.primary_topic
            , nt.curator_topic
            , nt.crowd_topic
            , vt.predicted_topic

            , CASE
                WHEN (vt.predicted_topic IS NULL) AND (nt.primary_topic IS NULL) THEN 'missing-pred_and_label'
                WHEN (vt.predicted_topic IS NULL) THEN 'missing-pred'
                WHEN (nt.primary_topic IS NULL) THEN 'missing-label'
                WHEN (vt.predicted_topic = nt.primary_topic) THEN 'agree'
                ELSE 'disagree'
            END AS topic_taxn_model_agree
            , CASE
                WHEN (vr.predicted_rating IS NULL) AND (nt.rating_short IS NULL) THEN 'missing-pred_and_label'
                WHEN (vr.predicted_rating IS NULL) THEN 'missing-pred'
                WHEN (nt.rating_short IS NULL) THEN 'missing-label'
                WHEN (vr.predicted_rating = nt.rating_short) THEN 'agree'
                ELSE 'disagree'
            END AS rating_taxn_model_agree
            , nt.rating_crowd_curator_agree
            , nt.topic_crowd_curator_agree
            , COALESCE(nt.topic_by_curator, FALSE) AS topic_by_curator
            , COALESCE(nt.rating_by_curator, FALSE) AS rating_by_curator

            , asr.users_l7
            , asr.users_l28
            , asr.votes_l28
            , asr.comments_l28
            , asr.posts_l28
            , s.verdict
            , s.is_spam
            , s.is_deleted
            , s.deleted
            , s.quarantine

            , m.k_0100_label_name
            , c.k_1000_majority_primary_topic AS k_1000_majority_topic
            , c.k_1000_label

            -- Flag based on NSFW clusters
            , IF(m.k_1000_label_recommend = 'no', 'remove', NULL) sensitive_cluster_filter
            -- Flag based on actual ratings & primary topics
            --  We need to be careful with some COALESCE() statements
            , CASE
                WHEN (COALESCE(nt.rating_short, '') = 'E') AND (nt.primary_topic NOT IN UNNEST(SENSITIVE_TOPICS)) THEN 'recommend'
                WHEN (nt.rating_short != 'E') AND (nt.primary_topic IN UNNEST(SENSITIVE_TOPICS)) THEN 'remove-rating_and_topic'
                WHEN (nt.rating_short != 'E') THEN 'remove-rating'
                WHEN (nt.primary_topic IN UNNEST(SENSITIVE_TOPICS)) THEN 'remove-topic'
                WHEN (nt.rating_short IS NULL) AND (nt.primary_topic IS NULL) THEN 'missing-rating_and_topic'
                WHEN nt.primary_topic IS NULL THEN 'missing-topic'
                WHEN nt.rating_short IS NULL THEN 'missing-rating'
                -- NOTE: The over_18 flag comes from mods, so treat it separately from topic & rating
                ELSE NULL
            END AS taxonomy_filter_detail

            -- Flag based on PREDICTED ratings & topics
            , CASE
                WHEN (COALESCE(vr.predicted_rating, '') = 'E') AND (vt.predicted_topic NOT IN UNNEST(SENSITIVE_TOPICS)) THEN 'recommend'
                WHEN (vr.predicted_rating != 'E') AND (vt.predicted_topic IN UNNEST(SENSITIVE_TOPICS)) THEN 'remove-rating_and_topic'
                WHEN (vr.predicted_rating != 'E') THEN 'remove-rating'
                WHEN (vt.predicted_topic IN UNNEST(SENSITIVE_TOPICS)) THEN 'remove-topic'
                -- Review if predictions are null or below threshold
                WHEN (vr.predicted_rating IS NULL) AND (vt.predicted_topic IS NULL) THEN 'review-missing_pred_rating_and_topic'
                WHEN vt.predicted_topic IS NULL THEN 'review-missing_pred_topic'
                WHEN vr.predicted_rating IS NULL THEN 'review-missing_pred_rating'

                -- NOTE: The over_18 flag comes from mods, so treat it separately from topic & rating
                ELSE NULL
            END AS predictions_filter_detail

        FROM (
            SELECT
                LOWER(name) AS subreddit_name
                , subreddit_id, over_18, allow_discovery
                , verdict, is_spam, is_deleted, deleted
                , type, quarantine
            FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
            WHERE dt = PARTITION_DATE
                AND NOT REGEXP_CONTAINS(LOWER(name), r'^u_.*')
        ) AS s
            -- Get subreddit activity so we can prioritize larger/more active subs
            LEFT JOIN (
                SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
                WHERE DATE(pt) = PARTITION_DATE
            ) AS asr
                ON s.subreddit_name = LOWER(asr.subreddit_name)

            LEFT JOIN rating_and_topic_curator_and_crowd AS nt
                ON s.subreddit_id = nt.subreddit_id

            -- Add clusters human-label clusters to remove sensitive clusters
            LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0050_subreddit_clusters_c_full` AS c
                ON s.subreddit_id = c.subreddit_id
            LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0050_subreddit_clusters_c_manual_names` m
                ON c.k_1000_label = m.k_1000_label

            -- We join rating & topic separately because the predictions are indepentent
            LEFT JOIN (
                SELECT subreddit_id, predicted_topic, topic_prediction_score
                FROM `reddit-employee-datasets.anna_scaramuzza.reddit_vault_predictions_20220411`
                WHERE topic_prediction_score >= 0.5
            ) AS vt
                ON s.subreddit_id = vt.subreddit_id
            LEFT JOIN (
                SELECT subreddit_id, predicted_rating, rating_prediction_score
                FROM `reddit-employee-datasets.anna_scaramuzza.reddit_vault_predictions_20220411`
                WHERE rating_prediction_score >= 0.5
            ) AS vr
                ON s.subreddit_id = vr.subreddit_id

        WHERE 1=1
            AND COALESCE(s.type, '') IN ('public', 'private', 'restricted')

            -- Only subreddits that match at least one:
            --   - have a prediction (rating|topic)
            --   - have a label (crowd or curator rating|topic)
            --   - have had some recent activity
            AND (
                vt.subreddit_id IS NOT NULL
                OR vr.subreddit_id IS NOT NULL
                OR (
                    nt.primary_topic IS NOT NULL
                    OR nt.rating_short IS NOT NULL
                )

                OR COALESCE(asr.users_l7, 0) >= 1
                OR COALESCE(asr.comments_l28, 0) >= 1
                OR COALESCE(asr.posts_l28, 0) >= 1
            )
    )
)
, combined_filters AS (
    -- Again, use subquery to reduce the need to define case statements twice
    SELECT
        PARTITION_DATE AS pt
        , dt_curator
        , dt_blocklist
        , subreddit_id
        , over_18
        , whitelist_status
        , blocklist_status
        , blocklist_reason
        , users_l7
        , subreddit_name

        , primary_topic
        , curator_topic
        , crowd_topic
        , predicted_topic

        , rating_short
        , curator_rating
        , crowd_rating
        , predicted_rating

        , CASE
            WHEN
                (combined_filter_reason IN ("sensitive_cluster", "allow_discovery_f"))
                AND (taxonomy_filter = "missing")
                THEN REGEXP_REPLACE(taxonomy_filter_detail, '-', '_')
            WHEN
                (combined_filter_reason IN ("sensitive_cluster", "allow_discovery_f"))
                AND (taxonomy_filter = "recommend")
                AND (predictions_filter = "remove")
                THEN CONCAT("review_", predictions_filter_reason)
            WHEN combined_filter_reason IN (
                'sensitive_cluster', 'over_18', 'allow_discovery_f'
                , 'taxonomy_and_preds_clean'
                , 'rating', 'topic', 'rating_and_topic'
                , 'missing_pred_rating_and_topic'
                , 'missing_pred_topic'
                , 'missing_pred_rating'
                , 'spam_banned_or_deleted'
                , 'gaming_override', 'sports_override'
            ) THEN NULL
            ELSE combined_filter_reason
        END AS taxonomy_action
        , combined_filter_detail
        , taxonomy_filter_detail
        , predictions_filter_detail
        , verdict
        , quarantine
        , allow_discovery
        , sensitive_cluster_filter
        , combined_filter
        , combined_filter_reason
        , topic_taxn_model_agree
        , rating_taxn_model_agree

        , * EXCEPT (
            deleted
            , is_deleted
            , is_spam

            , dt_curator
            , dt_blocklist
            , whitelist_status
            , blocklist_status
            , blocklist_reason
            , subreddit_id
            , over_18

            , topic_taxn_model_agree
            , rating_taxn_model_agree
            , users_l7
            , subreddit_name
            , primary_topic
            , curator_topic
            , predicted_topic
            , crowd_topic
            , rating_short
            , curator_rating
            , crowd_rating
            , predicted_rating

            , allow_discovery
            , verdict
            , quarantine
            , combined_filter_detail
            , combined_filter
            , combined_filter_reason
            , taxonomy_filter_detail
            , predictions_filter_detail
            , sensitive_cluster_filter
        )

    FROM (
        SELECT
            *
            , SPLIT(combined_filter_detail, '-')[SAFE_OFFSET(0)] AS combined_filter
            , SPLIT(combined_filter_detail, '-')[SAFE_OFFSET(1)] AS combined_filter_reason
        FROM (
            SELECT
                *
                -- NOTE that order of filters can make a big difference!
                --   Remove over_18 & sensitive clusters first
                , CASE
                    WHEN (COALESCE(over_18, '') = 't') THEN 'remove-over_18'

                    -- For sensitive cluster subs, we always want to remove them
                    --   BUT we still might want to flag them for taxonomy team to review
                    WHEN (sensitive_cluster_filter = 'remove') THEN 'remove-sensitive_cluster'

                    -- Exclude subs that have been marked as spam or removed
                    WHEN (
                        COALESCE(verdict, '') = 'admin-removed'
                        OR COALESCE(is_spam, FALSE) != FALSE
                        OR COALESCE(is_deleted, FALSE) != FALSE
                        OR deleted IS NOT NULL
                        OR COALESCE(quarantine, FALSE) != FALSE
                    ) THEN 'remove-spam_banned_or_deleted'

                    -- Exclude geo-subreddits that don't want to be discovered
                    --  NOTE: We can use them as seeds, but we can't use them as recommendations so we don't need to rate them
                    WHEN COALESCE(allow_discovery, '') = 'f' THEN 'remove-allow_discovery_f'

                    -- Only RECOMMEND if cleared by BOTH taxonomy & predictions
                    WHEN (taxonomy_filter_detail = 'recommend') AND (predictions_filter_detail = 'recommend') THEN 'recommend-taxonomy_and_preds_clean'

                    -- REVIEW if predictions are misssing or not above threshold
                    --  Can't recommend w/o agreement
                    WHEN (taxonomy_filter_detail = 'recommend') AND (predictions_filter = 'review')
                        THEN CONCAT('review_model', COALESCE(CONCAT('-', predictions_filter_reason), ''))

                    -- Apply some overrides
                    -- Subs like r/india & r/mexico get a "culture" topic, flag them for review so we can at least
                    --  use them as seeds
                    WHEN (
                        (taxonomy_filter_detail = 'remove-topic')
                        AND (primary_topic IN ('Culture, Race, and Ethnicity'))
                        AND (rating_short = 'E')
                        AND (COALESCE(predictions_filter_detail, 'recommend') = 'recommend')
                    ) THEN 'review-review_topic'

                    -- Gaming & History subs sometimes get an "M" predicted rating (incorrectly)
                    WHEN (
                        (taxonomy_filter_detail = 'recommend')
                        AND (predictions_filter_detail = 'remove-rating')
                        AND (primary_topic IN ('Gaming', 'History'))
                        AND (predicted_rating = 'M')
                    ) THEN 'recommend-gaming_override'

                    -- Some sports subreddits mis-labeled as "fitness & nutrition"
                    WHEN (
                        (taxonomy_filter_detail = 'recommend')
                        AND (predictions_filter_detail = 'remove-topic')
                        AND (primary_topic = 'Sports')
                        AND (predicted_topic = 'Fitness and Nutrition')
                        AND (predicted_rating = 'E')
                    ) THEN 'recommend-sports_override'

                    -- Careers in medicine that get labeled as "medical"
                    WHEN (
                        (taxonomy_filter_detail = 'recommend')
                        AND (predictions_filter_detail = 'remove-topic')
                        AND (primary_topic = 'Careers')
                        AND (predicted_topic = 'Medical and Mental Health')
                        AND (predicted_rating = 'E')
                    ) THEN 'recommend-review_topic'

                    -- Remove or review based on prediction
                    WHEN (taxonomy_filter_detail = 'recommend') AND (predictions_filter_detail = 'remove-rating') THEN 'review-review_rating'
                    WHEN (taxonomy_filter_detail = 'recommend') AND (predictions_filter_detail = 'remove-topic') THEN 'remove-review_topic'
                    WHEN (taxonomy_filter_detail = 'recommend') AND (predictions_filter_detail = 'remove-rating_and_topic') THEN 'remove-review_rating_and_topic'

                    -- Review & flag missing topic/rating
                    WHEN (taxonomy_filter_detail = 'missing-rating') AND  (predictions_filter_detail = 'recommend') THEN 'review-missing_rating'
                    WHEN (taxonomy_filter_detail = 'missing-topic') AND   (predictions_filter_detail = 'recommend') THEN 'review-missing_topic'
                    WHEN (taxonomy_filter_detail = 'missing-rating_and_topic') AND (predictions_filter_detail = 'recommend') THEN 'review-missing_rating_and_topic'

                    -- Review & Flag missing topic/rating if prediction is to review
                    WHEN (taxonomy_filter_detail = 'missing-rating') AND  (predictions_filter != 'remove') THEN 'review-missing_rating'
                    WHEN (taxonomy_filter_detail = 'missing-topic') AND   (predictions_filter != 'remove') THEN 'review-missing_topic'
                    WHEN (taxonomy_filter_detail = 'missing-rating_and_topic') AND (predictions_filter != 'remove') THEN 'review-missing_rating_and_topic'

                    -- REMOVE & Flag missing topic/rating if prediction is to remove
                    WHEN (taxonomy_filter_detail = 'missing-rating') AND  (predictions_filter = 'remove') THEN 'remove-missing_rating'
                    WHEN (taxonomy_filter_detail = 'missing-topic') AND   (predictions_filter = 'remove') THEN 'remove-missing_topic'
                    WHEN (taxonomy_filter_detail = 'missing-rating_and_topic') AND (predictions_filter = 'remove') THEN 'remove-missing_rating_and_topic'

                    -- Apply remove cases last to allow overrides above
                    WHEN taxonomy_filter_detail IS NOT NULL THEN taxonomy_filter_detail

                    -- Remove by predicttions last & apply "pred_" prefix
                    WHEN predictions_filter_detail IS NOT NULL THEN REGEXP_REPLACE(predictions_filter_detail, "-", "-pred_")

                    ELSE NULL
                END AS combined_filter_detail

            FROM base_filters
        )
    )
    ORDER BY combined_filter_detail, k_1000_label DESC
)

SELECT * FROM combined_filters
);  -- close INSERT or CREATE table parens
