-- Get rough precision & recall per rating
-- Note: we need to pull latest ratings b/c these are ratings as of a fixed date
DECLARE RATING_THRESHOLD NUMERIC DEFAULT 0.45;
DECLARE TOPIC_THRESHOLD NUMERIC DEFAULT 0.35;

WITH
predicted_rating_agg AS (
    SELECT
        predicted_rating
        , COALESCE(COUNT(*), 0) AS total_predicted
    FROM `reddit-employee-datasets.anna_scaramuzza.reddit_vault_predictions_20220426`
    WHERE 1=1
        AND taxonomy_rating IS NOT NULL
        AND rating_prediction_score >= RATING_THRESHOLD
    GROUP BY 1
)
, rating_agg AS (
    SELECT
        taxonomy_rating
        , SUM(IF(taxonomy_rating = predicted_rating, 1, 0)) AS true_positives
        , SUM(IF(taxonomy_rating != predicted_rating, 1, 0)) AS false_positives
        , COUNT(*) AS positives
    FROM `reddit-employee-datasets.anna_scaramuzza.reddit_vault_predictions_20220426`
    WHERE 1=1
        AND taxonomy_rating IS NOT NULL
        AND rating_prediction_score >= RATING_THRESHOLD
    GROUP BY 1
)

SELECT
    taxonomy_rating
    , true_positives / r.positives AS recall
    , true_positives / p.total_predicted AS precision
    , COALESCE(p.total_predicted, 0) AS total_predicted
    , positives
FROM rating_agg AS r
    LEFT JOIN predicted_rating_agg AS p
        ON r.taxonomy_rating = p.predicted_rating
;



-- Get rough precision & recall per PRIMARY TOPIC
-- Note: we need to pull latest ratings b/c these are ratings as of a fixed date
WITH
predicted_topic_agg AS (
    SELECT
        predicted_topic
        , COALESCE(COUNT(*), 0) AS total_predicted
    FROM `reddit-employee-datasets.anna_scaramuzza.reddit_vault_predictions_20220426`
    WHERE 1=1
        AND taxonomy_topic IS NOT NULL
        AND topic_prediction_score >= TOPIC_THRESHOLD
    GROUP BY 1
)
, topic_agg AS (
    SELECT
        taxonomy_topic
        , SUM(IF(taxonomy_topic = predicted_topic, 1, 0)) AS true_positives
        , SUM(IF(taxonomy_topic != predicted_topic, 1, 0)) AS false_positives
        , COUNT(*) AS positives
    FROM `reddit-employee-datasets.anna_scaramuzza.reddit_vault_predictions_20220426`
    WHERE 1=1
        AND taxonomy_topic IS NOT NULL
        AND topic_prediction_score >= TOPIC_THRESHOLD
    GROUP BY 1
)

SELECT
    taxonomy_topic
    , true_positives / r.positives AS recall
    , true_positives / p.total_predicted AS precision
    , COALESCE(p.total_predicted, 0) AS total_predicted
    , positives
FROM topic_agg AS r
    LEFT JOIN predicted_topic_agg AS p
        ON r.taxonomy_topic = p.predicted_topic

ORDER BY precision DESC, positives DESC, recall DESC
;
