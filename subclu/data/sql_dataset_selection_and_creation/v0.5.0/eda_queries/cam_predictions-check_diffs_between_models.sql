-- SELECT *
-- FROM `reddit-employee-datasets.anna_scaramuzza.reddit_vault_predictions_20220411`
-- WHERE subreddit_name IN (
--     'tifu', 'nextfuckinglevel', 'selfawarewolves'
--     , 'askmeover30', 'copypasta_es', 'monterrey'
-- )
-- LIMIT 1000


SELECT
    a.subreddit_name
    , a.predicted_topic
    , b.predicted_topic AS pred_b
    , a.topic_prediction_score
    , b.topic_prediction_score AS pred_score_b
FROM `reddit-employee-datasets.anna_scaramuzza.reddit_vault_predictions_20220411` a
    INNER JOIN `reddit-employee-datasets.anna_scaramuzza.reddit_vault_predictions_20220426` b
        ON a.subreddit_id=b.subreddit_id
            AND ROUND(a.topic_prediction_score, 4)=ROUND(b.topic_prediction_score, 4)
