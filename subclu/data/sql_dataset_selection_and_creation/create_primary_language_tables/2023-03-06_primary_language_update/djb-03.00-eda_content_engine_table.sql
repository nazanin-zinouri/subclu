SELECT
  JSON_EXTRACT_SCALAR(ml_model_prediction_scores, "$.language") language_code
  , JSON_EXTRACT_SCALAR(ml_model_prediction_scores, "$.score") score
  , JSON_EXTRACT(ml_model_prediction_scores, "$.score[0]") score_0
  , ml_model_prediction_scores
  , * EXCEPT(ml_model_prediction_scores)
FROM `data-prod-165221.fact_tables.content_engine_post_language_detection`
WHERE DATE(pt) = "2023-03-04"
LIMIT 1000
