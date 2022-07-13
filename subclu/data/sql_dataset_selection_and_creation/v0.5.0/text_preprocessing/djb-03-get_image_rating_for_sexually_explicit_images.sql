-- Get image rating from SWAT team's model
SELECT
  post_id,
  media_url,
  ml_model_name,
  ml_model_prediction_scores
  , CAST(JSON_EXTRACT(ml_model_prediction_scores, "$.SexuallyExplicit[0]") AS FLOAT64) sexually_explicit_probability
  , CASE
      WHEN
          COALESCE(
              CAST(JSON_EXTRACT(ml_model_prediction_scores, "$.SexuallyExplicit[0]") AS FLOAT64)
              , 0.0
          ) >= 0.9
          THEN "Sexually explicit image."
      ELSE ""
  END AS sexually_expicit_text
FROM
  `data-prod-165221.events_v2.analytics`
WHERE pt between "2022-04-25" and "2022-04-25"
  AND source = "content_classification"
  AND action = "record_response"
  AND media_type NOT IN ("video", "third_party_video")
  AND ml_model_name = "safety_media_x_model"
LIMIT 1000
;
