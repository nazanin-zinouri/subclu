-- Create new table fixing primary topic & rating (pull curator label if available)
CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddits_for_modeling_20221107_fix_topic` AS (
SELECT
  tx.survey_version
  , (tx.taxonomy_rating_name) AS rating_name
  , (tx.taxonomy_rating) AS rating_short
  , (tx.taxonomy_topic) AS primary_topic
  , sm.* EXCEPT(primary_topic, rating_short, rating_name)
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_for_modeling_20221107` AS sm
  LEFT JOIN `reddit-employee-datasets.david_bermejo.reddit_vault_predictions_and_overrides_vw` AS tx
    ON sm.subreddit_id = tx.subreddit_id
)
;
