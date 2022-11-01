-- ==============================
-- Export subreddits to google cloud storage (GCS)
-- CHANGE/Update:
--  1) URI date folder
--  2) source table
EXPORT DATA
    OPTIONS(
        uri=r'gs://${output_bucket_name}/i18n_topic_model_batch/runs/${run_id}/subreddits/text/*.parquet',
        format='PARQUET',
        overwrite=true
    ) AS
    SELECT
        sel.*
    FROM `reddit-relevance.${dataset}.subclu_subreddits_for_modeling_${run_id}` AS sel
    ORDER BY users_l7 DESC, posts_not_removed_l28 DESC
;


EXPORT DATA
    OPTIONS(
        uri=r'gs://${output_bucket_name}/i18n_topic_model_batch/runs/${run_id}/subreddits/geo_relevance_standardized/*.parquet',
        format='PARQUET',
        overwrite=true
    ) AS
    SELECT
        sel.*
    FROM `reddit-relevance.${dataset}.subclu_subreddit_geo_relevance_standardized_${run_id}` AS sel
    ORDER BY country_name ASC, subreddit_rank_in_country ASC
;
