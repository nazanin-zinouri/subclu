-- ==============================
-- Export COMMENT text + metadata google cloud storage (GCS)
-- CHANGE/Update:
--  1) URI run (date) folder
--  2) source table

EXPORT DATA
    OPTIONS(
        uri=r'gs://${output_bucket_name}/i18n_topic_model_batch/runs/${run_id}/comments/*.parquet',
        format='PARQUET',
        overwrite=true
    ) AS
    SELECT *

    FROM `reddit-relevance.${dataset}.subclu_comments_for_modeling_${run_id}` AS t

    -- Order by subreddit_id & post_id because it should randomize subreddits & short & long posts
    --  For vectorizing we want to prevent all the long posts next to each other
    ORDER BY subreddit_id, post_id, comment_id
;
