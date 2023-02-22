-- ==============================
-- Export COMBINED TEXT to google cloud storage (GCS)
-- CHANGE/Update:
--  1) URI date folder
--  2) source table

-- Export all data to a single location
--  This way it's easier to run vectorization on all posts in a single job (one location)
EXPORT DATA
    OPTIONS(
        uri=r'gs://${output_bucket_name}/i18n_topic_model_batch/runs/${run_id}/post_and_comment_text_combined/text_all/*.parquet',
        format='PARQUET',
        overwrite=true
    ) AS
    SELECT
        sel.subreddit_seed_for_clusters
        , t.subreddit_id
        , t.subreddit_name
        , t.* EXCEPT(subreddit_id, subreddit_name)

    FROM `reddit-relevance.${dataset}.subclu_post_and_comment_text_combined_${run_id}` AS t
        LEFT JOIN `reddit-relevance.${dataset}.subclu_subreddits_for_modeling_${run_id}` AS sel
            ON sel.subreddit_id = t.subreddit_id

    -- Order by subreddit_id & post_id because it should randomize subreddits & short & long posts
    --  For vectorizing we want to prevent all the long posts next to each other
    ORDER BY subreddit_id, post_id
;

