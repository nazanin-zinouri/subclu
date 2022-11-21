-- Export subreddits to google cloud storage (GCS)
-- Created a new folder "subreddit_fix" instead of "subreddit" to flag as fix
--  w/o overwriting existing data
EXPORT DATA
    OPTIONS(
        uri=r'gs://i18n-subreddit-clustering/i18n_topic_model_batch/runs/20221107/subreddits_fix/text/*.parquet',
        format='PARQUET',
        overwrite=false
    ) AS
    SELECT
        sel.*
    FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_for_modeling_20221107_fix_topic` AS sel
    ORDER BY users_l7 DESC, posts_not_removed_l28 DESC
;
