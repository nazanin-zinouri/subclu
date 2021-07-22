-- ==================
-- Create tables based on template in:
--   transform_distance_data_for_bq.py
-- ==================
-- This time I'm creating tables from mlflow runs, so the GCS paths are longer
-- but now it should be easier to trace the lineage of the data.
-- Example for paths:
--     - single file:
--         data/models/fse/manual_merge_2021-06-07_17/df_one_file.parquet
--     - files matching pattern:
--         data/models/fse/manual_merge_2021-06-07_17/df_*.parquet
--     - folder. NOTE: ALL FILES IN FOLDER MUST HAVE THE SAME COLUMNS/FORMAT
--         data/models/fse/manual_merge_2021-06-07_17/

-- To use these tables in Mode/Andrelytics, we need to grant access to the user groups listed in this wiki:
--  https://reddit.atlassian.net/wiki/spaces/DataScience/pages/2089844753/Creating+BigQuery+External+Tables+Using+them+in+Mode+Dashboards
--  https://reddit.atlassian.net/wiki/spaces/DE/pages/753991694/How+to+grant+access+to+a+Google+Sheet-backed+BigQuery+table


CREATE OR REPLACE EXTERNAL TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddit_distance_v0031_german_a_posts_only`
OPTIONS (
    format='PARQUET',
    uris=["gs://i18n-subreddit-clustering/mlflow/mlruns/8/cf3c6dfb599b414a812085659e62ec85/artifacts/df_sub_level_agg_a_post_only_similarity_pair/*.parquet"]
)
;

CREATE OR REPLACE EXTERNAL TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddit_distance_v0031_german_b_posts_and_comments`
OPTIONS (
    format='PARQUET',
    uris=["gs://i18n-subreddit-clustering/mlflow/mlruns/8/cf3c6dfb599b414a812085659e62ec85/artifacts/df_sub_level_agg_b_post_and_comments_similarity_pair/*.parquet"]
)
;

CREATE OR REPLACE EXTERNAL TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddit_distance_v0031_german_c_posts_and_comments_and_meta`
OPTIONS (
    format='PARQUET',
    uris=["gs://i18n-subreddit-clustering/mlflow/mlruns/8/cf3c6dfb599b414a812085659e62ec85/artifacts/df_sub_level_agg_c_post_comments_and_sub_desc_similarity_pair/*.parquet"]
)
;

