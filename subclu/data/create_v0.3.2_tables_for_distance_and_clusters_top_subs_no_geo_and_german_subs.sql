-- ==================
-- Create tables based on template in:
--   transform_distance_data_for_bq.py
-- Original date created: 2021-08-12
-- ==================
-- For this one I'm using mostly manual tables b/c the mlflow run didn't include metadata that is helpful:
--   example: primary post language & primary post-type
-- I did include the mlflow run UUID as a new column so it's easier to trace the lineage.


-- To use these tables in Mode/Andrelytics, we need to grant access to the user groups listed in this wiki:
--  https://reddit.atlassian.net/wiki/spaces/DataScience/pages/2089844753/Creating+BigQuery+External+Tables+Using+them+in+Mode+Dashboards
--  https://reddit.atlassian.net/wiki/spaces/DE/pages/753991694/How+to+grant+access+to+a+Google+Sheet-backed+BigQuery+table


-- ==============================
-- Distance tables
-- ===
-- A & B are not needed now, only upload them if necessary.
-- CREATE OR REPLACE EXTERNAL TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddit_distance_v0032_german_a_posts_only_a`
-- OPTIONS (
--     format='PARQUET',
--     uris=["gs://i18n-subreddit-clustering/mlflow/mlruns/8/cf3c6dfb599b414a812085659e62ec85/artifacts/df_sub_level_agg_a_post_only_similarity_pair/*.parquet"]
-- )
-- ;

-- CREATE OR REPLACE EXTERNAL TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddit_distance_v0032_german_b_posts_and_comments_a`
-- OPTIONS (
--     format='PARQUET',
--     uris=["gs://i18n-subreddit-clustering/mlflow/mlruns/8/cf3c6dfb599b414a812085659e62ec85/artifacts/df_sub_level_agg_b_post_and_comments_similarity_pair/*.parquet"]
-- )
-- ;

CREATE OR REPLACE EXTERNAL TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddit_distance_v0032_c_posts_and_comments_and_meta`
OPTIONS (
    format='PARQUET',
    uris=["gs://i18n-subreddit-clustering/data/models/clustering/manual_2021-08-11_04_45/df_sub_level_agg_c_similarity_pair-2021-08-13_021828-14186522_by_18.parquet"]
)
;

-- ==============================
-- Cluster tables
-- ===
CREATE OR REPLACE EXTERNAL TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v032_a`
OPTIONS (
    format='PARQUET',
    uris=["gs://i18n-subreddit-clustering/data/models/clustering/manual_2021-08-11_04_45/df_subs_only-meta_and_multiple_clustering_algos-2021-08-13_022453-3767_by_58.parquet"]
)
;

