-- ==================
-- Create tables based on template in:
--   transform_distance_data_for_bq.py
-- ==================
-- Current compression & manual labels. Later on want to do this from
--  mlflow jobs

-- To use these tables in Mode/Andrelytics, we need to grant access to the user groups listed in this wiki:
--  https://reddit.atlassian.net/wiki/spaces/DataScience/pages/2089844753/Creating+BigQuery+External+Tables+Using+them+in+Mode+Dashboards
--  https://reddit.atlassian.net/wiki/spaces/DE/pages/753991694/How+to+grant+access+to+a+Google+Sheet-backed+BigQuery+table


CREATE OR REPLACE EXTERNAL TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v031_a`
OPTIONS (
    format='PARQUET',
    uris=["gs://i18n-subreddit-clustering/data/models/clustering/manual_2021-07-20_15_13/df_subs_only-multiple_clustering_algos-628_by_58.parquet"]
)
;

CREATE OR REPLACE EXTERNAL TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddit_meta_and_svd_v031_a`
OPTIONS (
    format='PARQUET',
    uris=["gs://i18n-subreddit-clustering/data/models/clustering/manual_2021-07-20_15_13/df_subs_meta_and_svd-628_by_93.parquet"]
)
;

CREATE OR REPLACE EXTERNAL TABLE `reddit-employee-datasets.david_bermejo.subclu_posts_meta_and_svd_v031_a`
OPTIONS (
    format='PARQUET',
    uris=["gs://i18n-subreddit-clustering/data/models/clustering/manual_2021-07-20_15_13/df_posts_meta_and_svd-262226_by_72.parquet"]
)
;

