# posts & comments metadata
bq load \
    --source_format=PARQUET \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_subreddit_distance_v0032_c_posts_and_comments_and_meta \
    "gs://i18n-subreddit-clustering/data/models/clustering/manual_2021-08-11_04_45/df_sub_level_agg_c_similarity_pair-2021-08-13_021828-14186522_by_18.parquet"


#-- ==============================
#-- Cluster tables
#-- ===
bq load \
    --source_format=PARQUET \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_subreddit_cluster_labels_v032_a \
    "gs://i18n-subreddit-clustering/data/models/clustering/manual_2021-08-11_04_45/df_subs_only-meta_and_multiple_clustering_algos-2021-08-13_022453-3767_by_58.parquet"
