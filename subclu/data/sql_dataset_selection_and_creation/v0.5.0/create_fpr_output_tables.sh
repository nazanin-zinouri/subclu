# Create tables with FPR outputs so that we can create a dashboard to
#  review and share FPR outputs

bq load \
    --source_format=PARQUET \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_v0050_fpr_summary \
    "gs://i18n-subreddit-clustering/i18n_topic_model_batch/fpr/runs/2022-07-30_005122/df_fpr_qa_summary/*.parquet"


bq load \
    --source_format=PARQUET \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_v0050_fpr_outputs \
    "gs://i18n-subreddit-clustering/i18n_topic_model_batch/fpr/runs/2022-07-30_005122/df_fpr/*.parquet"

bq load \
    --source_format=PARQUET \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_v0050_fpr_dynamic_clusters \
    "gs://i18n-subreddit-clustering/i18n_topic_model_batch/fpr/runs/2022-07-30_005122/df_dynamic_clusters/*.parquet"

bq load \
    --source_format=PARQUET \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_v0050_fpr_cluster_summary \
    "gs://i18n-subreddit-clustering/i18n_topic_model_batch/fpr/runs/2022-07-30_005122/df_fpr_cluster_summary/*.parquet"
