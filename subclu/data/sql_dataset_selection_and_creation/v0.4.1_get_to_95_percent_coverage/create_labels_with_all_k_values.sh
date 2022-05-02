# Create a table with all values of k b/c the "optimal" values might be too sparse
#  Example: we need something between 118 and 320 to get split for:
#   nhl, nba, soccer, formula1

bq load \
    --source_format=PARQUET \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_v0041_subreddit_clusters_c_a_full \
    gs://i18n-subreddit-clustering/mlflow/mlruns/25/e37b0a2c3af54c588818e7efdde15df5/artifacts/df_labels/df_labels.parquet


