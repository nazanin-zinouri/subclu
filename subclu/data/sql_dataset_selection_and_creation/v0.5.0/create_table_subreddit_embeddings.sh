# use bq load to load embeddings from GCS into a BQ table

bq load \
    --source_format=PARQUET \
    --replace \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_v0050_subreddit_embeddings \
    "gs://i18n-subreddit-clustering/mlflow/mlruns/29/bfe6cbd59a21480c8c2b9923a3a9cbd6/artifacts/df_subs_agg_c1/*.parquet"

# Note that zsh requires quotes around the path with *
# Note that when we use parquet, for some reason we can't cluster ;_;
# Other flags:
#  Replace (overwrite) if data already exists in table
#  --replace
