# NOTE: THIS IS DEPRECATED!!
#  use python library so that we can upload "field description" metadata
# See notebook:
# notebooks/v0.5.0/djb_08.00-2022-08-01-subreddit_level_ANNOY_for_nearest_neighbors_and_counterparts.ipynb

#bq load \
#    --source_format=PARQUET \
#    --project_id=reddit-employee-datasets \
#    david_bermejo.subclu_v0050_subreddit_distances_c_top_100 \
#    "gs://i18n-subreddit-clustering/data/models/nearest_neighbors/manual_model_2022-08-01_181632/df_nearest_neighbors_top_bigquery-8115327_by_8.parquet"

# Note that zsh requires quotes around the path with *
# Note that when we use parquet, for some reason we can't cluster ;_;
#  --clustering_fields="subreddit_id_a","subreddit_name_a" \
# Other flags:
#  Replace (overwrite) if data already exists in table
#  --replace

# Display the table's schema
bq show --format \
    prettyjson \
    --schema \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_v0050_subreddit_distances_c_top_100
