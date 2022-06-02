bq load \
    --source_format=PARQUET \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_v0041_subreddit_distances_c_top_100 \
    "gs://i18n-subreddit-clustering/data/models/nearest_neighbors/manual_model_2022-03-28_191331/df_nearest_neighbors_top-4906242_by_7.parquet"

# Note that zsh requires quotes around the path with *

