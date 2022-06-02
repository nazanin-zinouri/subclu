bq load \
    --source_format=PARQUET \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_posts_top_no_geo_20211214 \
    gs://i18n-subreddit-clustering/posts/top/2021-12-14_fix/*.parquet

# Note that zsh requires quotes around the path with *
