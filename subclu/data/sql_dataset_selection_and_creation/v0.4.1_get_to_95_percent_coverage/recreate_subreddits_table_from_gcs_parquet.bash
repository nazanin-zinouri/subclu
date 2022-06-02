bq load \
    --source_format=PARQUET \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_subreddits_top_no_geo_20211214 \
    'gs://i18n-subreddit-clustering/subreddits/top/2021-12-14/*.parquet'
