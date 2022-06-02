bq load \
    --source_format=PARQUET \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_v0041_subreddit_tsne1 \
    "gs://i18n-subreddit-clustering/data/models/clustering/manual_v041_2022-03-02_21_09/manual_v041_2022-03-02_21_09/df_emb_svd2_meta-49558_by_186.parquet"

# Note that zsh requires quotes around the path with *
