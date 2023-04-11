# This is the primary representation
#   Note that zsh requires quotes around the path with *
bq load \
    --source_format=PARQUET \
    --project_id=reddit-employee-datasets \
    david_bermejo.cau_subreddit_tsne_full \
    gs://i18n-subreddit-clustering/data/models/projections/manual_v061_2023-03-18_07_57/full/per=50exag=1.8lr=450init=spectralmet=cosinei_mom=0.7f_mom=0.95full-subreddits_781653-2023-03-19_06_03.parquet

# download file
gsutil cp "gs://i18n-subreddit-clustering/data/models/projections/manual_v061_2023-03-18_07_57/full/per=50|exag=1.8|lr=450|init=spectral|met=cosine|i_mom=0.7|f_mom=0.95||full-subreddits_781,653-2023-03-19_06_03.parquet"

# upload file:
gsutil cp "per=50,exag=1.8,lr=450,init=spectral,met=cosine,i_mom=0.7,f_mom=0.95,full-subreddits_781,653-2023-03-19_06_03.parquet" "gs://i18n-subreddit-clustering/data/models/projections/manual_v061_2023-03-18_07_57/full/"
gsutil cp "per=50exag=1.8lr=450init=spectralmet=cosinei_mom=0.7f_mom=0.95full-subreddits_781653-2023-03-19_06_03.parquet" "gs://i18n-subreddit-clustering/data/models/projections/manual_v061_2023-03-18_07_57/full/"


# this is an alternate projection with SVD:
