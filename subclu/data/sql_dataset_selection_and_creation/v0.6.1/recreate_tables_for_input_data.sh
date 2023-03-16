# The original tables were in relevance.tmp, so they've been deleted
#  Use these to recreate the tables based on the saved parquet files in GCS

# ==================
# Subreddit level
# ===
bq load \
    --source_format=PARQUET \
    --replace \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_subreddits_for_modeling_20221107 \
    'gs://i18n-subreddit-clustering/i18n_topic_model_batch/runs/20221107/subreddits_fix/text/0*.parquet'


# ==================
# Post & Comment level
# ===
bq load \
    --source_format=PARQUET \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_comments_for_modeling_20221107 \
    'gs://i18n-subreddit-clustering/i18n_topic_model_batch/runs/20221107/comments/*.parquet'


bq load \
    --source_format=PARQUET \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_posts_for_modeling_20221107 \
    'gs://i18n-subreddit-clustering/i18n_topic_model_batch/runs/20221107/posts/*.parquet'


# Sometimes can get error loading long text column, so exclude text column by listing the
#  columns to load:
#  subreddit_id:STRING,subreddit_name:STRING,post_id:STRING,net_upvotes_lookup:INTEGER,comment_for_embedding_count:INTEGER,post_and_comment_text_clean_len:INTEGER
bq load \
    --source_format=PARQUET \
    --replace \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_post_and_comment_text_combined_20221107 \
    'gs://i18n-subreddit-clustering/i18n_topic_model_batch/runs/20221107/post_and_comment_text_combined/text_all/0*.parquet'
