# The original tables were in relevance.tmp, so they've been deleted
#  Use these to recreate the tables based on the saved parquet files in GCS

bq load \
    --source_format=PARQUET \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_comments_for_modeling_20220707 \
    'gs://i18n-subreddit-clustering/i18n_topic_model_batch/runs/20220707/comments/*.parquet'


# Error loading long text, so exclude text column
bq load \
    --source_format=PARQUET \
    --replace \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_post_and_comment_text_combined_20220629 \
    'gs://i18n-subreddit-clustering/i18n_topic_model_batch/runs/20220629/post_and_comment_text_combined/text_subreddit_seeds/*.parquet','gs://i18n-subreddit-clustering/i18n_topic_model_batch/runs/20220629/post_and_comment_text_combined/text_non_subreddit_seeds/*.parquet' \
    subreddit_id:STRING,subreddit_name:STRING,post_id:STRING,net_upvotes_lookup:INTEGER,comment_for_embedding_count:INTEGER,post_and_comment_text_clean_len:INTEGER


bq load \
    --source_format=PARQUET \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_posts_for_modeling_20220707 \
    'gs://i18n-subreddit-clustering/i18n_topic_model_batch/runs/20220707/posts/*.parquet'


bq load \
    --source_format=PARQUET \
    --replace \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_subreddits_for_modeling_20220629 \
    'gs://i18n-subreddit-clustering/i18n_topic_model_batch/runs/20220629/subreddits/text/0*0.parquet'

bq load \
    --source_format=PARQUET \
    --project_id=reddit-employee-datasets \
    david_bermejo.subclu_subreddits_for_modeling_20220707 \
    'gs://i18n-subreddit-clustering/i18n_topic_model_batch/runs/20220707/subreddits/text/*.parquet'
