# Run select data locally (withouth kubeflow)
#  Use this script to test the __main___.py functions before integrating with kubeflow
#  append a project name to run using a different GCP project. DS-prod is usually much faster
# GOOGLE_CLOUD_PROJECT='data-science-prod-218515'

# make a call with only the required argument (which query to run)
# NOTE: order matters because some queries depend on other queries existing
# ETA: 6 seconds
python select_data/__main__.py --query-name "_0a1_countrycode_name_mapping.sql" --no-log-query

# ETA: n/a
#  This one doesn't work from laptop because it needs google drive credentials
#  For now need to manually run it in BigQuery UI
# python select_data/__main__.py --query-name "_0a2_all_ambasssador_subreddits.sql"

# ETA: 18 seconds
python select_data/__main__.py --query-name "_0b1_select_candidate_subs.sql" --no-log-query

# ETA: 5 to 9 seconds
python select_data/__main__.py --query-name "_0b2_geo_relevance_baseline.sql" --no-log-query

# Example to run query with non-default values
#python select_data/__main__.py --query-name "_0b2_geo_relevance_baseline.sql" \
#  --run-id "20220406" \
#  --end-date "CURRENT_DATE() - 2" \
#  --no-log-query


# ETA: 3 minutes to 20 MINUTES (depends on load & slots)
# It seems that create table job finishes in ~5 minutes (I can see the table and query it in the web UI)
#  but for some reason the bigquery-python client can take more than 13 minutes to close the connection.
python select_data/__main__.py --query-name "_0b3_subreddit_relevance_raw.sql" --no-log-query


# ETA: ~35 seconds early in the morning (no load)
#  ~20 seconds on BQ UI early in the morning
python select_data/__main__.py --query-name "_0b4_subreddit_relevance_standardized.sql" --no-log-query

# Example to run query with non-default values
#python select_data/__main__.py --query-name "_0b4_subreddit_relevance_standardized.sql" \
#  --run-id "20220406" \
#  --end-date "CURRENT_DATE() - 2" \
#  --no-log-query


# ETA: ~35 seconds
python select_data/__main__.py --query-name "_0c1_select_geo_subreddits_for_modeling.sql" --no-log-query

# Example to run query with non-default values
#python select_data/__main__.py --query-name "_0c1_select_geo_subreddits_for_modeling.sql" \
#  --run-id "20220406" \
#  --end-date "CURRENT_DATE() - 2" \
#  --no-log-query


# ETA: ~22 seconds
# Select subreddits (seeds) for topic model
python select_data/__main__.py --query-name "_01_select_subreddits.sql" --no-log-query

# Example to run query with non-default values
#python select_data/__main__.py --query-name "_01_select_subreddits.sql" \
#  --run-id "20220406" \
#  --end-date "CURRENT_DATE() - 2" \
#  --no-log-query

# Get posts
# ETA: ~11 minutes -> 28 days
#     ~20 minutes -> 90 days
python select_data/__main__.py --query-name "_02_select_posts.sql" --no-log-query

# Get comments
# ETA: ~2.5 minutes -> 28 days
#     ~ 3 minutes -> 90 days
python select_data/__main__.py --query-name "_03_select_comments.sql" --no-log-query

# Merge posts + comments into single text field
# ETA: ~1 minute
python select_data/__main__.py --query-name "_04_combine_post_and_comment_text.sql" --no-log-query


# ==================
# Export tables to GCS as parquet files
# ===
# ETA: ~13 to 25 seconds
#python select_data/__main__.py --query-name "_01a_export_subreddits_to_gcs.sql"

python select_data/__main__.py --query-name "_01a_export_subreddits_to_gcs.sql" \
  --output-bucket-name "i18n-subreddit-clustering"
#  --run-id "20220406" \
#  --end-date "CURRENT_DATE() - 2"


# Export posts (all subreddits)
#  ETA: ~1 minute
#python select_data/__main__.py --query-name "_02a_export_posts_to_gcs.sql"

python select_data/__main__.py --query-name "_02a_export_posts_to_gcs.sql" \
  --output-bucket-name "i18n-subreddit-clustering"
#  --run-id "20220707" \

# Export comments (all subreddits)
#  ETA: ~30 seconds
#python select_data/__main__.py --query-name "_03a_export_comments_to_gcs.sql"

python select_data/__main__.py --query-name "_03a_export_comments_to_gcs.sql" \
  --output-bucket-name "i18n-subreddit-clustering"
#  --run-id "20220707" \

# Export posts + comments  -- seed & non-seed subreddits to diff folders
#  ETA: ~1 minute
#python select_data/__main__.py --query-name "_04a_export_combined_text.sql"

python select_data/__main__.py --query-name "_04a_export_combined_text.sql" \
  --output-bucket-name "i18n-subreddit-clustering"
#  --run-id "20220707" \
