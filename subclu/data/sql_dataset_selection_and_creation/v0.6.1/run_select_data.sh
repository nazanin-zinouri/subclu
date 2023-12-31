# Run select data locally (withouth kubeflow)
#  Use this script to test the __main___.py functions before integrating with kubeflow
#  append a project name to run using a different GCP project. DS-prod is usually much faster
# GOOGLE_CLOUD_PROJECT='data-science-prod-218515' bash run_select_data.sh

# Total Expected run time: ~49 minutes
#  Queries that take the longest:
#  - Raw relevance scores:  ~20 minutes (for 90 days)
#  - Get posts:             ~20 minutes (for 90 days)
#  - Get Comments:          ~ 3 minutes (for 90 days)

# make a call with only the required argument (which query to run)
# NOTE: order matters because some queries depend on other queries existing
# ETA: 6 seconds
python select_data/__main__.py --query-name "_0a1_countrycode_name_mapping.sql" --no-log-query

# ETA: 18 seconds to 1:15
python select_data/__main__.py --query-name "_0b1_select_candidate_subs.sql" --no-log-query

# ETA: 5 to 9 seconds
python select_data/__main__.py --query-name "_0b2_geo_relevance_baseline.sql" --no-log-query

# Example to run query with non-default values
#python select_data/__main__.py --query-name "_0b2_geo_relevance_baseline.sql" \
#  --run-id "20220406" \
#  --end-date "CURRENT_DATE() - 2" \
#  --no-log-query


# ETA: ~35 seconds early in the morning (no load)
#  This new query uses the pre-computed `community_score` table
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


# ETA: ~22 to 31 seconds
# Select subreddits (seeds) for topic model
python select_data/__main__.py --query-name "_01_select_subreddits.sql" --no-log-query

# Example to run query with non-default values
#python select_data/__main__.py --query-name "_01_select_subreddits.sql" \
#  --run-id "20220406" \
#  --end-date "CURRENT_DATE() - 2" \
#  --no-log-query

# Get posts
# ETA: ~3 minutes -> 28 days
#     ~3 to 9 minutes -> 90 days (~3 minutes after getting image & video labels from FACT table!)
python select_data/__main__.py --query-name "_02_select_posts.sql" --no-log-query


#python select_data/__main__.py --query-name "_02_select_posts.sql" --no-log-query \
#    --end-date "'2022-10-24'" \
#    --run-id "20221101_1650"

# Get comments
# ETA: ~2.5 minutes -> 28 days
#     ~ 3 minutes -> 90 days
python select_data/__main__.py --query-name "_03_select_comments.sql" --no-log-query

# Merge posts + comments into single text field
# ETA: ~1 to 2 minutes
python select_data/__main__.py --query-name "_04_combine_post_and_comment_text.sql" --no-log-query



# ==================
# Export tables to GCS as parquet files
# ===
# ETA: ~13 to 25 seconds
python select_data/__main__.py --query-name "_01a_export_subreddits_to_gcs.sql"

#python select_data/__main__.py --query-name "_01a_export_subreddits_to_gcs.sql" \
#  --output-bucket-name "i18n-subreddit-clustering"
#  --run-id "20221101_1650"
#  --end-date "CURRENT_DATE() - 2"


# Export posts (all subreddits)
#  ETA: ~1 minute
python select_data/__main__.py --query-name "_02a_export_posts_to_gcs.sql"

#python select_data/__main__.py --query-name "_02a_export_posts_to_gcs.sql" \
#  --output-bucket-name "i18n-subreddit-clustering"
#  --run-id "20221101_1650"


# Export comments (all subreddits)
#  ETA: ~30 seconds
python select_data/__main__.py --query-name "_03a_export_comments_to_gcs.sql"

#python select_data/__main__.py --query-name "_03a_export_comments_to_gcs.sql" \
#  --output-bucket-name "i18n-subreddit-clustering"
#  --run-id "20221101_1650"


# Export posts + comments  -- seed & non-seed subreddits to diff folders
#  ETA: ~1 minute
python select_data/__main__.py --query-name "_04a_export_combined_text.sql"

#python select_data/__main__.py --query-name "_04a_export_combined_text.sql" \
#  --output-bucket-name "i18n-subreddit-clustering"
#  --run-id "20221101_1650"
