# Run pseudo dag to create primary language tables
# NOTE: order matters because some queries depend on other tables existing

# ETA: 6.5 minutes
#python run_sql_queries_from_python.py --query-name "create_primary_language_tables/_01_post_base_language_cld3_clean.sql" --no-log-query

# ETA: 5 minutes (actual)
#python run_sql_queries_from_python.py --query-name "create_primary_language_tables/_02_comments_base_language_cld3_clean.sql" --no-log-query

# ETA: 1 minute
#python run_sql_queries_from_python.py --query-name "create_primary_language_tables/_03_create_table_with_subreddit_language_ranks.sql" --no-log-query


# ETA: 1 minute
#python run_sql_queries_from_python.py --query-name "create_primary_language_tables/_04_create_table_primary_and_secondary_language.sql" --no-log-query
