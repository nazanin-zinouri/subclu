-- ==================
-- Create tables from embeddings to do aggregation in BigQuery
-- ==================
-- Create tables with embeddings to test how fast/easy it is to
--  get the weighted average of the embeddings if we use BigQuery
--  maybe it's fast enough in there and would save dependencies on
--  new distributed processing/compute platforms.
-- Example for paths:
--     - single file:
--         data/models/fse/manual_merge_2021-06-07_17/df_one_file.parquet
--     - files matching pattern:
--         data/models/fse/manual_merge_2021-06-07_17/df_*.parquet
--     - folder. NOTE: ALL FILES IN FOLDER MUST HAVE THE SAME COLUMNS/FORMAT
--         data/models/fse/manual_merge_2021-06-07_17/

CREATE OR REPLACE EXTERNAL TABLE `reddit-employee-datasets.david_bermejo.subclu_v0040_embeddings_comments`
OPTIONS (
    format='PARQUET',
    uris=[
        "gs://i18n-subreddit-clustering/mlflow/mlruns/14/5f10cd75334142168a6ebb787e477c1f/artifacts/df_vect_comments/*.parquet",
        "gs://i18n-subreddit-clustering/mlflow/mlruns/14/2fcfefc3d5af43328168d3478b4fdeb6/artifacts/df_vect_comments/*.parquet"
    ]
)
;

-- Check table counts
-- SELECT
--     *
--     , (post_unique_count / subreddit_unique_count) AS posts_per_subreddit_mean
--     , (comment_unique_count / subreddit_unique_count) AS comments_per_subreddit_mean
--     , (comment_unique_count / post_unique_count) AS comments_per_post_mean
-- FROM (
--     SELECT
--         COUNT(*)       AS row_count
--         , COUNT(DISTINCT post_id) AS post_unique_count
--         , COUNT(DISTINCT comment_id) AS comment_unique_count
--         , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count
--
--     FROM `reddit-employee-datasets.david_bermejo.subclu_v0040_embeddings_comments`
-- );
-- Results
-- Query complete (11.2 sec elapsed, 1.2 GB processed)
--  row_count 	 post_unique_count 	 comment_unique_count 	 subreddit_unique_count 	 posts_per_subreddit_mean 	 comments_per_subreddit_mean 	 comments_per_post_mean
--  39,901,968 	 7,038,219 	 39,901,968 	 19,020 	 370.04 	 2,097.90 	 5.67


-- Embeddings created when we pre-process the text to lowercase before running through USE
-- I haven't run this job, so we can't create this table yet.
-- CREATE OR REPLACE EXTERNAL TABLE `reddit-employee-datasets.david_bermejo.subclu_v0040_embeddings_comments_lowercase`
-- OPTIONS (
--     format='PARQUET',
--     uris=[]
-- )
-- ;

