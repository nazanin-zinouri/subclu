-- Tables to copy from relevance.tmp to employee datasets for long-term use

-- Copy relevance table with fixed standardized score so that we can use it
--  until the PR for production table goes into effect
--  NOTE: we'll only pull the standardized scores from here, but we'll use the
--    L28 day per subreddit score from the live table (that score is fine)
CREATE TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_relevance_standardized_20220901` AS (
SELECT *
FROM `reddit-relevance.tmp.subclu_subreddit_geo_relevance_standardized_20220901`
);


-- Tables needed to create v0.6.0 model:
CREATE TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_relevance_standardized_20220811` AS (
SELECT *
FROM `reddit-relevance.tmp.subclu_subreddit_geo_relevance_standardized_20220811`
);

CREATE TABLE `reddit-employee-datasets.david_bermejo.subclu_comments_for_modeling_20220811` AS (
SELECT *
FROM `reddit-relevance.tmp.subclu_comments_for_modeling_20220811`
);

CREATE TABLE `reddit-employee-datasets.david_bermejo.subclu_post_and_comment_text_combined_20220811` AS (
SELECT *
FROM `reddit-relevance.tmp.subclu_post_and_comment_text_combined_20220811`
);


CREATE TABLE `reddit-employee-datasets.david_bermejo.subclu_posts_for_modeling_20220811` AS (
SELECT *
FROM `reddit-relevance.tmp.subclu_posts_for_modeling_20220811`
);


CREATE TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddit_candidates_20220811` AS (
SELECT *
FROM `reddit-relevance.tmp.subclu_subreddit_candidates_20220811`
);


CREATE TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddits_for_modeling_20220811` AS (
SELECT *
FROM `reddit-relevance.tmp.subclu_subreddits_for_modeling_20220811`
);
