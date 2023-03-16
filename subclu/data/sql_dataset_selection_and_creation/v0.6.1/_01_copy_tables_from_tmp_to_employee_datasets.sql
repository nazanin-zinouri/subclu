-- Tables needed to create v0.6.1 model:
--  Copy from relevance.tmp to employee datasets for long-term use (dashboards & replicate)

-- Subreddit-level tables. Include fixes for Primary topic & Ratings!
CREATE TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddit_candidates_20230313` AS (
SELECT
    -- FIX for rating & topic: get value from CURATOR (if available)
    tx.survey_version
    , (tx.taxonomy_rating_name) AS rating_name
    , (tx.taxonomy_rating) AS rating_short
    , (tx.taxonomy_topic) AS primary_topic
    , tx.curator_topic_v2
    , sm.* EXCEPT(primary_topic, rating_short, rating_name)
FROM `reddit-relevance.tmp.subclu_subreddit_candidates_20230313` AS sm
    LEFT JOIN `reddit-employee-datasets.david_bermejo.reddit_vault_predictions_and_overrides_vw` AS tx
        ON sm.subreddit_id = tx.subreddit_id
);

CREATE TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_relevance_standardized_20230313` AS (
SELECT *
FROM `reddit-relevance.tmp.subclu_subreddit_geo_relevance_standardized_20230313` AS sm
);

CREATE TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddits_for_modeling_20230313` AS (
SELECT
    -- FIX for rating & topic: get value from CURATOR (if available)
    tx.survey_version
    , (tx.taxonomy_rating_name) AS rating_name
    , (tx.taxonomy_rating) AS rating_short
    , (tx.taxonomy_topic) AS primary_topic
    , tx.curator_topic_v2
    , sm.* EXCEPT(primary_topic, rating_short, rating_name)
FROM `reddit-relevance.tmp.subclu_subreddits_for_modeling_20230313` AS sm
    LEFT JOIN `reddit-employee-datasets.david_bermejo.reddit_vault_predictions_and_overrides_vw` AS tx
        ON sm.subreddit_id = tx.subreddit_id
);


-- Post & comment tables
CREATE TABLE `reddit-employee-datasets.david_bermejo.subclu_posts_for_modeling_20230313` AS (
SELECT *
FROM `reddit-relevance.tmp.subclu_posts_for_modeling_20230313`
);

CREATE TABLE `reddit-employee-datasets.david_bermejo.subclu_comments_for_modeling_20230313` AS (
SELECT *
FROM `reddit-relevance.tmp.subclu_comments_for_modeling_20230313`
);

CREATE TABLE `reddit-employee-datasets.david_bermejo.subclu_post_and_comment_text_combined_20230313` AS (
SELECT *
FROM `reddit-relevance.tmp.subclu_post_and_comment_text_combined_20230313`
);
