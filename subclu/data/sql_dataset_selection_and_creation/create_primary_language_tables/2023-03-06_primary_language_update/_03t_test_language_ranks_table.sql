-- Test created table outputs
-- Get primary language (using post & comment language detection)

SELECT
    subreddit_id
    , subreddit_name
    , language_name
    , language_rank
FROM `reddit-employee-datasets.david_bermejo.subreddit_language_rank_20230306` AS lan

WHERE language_rank = 1
    AND lan.thing_type = 'posts_and_comments'
;
