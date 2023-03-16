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



-- Check top languages for test subreddits
SELECT * EXCEPT(thing_type)
FROM `reddit-employee-datasets.david_bermejo.subreddit_language_rank_20230305` AS lan

WHERE language_rank <= 2
    AND lan.thing_type = 'posts_and_comments'
    AND subreddit_name IN (
        'de', 'mexico', 'meirl', 'ich_iel'
        , 'india'
        , 'france', 'rance'
        , 'czech', 'prague', 'sweden'
        , 'japan', 'china_irl', 'newsokunomoral'
        , 'ligamx', 'absoluteunits', 'aww'
    )
ORDER BY subreddit_name, language_rank
;
