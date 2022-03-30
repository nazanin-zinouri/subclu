-- Check the language rank for specific subreddits
SELECT *
FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_language_rank`
WHERE 1=1
    AND thing_type = 'posts_and_comments'
    AND subreddit_name IN (
        'mauerstrassenwetten'
        -- , 'fussball', 'bundesliga', 'bayernmunich'
        -- , 'fcbayern', 'beer'
        -- , 'rappers', 'anno'
    )
ORDER BY subreddit_name, language_rank
LIMIT 1000
