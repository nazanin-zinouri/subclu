-- Check relevance for selected subreddits
SELECT *
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220329`

WHERE 1=1
    AND subreddit_name IN (
        'fifa', 'soccer', 'photography'
        , 'beer'
        -- 'rap', 'kpop'

        , 'anno'
        -- Should be German, but have a lot of English
        -- , 'bundesliga'
        -- , 'bayernmunich', 'fcbayern', 'borussiadortmund'

        -- German subs, these seem to be dead now
        , 'kpopde', 'southparkde', 'diesimpsons'
    )
    AND users_percent_by_subreddit_l28 >= 0.05

ORDER BY subreddit_name ASC, users_percent_by_subreddit_l28 DESC
LIMIT 1000
