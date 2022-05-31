-- Get relevant subreddits for a country
SELECT *
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220502`
WHERE 1=1
    AND (
        geo_relevance_default = TRUE
        OR relevance_percent_by_subreddit = TRUE
        OR users_percent_by_country_standardized >= 3.0
        -- Use relevance_combined_score only if we're finding "relevant" subs that might not be local
        -- OR relevance_combined_score >= 0.19
    )
    AND geo_country_code IN ('DE')

ORDER BY subreddit_rank_in_country ASC
;


-- Find relevant countries for specific subreddits
SELECT *
-- FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220502`
FROM `reddit-relevance.tmp.subclu_subreddit_geo_relevance_standardized_20220526`
WHERE 1=1
    AND subreddit_name IN (
        -- Subs that are expected to be relevant in multiple countries
        'cricket'
        , 'soccer', 'futbol'
        , 'formula1', 'premierleague'

        -- subs expected to be relevant in one or 2 countries
        ,  'fussball', 'ligamx', 'ipl', 'rugbyunion'
        , 'mexico', 'india', 'france', 'rance', 'ich_iel'

        -- subs popular all over the world
        , 'askreddit', 'worldnews', 'funny', 'movies', 'antiwork', 'pics', 'facepalm', 'gaming'
        , 'walstreetbets', 'todayilearned', 'eldenring'
        , 'nextfuckinglevel', 'publicfreakout', 'explainlikeimfive'

    )
    AND (
        geo_relevance_default = TRUE
        OR relevance_percent_by_subreddit = TRUE
        -- OR relevance_percent_by_country_standardized = TRUE
        OR users_percent_by_country_standardized >= 2.5
        OR relevance_combined_score >= 0.17
    )
ORDER BY subreddit_name, relevance_combined_score DESC -- users_percent_by_subreddit_l28 DESC
;
