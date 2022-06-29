-- Get top 100 ANN's for a given subreddit
--  See ML-Content team's wiki for more info about their embeddings
--   https://reddit.atlassian.net/wiki/spaces/ML/pages/2319843329/

SELECT
    s.subreddit_id AS subreddit_id_seed
    , s.subreddit_name AS subreddit_name_seed
    , n.*
FROM `data-prod-165221.ml_content.similar_subreddit_ft2` AS s
    -- We need to UNNEST & join the field with nested JSON
    LEFT JOIN UNNEST(similar_subreddit) AS n

WHERE pt = "2022-06-21"
    AND s.subreddit_name IN (
        'aww'
        , 'formula1', 'uxdesign', 'absoluteunits'
        , 'wallstreetbets', 'de', 'france', 'fire', 'mexico'
        , 'me_irl', 'ich_iel'
        , 'bollywood', 'bollyarm'
    )
    -- After unnesting, we can apply filters based on nested fields
    AND n.score >= 0.7
ORDER BY subreddit_name_seed, score DESC
;
