-- Get top subreddits by content similarity
SELECT
    s.subreddit_id AS subreddit_id_seed
    , s.subreddit_name AS subreddit_name_seed
    , n.*
FROM `reddit-employee-datasets.david_bermejo.cau_similar_subreddits_by_text` AS s
    -- We need to UNNEST & join the field with nested JSON
    LEFT JOIN UNNEST(similar_subreddit) AS n

WHERE pt = "2022-11-22"
    AND s.subreddit_name IN (
        'aww'
        , 'formula1', 'uxdesign', 'absoluteunits'
        , 'wallstreetbets', 'de', 'france', 'fire', 'mexico'
        , 'me_irl', 'ich_iel'
        , 'bollywood', 'bollyarm'
    )
    AND distance_rank <= 10
;
