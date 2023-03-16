-- Get top subreddits by content similarity + Sort by users_l7
SELECT
    s.subreddit_id AS subreddit_id_seed
    , s.subreddit_name AS subreddit_name_seed
    , n.*
    , asr.users_l7
FROM `reddit-employee-datasets.david_bermejo.cau_similar_subreddits_by_text` AS s
    -- We need to UNNEST & join the field with nested JSON
    LEFT JOIN UNNEST(similar_subreddit) AS n
    LEFT JOIN (
        SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
        WHERE DATE(pt) = CURRENT_DATE() - 2
    ) AS asr
        ON LOWER(asr.subreddit_name) = n.subreddit_name

WHERE s.pt = "2022-11-22"
    AND s.subreddit_name IN (
        'birding'
        , 'aww'
        , 'gardening'
        , 'cat'
        , 'cryptocurrency'
        -- , 'formula1', 'uxdesign', 'absoluteunits'
        -- , 'wallstreetbets', 'de', 'france', 'fire', 'mexico'
        -- , 'me_irl', 'ich_iel'
        -- , 'bollywood', 'bollyarm'
    )
    AND distance_rank <= 20
ORDER BY subreddit_name_seed, users_l7 DESC, distance_rank
;
