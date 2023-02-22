-- EDA for behavior similarity (ML)
SELECT
    s.subreddit_id AS subreddit_id_seed
    , s.subreddit_name AS subreddit_name_seed
    , ROW_NUMBER() OVER(
        PARTITION BY s.subreddit_id
        ORDER BY score DESC
    ) AS behavior_rank
    , n.*
FROM `data-prod-165221.ml_content.similar_subreddit_ft2` AS s
    -- We need to UNNEST & join the field with nested JSON
    LEFT JOIN UNNEST(similar_subreddit) AS n

WHERE pt = "2022-12-06"
    AND s.subreddit_name IN (
        -- Subs to test opposites:
        'vegande', 'carnivore', 'antivegan'
        -- , 'vegetarianketo'

        -- Memes
        -- , 'aww', 'eyebleach', 'absoluteunits'

        , 'mexico', 'ligamx'
        , 'de', 'bundesliga'
        , 'ich_iel'
        -- , 'formula1', 'uxdesign'
        -- , 'wallstreetbets'
        -- , 'france', 'fire'

        -- , 'me_irl'
        -- , 'bollywood', 'bollyarm'
    )
    -- After unnesting, we can apply filters based on nested fields
    AND n.score >= 0.5
ORDER BY subreddit_name_seed, score DESC
;


-- Check subreddit count
-- SELECT
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT s.subreddit_id) AS subreddit_count
-- FROM `data-prod-165221.ml_content.similar_subreddit_ft2` AS s
-- WHERE pt = "2022-12-06"
-- ;


-- ==================
-- EDA for latest local scores for subreddits that might be borderline local
-- ===
SELECT * EXCEPT(pt, sub_dau_l1, sub_dau_perc_l1)
FROM `data-prod-165221.i18n.community_local_scores`
WHERE DATE(pt) = "2022-12-07"
    -- AND geo_country_code IN (
    --     "DE", "AT", "US", "CH"
    -- )
    AND subreddit_name IN (
        "fussball", 'amcstocks', 'cricket'
    )
    -- AND localness != 'not_local'
    AND (
        sub_dau_perc_l28 >= 0.1
        OR perc_by_country_sd >= 1
    )

ORDER BY subreddit_name, sub_dau_perc_l28 DESC, perc_by_country_sd DESC
LIMIT 1000
