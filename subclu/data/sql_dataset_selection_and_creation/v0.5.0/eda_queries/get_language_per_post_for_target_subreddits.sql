-- Use it to check individual posts & their predicted language
--  We can use this as a basis to aggregate posts primary & secondary language
--    at the subreddit level. But it might be better to cache the value for
--    the past l28 days
DECLARE PARTITION_DATE DATE DEFAULT "2022-07-27";
DECLARE POST_DT_START DATE DEFAULT PARTITION_DATE - 4;

SELECT
    c.subreddit_id
    , c.subreddit_name
    , d.post_id
    , lc.language_name
    , d.weighted_language AS detected_language
    , d.text
FROM `data-prod-165221.cnc.subreddit_metadata_lookup` c
    LEFT JOIN `data-prod-165221.language_detection.post_language_detection_cld3` AS d
        ON c.subreddit_id = d.subreddit_id
        AND DATE(d._PARTITIONTIME) BETWEEN POST_DT_START AND PARTITION_DATE
    LEFT JOIN `reddit-employee-datasets.david_bermejo.language_detection_code_to_name_lookup_cld3` AS lc
        ON d.weighted_language = lc.language_code
WHERE 1=1
    AND c.pt = PARTITION_DATE
    -- Use QUALIFY to get a single row when there are duplicates. It saves us from having to do a CTE!
    --  post lang detection table has duplicates (e.g., when a user edits a post)
    QUALIFY ROW_NUMBER() OVER(PARTITION BY d.post_id ORDER BY d.created_timestamp DESC) = 1
    -- AND weighted_language != 'en'
    -- AND weighted_language IN (
    --     'de', 'da'
    --     , 'en', 'sr'
    -- )
    AND c.subreddit_name IN (
        'antiwork'
        -- , 'india'
        -- , 'argentina'
        -- , 'de', 'spain'
        -- , 'meirl', 'ich_iel', 'formula1'
    )
ORDER BY detected_language
LIMIT 1000
;
