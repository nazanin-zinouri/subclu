-- This table combines:
--   subreddit_lookup & cnc's ratings into a single table:

SELECT
    subreddit_name
    , over_18
    , whitelist_status
    , rating_short
    , rating_name
    , rating_sub_themes
FROM `data-prod-165221.cnc.subreddit_metadata_lookup`
WHERE pt = "2022-06-25"
    AND LOWER(subreddit_name) IN (
        'hindisexstories', 'sexstories'
    )
    -- AND rating_short = 'X'
    -- AND COALESCE(over_18, 'f') = 'f'
LIMIT 1000
