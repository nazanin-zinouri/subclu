
-- ===========================
-- Tests/checks for geo-relevant query
-- These tests check the CTE BEFORE creating the table
-- ===
-- final output COUNT
-- SELECT
--     COUNT(*)  AS row_count
--     , COUNT(DISTINCT subreddit_id)  AS subreddit_unique_count
--     , COUNT(DISTINCT country_name)  AS country_unique_count
-- FROM final_geo_output AS geo
-- LEFT JOIN `data-prod-165221.all_reddit.all_reddit_subreddits` AS asr
--     ON LOWER(geo.subreddit_name) = asr.subreddit_name
-- WHERE 1=1
--     -- country filters
--     AND (
--         country_name IN ('Germany', 'Austria', 'Switzerland', 'India', 'France', 'Spain', 'Brazil', 'Portugal', 'Italy')
--         OR geo_region = 'LATAM'
--         -- OR country_code IN ('CA', 'GB', 'AU')
--     )
--     -- activity filters
--     AND asr.users_l7 >= min_users_geo_l7
--     AND asr.posts_l28 >= min_posts_geo_l28
-- ;


-- Check geo_sub
--   All subreddits appear here
-- SELECT
--     *
--     , (users_country / total_users)  AS users_percent_by_country
-- FROM geo_sub
-- WHERE 1=1
--     -- David's filter specific subs
--     AND LOWER(subreddit_name ) IN (
--         'fussball', 'fifa_de', 'borussiadortmund', 'futbol', 'fcbayern',
--         'barca', 'realmadrid', 'psg'
--     )
--     AND users_country >= 88
-- ORDER BY subreddit_name, users_country DESC
-- ;


-- Check filtered subs
-- Expected: fifa_de & fussball
--      `borussiadortmund` gets dropped b/c no country is over 40%
-- Output: as expected :)
-- SELECT
--     *
-- FROM filtered_subreddits
-- WHERE 1=1
--     -- David's filter specific subs
--     AND LOWER(subreddit_name ) IN ('fussball', 'fifa_de', 'borussiadortmund', 'futbol', 'fcbayern')

-- ORDER BY subreddit_name, users_percent_by_country DESC
-- ;


-- Check final output
--  Expected: fifa_de, fussball
--  Output: fifa_de used to get drop b/c of old `active=true` filter
-- SELECT
--     *
-- FROM final_geo_output
-- WHERE 1=1
--     -- David's filter specific subs
--     -- AND LOWER(subreddit_name ) IN (
--     --     'fussball', 'fifa_de', 'borussiadortmund', 'futbol', 'soccer'
--     --     , 'dataisbeautiful', 'fcbayern'
--     --     )
--     AND geo_country_code NOT IN ("US", "GB")

-- LIMIT 10000
-- ;

-- Count subreddits per country BEFORE filtering
SELECT
    geo_country_code
    , country_name
    , geo_region

    , COUNT(DISTINCT subreddit_id) AS subreddit_unique_count

-- FROM final_geo_output
FROM `reddit-employee-datasets.david_bermejo.subclu_geo_subreddits_20211214`
WHERE 1=1
    AND total_users >= 1000

    -- David's filter specific subs
    -- AND LOWER(subreddit_name ) IN (
    --     'fussball', 'fifa_de', 'borussiadortmund', 'futbol', 'soccer'
    --     , 'dataisbeautiful'
    --     )
    -- AND geo_country_code NOT IN ("US", "GB")

GROUP BY 1, 2, 3

ORDER BY subreddit_unique_count DESC
;
