-- ============
-- Test CTEs
-- ===

-- Check the baseline subreddits
--  Note: we expect to filter more of these because we'll only allow unrated for ROW countries
--    (exclude unrated subreddits when they're only relevant to large English-speaking countries)
SELECT *
FROM subs_above_thresholds
ORDER BY over_18 DESC, curator_rating, curator_topic_v2, users_l7 DESC, posts_l7 DESC
;


-- Check sub<>geo CTE
SELECT *
FROM subs_with_geo
WHERE 1=1
    -- AND geo_country_code IN (
    --     'MX'
    --     , 'US'
    -- )
ORDER BY geo_country_code, posts_l7 DESC, users_l7 DESC, curator_rating, curator_topic_v2
;
