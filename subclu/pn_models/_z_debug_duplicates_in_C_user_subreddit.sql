-- Investigate dupes in user<>subreddit & user tables
--  Turns out some of the problem is local-scores table can have multiple rows for 1 subreddit+geo

DECLARE TARGET_COUNTRY_CODES DEFAULT [
    "MX", "ES", "AR"
    , "DE", "AT", "CH"
    , "US", "GB", "IN", "CA", "AU", "IE"
    , "FR", "NL", "IT"
    , "BR", "PT"
    , "PH"
];


-- SELECT *
-- FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230509`
-- WHERE pt = "2023-05-06"
--     AND user_geo_country_code NOT IN UNNEST(TARGET_COUNTRY_CODES)
--     -- AND user_id IN (
--     --     "t2_b8nvw", "t2_6y990dvb", "t2_8ev9tif2", "t2_1003rg", "t2_100657"
--     -- )
-- ORDER BY user_id, target_subreddit
-- -- LIMIT 1000
-- ;

-- Check for dupes in this table. ETA: 7 mins
-- We expecte zero dupes WHEN we group by user_id + target_subreddit (or target_subreddit_id)
-- SELECT
--     user_id
--     -- , target_subreddit_id
--     , target_subreddit

--     , COUNT(*) AS dupe_count
-- FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230509`
-- WHERE pt = "2023-05-07"
-- GROUP BY 1,2
-- HAVING dupe_count > 1
-- ORDER BY dupe_count DESC, target_subreddit, user_id
-- ;

-- Investigate user-id with dupe target subreddits
-- SELECT *
-- FROM `reddit-employee-datasets.david_bermejo.pn_ft_user_subreddit_20230509`
-- WHERE pt = "2023-05-07"
--     -- AND user_geo_country_code NOT IN UNNEST(TARGET_COUNTRY_CODES)
--     AND user_id IN (
--         "t2_10b6i5rs", "t2_10cv18", "t2_10fecj", "t2_1i0b9wlr", "t2_47db7a4y"
--         , 't2_zstq3', 't2_yo40r', 't2_v2z84qop', 't2_uitorw3z', 't2_t0akd'
--         , 't2_gp7te', 't2_ar6f5i97l', 't2_6j4kv'
--     )
-- ORDER BY user_id, target_subreddit
-- ;

-- Is the problem that r/fifa is in the top subreddits twice?
-- No, but it looks like it's in local scores twice for GB! wtf???
-- SELECT *
-- FROM `reddit-employee-datasets.david_bermejo.pn_ft_subreddits_20230509`
-- WHERE pt = "2023-05-07"
--     AND subreddit_name IN ('fifa')
-- ;

-- No, but it looks like it's in local scores twice for GB! wtf???
-- TODO(djb): need to add a DISTINCT clause every time I'm using community_local_scores
SELECT
    DISTINCT
    subreddit_id
    , geo_country_code
    , sub_dau_perc_l28
    , perc_by_country_sd
FROM `data-prod-165221.i18n.community_local_scores`
WHERE DATE(pt) = "2023-05-07"
    AND subreddit_name IN ("fifa")
    AND geo_country_code IN ("GB")
;
