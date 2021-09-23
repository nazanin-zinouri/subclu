-- ==========================================
-- NOTE: WIP. Won't be using for first iteration because of QA issues with duplicates
--      in language detection tables (hard to get a good count number of posts in a language)
-- ===
-- Get count of Spanish language subreddits in past l28 days
--  we can use this to figure out if we want to select subreddits in a language
--  even if they don't qualify from a geo-location score
DECLARE partition_date DATE DEFAULT '2021-08-31';
DECLARE pt_start_date DATE DEFAULT CURRENT_DATE() - 3;
DECLARE pt_end_date DATE DEFAULT CURRENT_DATE() - 2;

DECLARE min_users_geo_l7 NUMERIC DEFAULT 15;
DECLARE min_posts_geo_l28 NUMERIC DEFAULT 5;


WITH
    -- First select subreddits based on geo-relevance
    geo_subs_raw AS (
        SELECT
            -- Lower so it's easier to merge with other tables
            LOWER(geo.subreddit_name)  AS subreddit_name
            , geo.* EXCEPT(subreddit_name)
            -- We need to split the name b/c: "Bolivia, State of..."
            , SPLIT(cm.country_name, ', ')[OFFSET(0)] AS country_name
            , cm.country_code
            , cm.region
            , ROW_NUMBER() OVER (PARTITION BY subreddit_id, country ORDER BY pt desc) as sub_geo_rank_no
        FROM `data-prod-165221.i18n.geo_sfw_communities` AS geo
            LEFT JOIN `data-prod-165221.ds_utility_tables.countrycode_region_mapping` AS cm
                ON geo.country = cm.country_code
        WHERE DATE(pt) BETWEEN (CURRENT_DATE() - 56) AND (CURRENT_DATE() - 2)
            AND (
                cm.country_name IN ('Germany', 'Austria', 'Switzerland', 'India', 'France', 'Spain', 'Brazil', 'Portugal', 'Italy')
                OR cm.region = 'LATAM'
            )
        ),

    subs_selected_by_geo AS (
        SELECT
            geo.subreddit_id
            , geo.subreddit_name
            , STRING_AGG(geo.country_name, ', ' ORDER BY geo.country_name) AS geo_relevant_countries
            , STRING_AGG(geo.country_code, ', ' ORDER BY geo.country_code) AS geo_relevant_country_codes
            , COUNT(geo.country_code) AS geo_relevant_country_count

            -- use for checks but exclude for joins to prevent naming conflicts
            -- , asr.users_l7
            -- , asr.posts_l28
            -- , asr.comments_l28
            -- , nt.rating_name
            -- , nt.primary_topic

        FROM geo_subs_raw AS geo
            LEFT JOIN `data-prod-165221.all_reddit.all_reddit_subreddits` AS asr
                ON geo.subreddit_name = asr.subreddit_name
            LEFT JOIN `data-prod-165221.ds_subreddit_whitelist_tables.active_subreddits`    AS acs
                ON asr.subreddit_name = acs.subreddit_name
            LEFT JOIN `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`             AS slo
                ON asr.subreddit_name = LOWER(slo.name)
            LEFT JOIN `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsourced_topic_and_rating` AS nt
                ON acs.subreddit_id = nt.subreddit_id

        WHERE 1=1
            -- Drop duplicated country names
            AND geo.sub_geo_rank_no = 1

            AND DATE(asr.pt) = partition_date
            AND DATE(acs._PARTITIONTIME) = partition_date
            AND slo.dt = partition_date
            AND nt.pt = partition_date

            -- remove quarantine filter, b/c if we score them we might be able to clusters
            --   of subreddits that are similar to previoiusly quarantined subs
            -- AND slo.quarantine = false
            AND asr.users_l7 >= min_users_geo_l7
            AND asr.posts_l28 >= min_posts_geo_l28

        GROUP BY 1, 2
            -- , 5, 6, 7, 8, 9
        ORDER BY geo.subreddit_name
    ),

    language_data_raw as (
        SELECT
            post_id
            , user_id
            , subreddit_id
            , geolocation_country_code
            , language
            , DATE(_PARTITIONTIME) as pt
            , probability
            , weighted_language
            , possible_alternatives
            , text

        FROM reddit-protected-data.language_detection.comment_language_v2

        WHERE DATE(_PARTITIONTIME) BETWEEN pt_start_date AND pt_end_date
            and thing_type = 'post'
            and language != 'UNKNOWN'
    ),
    -- TODO: There are dupes in this table, so drop them ASAP
    -- TODO: WARNING! this is not de-duped yet. there's a ton of extra work to de-dupe
    --  and QA for deduping that I haven't done yet
    language_data AS (
        SELECT DISTINCT * FROM language_data_raw
    )


-- Check for duplicates in each CTE

-- SELECT
-- -- For geo_subs raw we expect duplicates b/c a sub can be relevant in multiple countries
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT subreddit_id) AS subreddit_count
-- FROM geo_subs_raw
-- ;
-- row_count 	 subreddit_count
-- 64,049 	     3,169


-- SELECT
-- -- subs_selected_by_geo: there should be NO duplicates!
-- -- There should be fewer because we're removing subs that have low activity
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT subreddit_id) AS subreddit_count
-- FROM subs_selected_by_geo
-- ;
--  row_count 	 subreddit_count
--  2,297 	     2,297


-- SELECT
-- -- language_data_raw: there a LOT of duplicates here...
-- --  17 million rows v. 3.6 million unique post_ids
-- --    WTF is going on here??!!
-- --  ARE THESE EDITS??? maybe each time someone edits a post we get a new row
--     COUNT(*) AS row_count
--     , COUNT(DISTINCT post_id) AS post_count
--     , COUNT(DISTINCT subreddit_id) AS subreddit_count
-- FROM language_data_raw
-- ;
-- Row	 row_count 	 post_count 	 subreddit_count
-- 1	 17,348,879 	 3,656,625 	 190,733


SELECT
-- language_data_raw: there a LOT of duplicates here...
    *
FROM language_data_raw
WHERE post_id = 't3_pewfhf'
;




-- Test query - check sub & post language at post-level
-- SELECT
--     s.subreddit_id
--     , s.subreddit_name
--     , COALESCE(s.post_id, l.post_id) AS post_id
--     , s.post_id AS post_id_success
--     , l.post_id AS post_id_language

--     , s.removed
--     , l.weighted_language
--     , geo.geo_relevant_countries
--     , s.post_title

-- FROM subs_selected_by_geo AS geo
-- LEFT JOIN `data-prod-165221.cnc.successful_posts` AS s
--     ON geo.subreddit_id = s.subreddit_id
-- LEFT JOIN language_data AS l
--     on cast(l.pt as date) = s.dt and l.post_id = s.post_id

-- where s.dt BETWEEN pt_start_date AND pt_end_date
--     and l.probability >= 0.5
--     and (s.subreddit_status IS NULL OR s.subreddit_status != 'inactive')
--     AND s.removed = 0

--     -- and weighted_language = 'en'
--     -- and post_nsfw = false
--     -- and successful = 1
--     -- and post_type = 'text'

-- ORDER BY subreddit_name, s.post_id
-- LIMIT 400
-- ;



-- Get Spanish-language counts for L28
-- SELECT
--     COUNT(*)
-- FROM language_data
-- ;

-- Test query - check geo-relevant subs to see if language makes sense there.
-- SELECT
--     s.subreddit_id
--     , s.subreddit_name

--     , COUNT(DISTINCT s.post_id) AS posts_subreddit_count
--     , COUNT(DISTINCT l.post_id) AS posts_with_detected_language_count
--     , SUM(
--         CASE
--             WHEN (weighted_language = 'es') THEN 1
--             ELSE 0
--         END
--     ) AS posts_detected_as_spanish_count


-- FROM `data-prod-165221.cnc.successful_posts` AS s
-- INNER JOIN subs_selected_by_geo AS geo
--     ON geo.subreddit_id = s.subreddit_id
-- LEFT JOIN language_data AS l
--     on cast(l.pt as date) = s.dt and l.post_id = s.post_id

-- where s.dt BETWEEN pt_start_date AND pt_end_date
--     and l.probability >= 0.5
--     and (s.subreddit_status IS NULL OR s.subreddit_status != 'inactive')
--     AND s.removed = 0

--     -- and weighted_language = 'en'
--     -- and post_nsfw = false
--     -- and successful = 1
--     -- and post_type = 'text'

-- GROUP BY subreddit_name, subreddit_id
-- ;
