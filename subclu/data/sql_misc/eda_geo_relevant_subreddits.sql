

-- My new table where I show that cricket is relevant to India
SELECT *
FROM `reddit-employee-datasets.david_bermejo.subclu_geo_subreddits_20210909`
WHERE subreddit_name LIKE "%cricket"
    AND total_users >= 100

ORDER BY total_users DESC, subreddit_name, users_percent_in_country DESC
LIMIT 100
;

-- Check whether r/cricket is relevant under the 40% threshold...
-- on 2021-09-21, r/Cricket only showed up as relevant to the US on 2021-08-12
SELECT
    subreddit_name
    , cm.country_name
    , cm.region
    , cm.country_code
    , geo.pt
FROM `data-prod-165221.i18n.geo_sfw_communities` AS geo
LEFT JOIN `data-prod-165221.ds_utility_tables.countrycode_region_mapping` AS cm
    ON geo.country = cm.country_code
WHERE DATE(pt) BETWEEN (CURRENT_DATE() - 66) AND (CURRENT_DATE() - 2)
    AND LOWER(subreddit_name) LIKE "%cricket"
;


-- ====================================
-- Select & compare subreddits from my table & new table (no filters)
-- Want to know if I still need to get some subs form Wacy's table (is my table missing any?)
-- ===

-- Geo-relevant from official table
DECLARE partition_date DATE DEFAULT '2021-09-20';
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
            , ROW_NUMBER() OVER (PARTITION BY geo.subreddit_id, country ORDER BY geo.pt desc) as sub_geo_rank_no

            -- use for activity checks
            , asr.users_l7
            , asr.posts_l28
            , asr.comments_l28
            , nt.rating_short
            , nt.rating_name
            , nt.primary_topic

        FROM `data-prod-165221.i18n.all_geo_relevant_subreddits` AS geo
            LEFT JOIN `data-prod-165221.ds_utility_tables.countrycode_region_mapping` AS cm
                ON geo.country = cm.country_code
            LEFT JOIN `data-prod-165221.all_reddit.all_reddit_subreddits` AS asr
                ON LOWER(geo.subreddit_name) = asr.subreddit_name
            LEFT JOIN `data-prod-165221.ds_subreddit_whitelist_tables.active_subreddits`    AS acs
                ON asr.subreddit_name = acs.subreddit_name
            LEFT JOIN `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`             AS slo
                ON asr.subreddit_name = LOWER(slo.name)
            LEFT JOIN `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsourced_topic_and_rating` AS nt
                ON acs.subreddit_id = nt.subreddit_id

        WHERE DATE(geo.pt) BETWEEN (CURRENT_DATE() - 56) AND (CURRENT_DATE() - 2)
            -- date partitions
            AND DATE(asr.pt) = partition_date
            AND DATE(acs._PARTITIONTIME) = partition_date
            AND slo.dt = partition_date
            AND nt.pt = partition_date

            -- country filters
            AND (
                cm.country_name IN ('Germany', 'Austria', 'Switzerland', 'India', 'France', 'Spain', 'Brazil', 'Portugal', 'Italy')
                OR cm.region = 'LATAM'
                -- OR cm.country_code IN ('CA', 'GB', 'AU')
            )

            -- activity filters
            AND asr.users_l7 >= min_users_geo_l7
            AND asr.posts_l28 >= min_posts_geo_l28
        ),

    subs_selected_by_geo AS (
        SELECT
            geo.subreddit_id
            , geo.subreddit_name

            , STRING_AGG(geo.country_name, ', ' ORDER BY geo.country_name) AS geo_relevant_countries
            , STRING_AGG(geo.country_code, ', ' ORDER BY geo.country_code) AS geo_relevant_country_codes
            , COUNT(geo.country_code) AS geo_relevant_country_count

            -- use for activity checks
            , asr.users_l7
            , asr.posts_l28
            , asr.comments_l28
            , nt.rating_short
            , nt.rating_name
            , nt.primary_topic

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

            -- date partitions
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
            -- Activity columns
            , 6, 7, 8, 9, 10, 11
        -- ORDER BY geo.subreddit_name
    )

-- Select one row per subreddit, countries as a list
-- SELECT *
-- FROM subs_selected_by_geo
-- ORDER BY users_l7 DESC, posts_l28 DESC
-- ;

-- Select long - multiple rows per subreddit (one for each relevant country)
SELECT *
FROM geo_subs_raw
WHERE 1=1
    -- keep only one sub per country
    AND sub_geo_rank_no = 1

ORDER BY users_l7 DESC, posts_l28 DESC, subreddit_name, country_name
;


-- Geo-relevant from my custom threshold and time table
