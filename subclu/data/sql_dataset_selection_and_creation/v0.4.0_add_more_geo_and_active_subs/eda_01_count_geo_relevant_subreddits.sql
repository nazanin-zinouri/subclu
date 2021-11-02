-- We have an OKR for topic model to cover 95% of i18n geo-relevant subreddits
--  So we need to actually count and define what this means
-- Colab notebook:
--  https://colab.research.google.com/drive/1ut0VvzRUkFpjSuVP0h88m0aT5LHUM8gK#scrollTo=Z2VDEgdL5IPX

-- Select all subreddits & get stats to check topic-cluster coverage
DECLARE partition_date DATE DEFAULT '2021-10-25';
DECLARE GEO_PT_START DATE DEFAULT '2021-09-06';
DECLARE GEO_PT_END DATE DEFAULT '2021-09-20';

DECLARE regex_cleanup_country_name_str STRING DEFAULT r" of Great Britain and Northern Ireland| of America|";


WITH
    unique_posts_per_subreddit AS (
        SELECT
            subreddit_id
            , subreddit_name
            , COUNT(*) as posts_l7_submitted
            , COUNT(DISTINCT user_id) as unique_posters_l7_submitted
        FROM
            -- Pull from cnc's table because it's more concistent with activity table
            -- `data-prod-165221.andrelytics_ez_schema.post_submit` as comment
            `data-prod-165221.cnc.successful_posts` AS sp
        WHERE
            DATE(dt) BETWEEN (partition_date - 7) AND partition_date
            AND noun = "post"
        GROUP BY
            subreddit_id, subreddit_name
    ),
    subs_geo_custom AS (
        SELECT
            subreddit_id
            , geo_relevant_countries AS geo_relevant_countries_v04
            , geo_relevant_country_codes AS geo_relevant_country_codes_v04
            , geo_relevant_country_count AS geo_relevant_country_count_v04
            , geo_relevant_subreddit AS geo_relevant_subreddit_v04
        FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20210924`
    ),
    subs_geo_default_raw AS (
        SELECT
            LOWER(geo.subreddit_name) AS subreddit_name
            , geo.subreddit_id
            , geo.country AS geo_country_code
            -- Split to remove long official names like:
            --   Tanzania, United Republic of; Bolivia, Plurinational State of
            -- Regex replace long names w/o a comma
            , REGEXP_REPLACE(
                SPLIT(cm.country_name, ', ')[OFFSET(0)],
                regex_cleanup_country_name_str, ""
            ) AS country_name
            , cm.region AS geo_region
            , ROW_NUMBER() OVER (PARTITION BY subreddit_id, country ORDER BY pt desc) as sub_geo_rank_no

        FROM `data-prod-165221.i18n.all_geo_relevant_subreddits` AS geo
        INNER JOIN `data-prod-165221.ds_utility_tables.countrycode_region_mapping` AS cm
            ON geo.country = cm.country_code
        WHERE DATE(pt) BETWEEN GEO_PT_START AND GEO_PT_END
            AND (
                cm.country_name IN ('Germany', 'Austria', 'Switzerland', 'India', 'France', 'Spain', 'Brazil', 'Portugal', 'Italy')
                OR cm.region = 'LATAM'
                -- eng-i18n =  Canada, UK, Australia
                OR geo.country IN ('CA', 'GB', 'AU')
            )
    ),
    subs_geo_default_agg AS (
        SELECT
            geo.subreddit_id
            -- , geo.subreddit_name

            -- Order so that we get (Mexico, US) only, and not both: (US, Mexico) AND (Mexico, US)
            , STRING_AGG(geo.country_name, ', ' ORDER BY geo.country_name) AS geo_relevant_countries
            , STRING_AGG(geo.geo_country_code, ', ' ORDER BY geo.geo_country_code) AS geo_relevant_country_codes
            , COUNT(geo.geo_country_code) AS geo_relevant_country_count

        FROM subs_geo_default_raw AS geo
        WHERE
            -- Drop duplicated country names
            geo.sub_geo_rank_no = 1
        GROUP BY 1
    )


SELECT
    COALESCE(slo.subreddit_id, acs.subreddit_id)  AS subreddit_id
    , COALESCE(acs.subreddit_name, LOWER(slo.name)) AS subreddit_name
    , slo.subscribers
    -- , DATE_DIFF(CURRENT_DATE(), DATE(slo.created_date), DAY) AS subreddit_age_days
    , DATE_DIFF(CURRENT_DATE(), DATE(slo.created_date), MONTH) AS subreddit_age_months
    , DATE_DIFF(CURRENT_DATE(), DATE(slo.created_date), YEAR) AS subreddit_age_years

    -- Geo-relevant columns v0.4 & 40% threshold
    , CASE WHEN (geo.geo_relevant_country_count >= 1) THEN true
        ELSE false
        END AS geo_relevant_subreddit
    , COALESCE(geo_relevant_subreddit_v04, false) AS geo_relevant_subreddit_v04

    , geoc.* EXCEPT(subreddit_id, geo_relevant_subreddit_v04)
    , geo.* EXCEPT (subreddit_id)


    -- Sub activity
    , CASE
        WHEN COALESCE(asr.users_l7, 0) >= 100 THEN true
        ELSE false
    END AS over_100_users_l7
    , asr.users_l7
    , asr.users_l28
    , acs.active
    , acs.activity_7_day

    , CASE
        WHEN (
            (COALESCE(asr.posts_l7, 0) = 0)
            OR (unique_posters_l7_submitted IS NULL) ) THEN "0_posters"
        WHEN COALESCE(unique_posters_l7_submitted, 0) = 1 THEN "1_poster"
        ELSE "2_or_more_posters"
    END AS unique_posters_l7_bin
    , u.unique_posters_l7_submitted

    , asr.posts_l7
    , asr.comments_l7
    , asr.posts_l28
    , asr.comments_l28

    -- Rating info
    , slo.whitelist_status AS ads_allow_list_status
    , slo.over_18
    , nt.rating_short
    , nt.rating_name
    , primary_topic
    , rating_weight
    -- , array_to_string(mature_themes,", ") as mature_themes

    , slo.verdict
    , slo.type
    , slo.quarantine
    , slo.allow_discovery
    , slo.is_deleted
    , slo.is_spam


FROM (
    SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
    WHERE DATE(pt) = partition_date
) AS asr
    INNER JOIN (
        -- subreddit_lookup includes pages for users, so we need LEFT JOIN
        --  or INNER JOIN with active_subreddits or all_reddit_subreddits
        SELECT * FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
        WHERE dt = partition_date
            AND COALESCE(type, '') != 'user'
    ) AS slo
        ON asr.subreddit_name = LOWER(slo.name)

    -- Need outer join for this b/c all of these subs have already been included in the model
    FULL OUTER JOIN subs_geo_custom AS geoc
        ON slo.subreddit_id = geoc.subreddit_id

    LEFT JOIN subs_geo_default_agg AS geo
        ON slo.subreddit_id = geo.subreddit_id

    LEFT JOIN unique_posts_per_subreddit AS u
        ON asr.subreddit_name = u.subreddit_name

    LEFT JOIN (
        SELECT * FROM `data-prod-165221.ds_subreddit_whitelist_tables.active_subreddits`
        WHERE DATE(_PARTITIONTIME) = partition_date
            -- AND activity_7_day IS NOT NULL
    ) AS acs
        ON slo.subreddit_id = acs.subreddit_id

    LEFT JOIN (
        -- New view should be visible to all, but still comes from cnc_taxonomy_cassandra_sync
        SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
        WHERE pt = partition_date
    ) AS nt
        ON acs.subreddit_id = nt.subreddit_id

WHERE 1=1
    AND asr.users_l7 >= 10
    AND COALESCE(verdict, 'f') <> 'admin_removed'
    AND COALESCE(is_spam, false) = false
    AND COALESCE(is_deleted, false) = false

ORDER BY users_l7 DESC, posts_l7 ASC, activity_7_day ASC # subreddit_name ASC

-- LIMIT 5000
;
