-- Initial candidate list for subreddit counterpart FPR for a single country

-- Use this date to pull the latest partitions for subreddit_lookup, etc.
DECLARE PT_DATE DATE DEFAULT CURRENT_DATE() - 2;

-- The query checks that the geo-relevant subreddit:
--  * Is geo-relevant to the target country
--  * Has the target language as one of the top 4 languages (rank<=4)
DECLARE GEO_TARGET_COUNTRY_CODE_TARGET STRING DEFAULT "DE";
DECLARE GEO_TARGET_LANGUAGE STRING DEFAULT "German";

-- Lower threshold  = add more subreddits, but they might be less relevant
-- Higher threshold = reduce relevant subreddits, but they're more local
--  Suggested ranges:
--    * between 2.5 and 3.0 for NON-English countries
--    * 4.0+ For English-speaking countries (CA, GB, AU)
DECLARE STANDARDIZED_COUNTRY_THRESHOLD NUMERIC DEFAULT 2.5;

-- For non-English countries: ~0.25 is ok
-- For English-speaking countries: 0.3 or 0.4+
DECLARE MIN_PCT_USERS_L28_COUNTRY NUMERIC DEFAULT 0.2;

-- Min & max number of counterparts to show
DECLARE MIN_COUNTERPARTS_TO_SHOW NUMERIC DEFAULT 1;
DECLARE MAX_COUNTERPARTS_TO_SHOW NUMERIC DEFAULT 5;

-- Min US subscribers: Only show counterparts that have at least these many subscribers
--  Otherwise the impact will be too small, try 8k or 4k
DECLARE MIN_US_SUBSCRIBERS NUMERIC DEFAULT 4000;


WITH
    subs_relevant_baseline AS (
        -- Use this CTE to prevent recommending geo-relevant subs (example: DE to DE)
        SELECT
            ga.subreddit_id
        FROM `data-prod-165221.i18n.community_local_scores` AS ga
        WHERE DATE(ga.pt) = PT_DATE
            -- filters for geo-relevant country
            AND ga.geo_country_code = GEO_TARGET_COUNTRY_CODE_TARGET
            AND (
                ga.sub_dau_perc_l28 >= 0.20
            )
    ),

    subreddits_relevant_to_country AS (
        SELECT
            ga.geo_country_code
            , ga.subreddit_id
            , ga.subreddit_name
            , tx.curator_rating
            , tx.curator_topic_v2
        FROM `data-prod-165221.i18n.community_local_scores` AS ga
            -- Get primary language
            LEFT JOIN `reddit-employee-datasets.david_bermejo.subreddit_language_rank_20220808` AS lan
                ON ga.subreddit_id = lan.subreddit_id

            -- TODO(djb): Use taxonomy's ratings to filter to subs already approved & some subs that haven't been rated
            LEFT JOIN `data-prod-165221.taxonomy.daily_export` AS tx
                ON ga.subreddit_id = tx.subreddit_id

        WHERE DATE(ga.pt) = PT_DATE
            -- Remove subreddits flagged as sensitive by taxonomy
            AND COALESCE(tx.curator_rating, "") IN ("", 'Everyone', 'Mature 1')

            -- filters for geo-relevant country
            AND (
                ga.geo_country_code = GEO_TARGET_COUNTRY_CODE_TARGET
                -- relevance filters
                AND (
                    ga.perc_by_country >= MIN_PCT_USERS_L28_COUNTRY
                    OR ga.perc_by_country_sd >= STANDARDIZED_COUNTRY_THRESHOLD
                )
                -- language filters
                AND (
                    lan.language_name = GEO_TARGET_LANGUAGE
                    AND lan.language_rank IN (1, 2, 3)
                    AND lan.thing_type = 'posts_and_comments'
                    AND lan.language_percent >= 0.05
                )
            )
    ),

    distance_lang_and_relevance_a AS (
        -- Select metadata for geo subs (sub_id_a) + get similarity
        SELECT
            ga.geo_country_code
            , d.subreddit_id AS subreddit_id_geo
            , n.subreddit_id AS subreddit_id_us

            , d.subreddit_name AS subreddit_name_geo
            , n.subreddit_name AS subreddit_name_us
            , cosine_similarity
            , slo.over_18 AS over_18_geo
            , slo.allow_discovery AS allow_discovery_geo
            -- TODO(djb): get curator rating & topic from TAXONOMY table
            , ga.curator_rating AS curator_rating_geo
            , ga.curator_topic_v2 AS curator_topic_v2_geo
            , tx.curator_rating AS curator_rating_us
            , tx.curator_topic_v2 AS curator_topic_v2_us

        FROM `reddit-employee-datasets.david_bermejo.cau_similar_subreddits_by_text` AS d
            -- We need to UNNEST & join the field with nested JSON
            LEFT JOIN UNNEST(similar_subreddit) AS n

            -- Get geo-relevance scores
            INNER JOIN subreddits_relevant_to_country AS ga
                ON d.subreddit_id = ga.subreddit_id
            LEFT JOIN subs_relevant_baseline AS gb
                ON n.subreddit_id = gb.subreddit_id
            LEFT JOIN (
                SELECT * FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
                WHERE dt = PT_DATE
            ) AS slo
                ON d.subreddit_id = slo.subreddit_id
            -- Get topic & rating from QA table because it includes CURATOR labels, not just crowd
            LEFT JOIN `data-prod-165221.taxonomy.daily_export` AS tx
                ON n.subreddit_id = tx.subreddit_id

        WHERE 1=1
            -- Exclude subreddits that are geo-relevant to the country (no DE<>DE recommendations)
            AND gb.subreddit_id IS NULL

            -- Filter out US subs that are not clean for recommendations
            AND COALESCE(tx.curator_rating, "") IN ("", 'Everyone', 'Mature 1')

            -- Exclude subs with covid or corona in name
            AND d.subreddit_name NOT LIKE "%covid%"
            AND d.subreddit_name NOT LIKE "%coronavirus%"
            AND n.subreddit_name NOT LIKE "%covid%"
            AND n.subreddit_name NOT LIKE "%coronavirus%"
    ),
    distance_lang_and_relevance_a_and_b AS (
        -- Keep only counterpart subs that are in English, large by subscribers, & US-relevant
        SELECT
            a.* EXCEPT(
                -- language_name_geo, language_percent_geo, language_rank_geo,
                over_18_geo, allow_discovery_geo
            )
            , slo.subscribers AS subscribers_us
            , asg.users_l7 AS users_l7_geo
            , asu.users_l7 AS users_l7_us

            , ROW_NUMBER() OVER (PARTITION BY subreddit_id_GEO ORDER BY cosine_similarity DESC) AS rank_geo_to_us
            , allow_discovery_geo

            -- , language_name_geo, language_percent_geo, language_rank_geo
            -- , lan.language_name AS primary_language_name_us
            -- , lan.language_percent AS primary_language_percent_us
            , over_18_geo
            , slo.over_18 AS over_18_us

        FROM distance_lang_and_relevance_a AS a
            LEFT JOIN `data-prod-165221.i18n.community_local_scores` AS g
                ON a.subreddit_id_us = g.subreddit_id

            -- Get primary language
            LEFT JOIN `reddit-employee-datasets.david_bermejo.subreddit_language_rank_20220808` AS lan
                ON a.subreddit_id_us = lan.subreddit_id
            -- Get subscribers
            LEFT JOIN `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup` AS slo
                ON a.subreddit_id_us = slo.subreddit_id

            -- Get users for geo
            LEFT JOIN (
                SELECT subreddit_name, users_l7 FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
                WHERE DATE(pt) = PT_DATE
            ) AS asg
                ON a.subreddit_name_geo = LOWER(asg.subreddit_name)

            -- Get users for US
            LEFT JOIN (
                SELECT subreddit_name, users_l7 FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
                WHERE DATE(pt) = PT_DATE
            ) AS asu
                ON a.subreddit_name_us = LOWER(asu.subreddit_name)

        WHERE DATE(g.pt) = PT_DATE
            AND slo.dt = PT_DATE
            -- Filter US counterparts that are too small
            AND slo.subscribers >= MIN_US_SUBSCRIBERS

            -- more filters for US counterparts
            AND (
                g.geo_country_code = 'US'
                -- relevance filters
                AND g.sub_dau_perc_l28 >= 0.25
                -- language filters
                AND (
                    lan.language_name = 'English'
                    AND lan.language_rank = 1
                    AND lan.thing_type = 'posts_and_comments'
                    AND lan.language_percent >= 0.5
                )
            )
    )
    , counterparts_geo AS (
        -- Final check and filters to pick only expected number of counterparts per seed sub
        SELECT
            d.* EXCEPT(over_18_geo, over_18_us)
        FROM distance_lang_and_relevance_a_and_b AS d
        WHERE 1=1
            AND (
                rank_geo_to_us <= MIN_COUNTERPARTS_TO_SHOW
                OR cosine_similarity >= 0.77
            )
            AND rank_geo_to_us <= MAX_COUNTERPARTS_TO_SHOW
    )

-- final counterpart FPR with expected format
SELECT
    PT_DATE AS pt
    , geo_country_code
    , subreddit_id_geo AS subreddit_id
    , subreddit_id_us

    , cosine_similarity
    , subreddit_name_geo AS subreddit_name
    , subreddit_name_us
    , users_l7_geo
    , users_l7_us
    , subscribers_us

    , curator_rating_geo
    , curator_topic_v2_geo
    , curator_rating_us
    , curator_topic_v2_us

FROM counterparts_geo
ORDER BY users_l7_geo DESC, subreddit_name_geo, users_l7_us DESC
-- GROUP BY 1, 2, 3, 4

