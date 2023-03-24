-- Create candidate list for subreddit counterparts in multiple countries
--  Use it to explore curation across multiple countries, not just a single country

-- Use this date to pull the latest partitions for subreddit_lookup, etc.
DECLARE PT_DATE DATE DEFAULT CURRENT_DATE() - 2;

DECLARE TARGET_SUBREDDIT_NAMES DEFAULT [
    'formula1', 'soccer'
    , 'askreddit', 'kpop', 'wallstreetbets'
    , 'personalfinance'
    -- , 'ligamx', 'de'
];


DECLARE GEO_TARGET_COUNTRY_CODE_TARGET DEFAULT ['DE', "MX", 'ES'];
-- DECLARE GEO_TARGET_COUNTRY_CODE_TARGET STRING DEFAULT '{{geo_country_code_param}}';


-- The query checks that the geo-relevant subreddit:
--  * Is geo-relevant to the target country (strict, loose, or modified (lower standardized threshold))
-- Lower threshold  = add more subreddits, but they might be less relevant
-- Higher threshold = reduce relevant subreddits, but they're more local
--  Suggested ranges:
--    * between 2.5 and 3.0 for NON-English countries
--    * 4.0+ For English-speaking countries (CA, GB, AU)
DECLARE STANDARDIZED_COUNTRY_THRESHOLD NUMERIC DEFAULT 2.0;

-- For non-English countries: ~0.25 is ok
-- For English-speaking countries: 0.3 or 0.4+
-- DECLARE MIN_PCT_USERS_L28_COUNTRY NUMERIC DEFAULT 0.22;

-- Min & max number of counterparts to show
DECLARE MIN_COUNTERPARTS_TO_SHOW NUMERIC DEFAULT 1;
DECLARE MAX_COUNTERPARTS_TO_SHOW NUMERIC DEFAULT 10;


WITH
    slo_pt AS (
        SELECT
            subreddit_id, title, over_18
            , allow_discovery, allow_top, allow_trending
        FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
        WHERE dt = PT_DATE
            AND NOT REGEXP_CONTAINS(LOWER(name), r'^u_.*')
    )
    , subreddits_relevant_to_country AS (
        SELECT
            ga.geo_country_code
            , ga.subreddit_id
            , ga.subreddit_name

            -- Remove the ratings & topics until we actually need them?
            , tx.curator_rating
            , tx.curator_topic
            , tx.curator_topic_v2
            , ga.localness
            , asr.users_l7 AS users_l7_geo
            , asr.posts_l7 AS posts_l7_geo
            , slo.over_18 AS over_18_geo
            , slo.allow_discovery AS allow_discovery_geo
            , slo.allow_top AS allow_top_geo
            , slo.title AS title_geo
        FROM `data-prod-165221.i18n.community_local_scores` AS ga
            LEFT JOIN (
                SELECT subreddit_name, users_l7, posts_l7
                FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
                WHERE DATE(pt) = CURRENT_DATE() - 2
            ) AS asr
                ON LOWER(asr.subreddit_name) = ga.subreddit_name

            LEFT JOIN slo_pt AS slo
                ON ga.subreddit_id = slo.subreddit_id
            -- Get primary language
            -- LEFT JOIN `reddit-employee-datasets.david_bermejo.subreddit_language_rank_20230306` AS lan
            --     ON ga.subreddit_id = lan.subreddit_id

            -- Use taxonomy's ratings to filter to subs already approved & some subs that haven't been rated
            LEFT JOIN `data-prod-165221.taxonomy.daily_export` AS tx
                ON ga.subreddit_id = tx.subreddit_id

        WHERE DATE(ga.pt) = PT_DATE
            AND users_l7 >= 100
            AND posts_l7 >= 1
            -- Remove subreddits flagged as sensitive by taxonomy
            -- TODO(djb): Should we make this a parameter?
            -- AND COALESCE(tx.curator_rating, "") IN ("", 'Everyone', 'Mature 1')

            -- filters for geo-relevant country
            AND (
                ga.geo_country_code IN UNNEST(GEO_TARGET_COUNTRY_CODE_TARGET)
                -- relevance filters
                AND (
                    ga.localness != 'not_local'
                    -- OR ga.perc_by_country >= MIN_PCT_USERS_L28_COUNTRY
                    -- OR ga.perc_by_country_sd >= STANDARDIZED_COUNTRY_THRESHOLD
                )
                -- language filters
                -- AND (
                --     lan.language_name IN ( {{geo_target_langs_param}} )
                --     AND lan.language_rank IN (1, 2, 3)
                --     AND lan.thing_type = 'posts_and_comments'
                --     AND lan.language_percent >= 0.09
                -- )
            )
    )
    , counterparts_geo AS (
        -- Select metadata for geo subs (sub_id_a) + get similarity
        SELECT
            d.subreddit_id AS subreddit_id
            , d.subreddit_id_geo
            , ga.geo_country_code

            , d.subreddit_name AS subreddit_name
            , d.subreddit_name_geo
            , ROW_NUMBER() OVER (PARTITION BY ga.geo_country_code, d.subreddit_id ORDER BY d.cosine_similarity DESC) AS rank_by_geo
            , cosine_similarity
            , LEFT(slo.title, 140) AS title
            , LEFT(title_geo, 140) AS title_geo
            , ga.users_l7_geo
            , ga.posts_l7_geo

            , slo.over_18 AS over_18
            , slo.allow_discovery AS allow_discovery
            , slo.allow_top AS allow_top
            , over_18_geo
            , allow_discovery_geo
            , allow_top_geo

            , ga.curator_rating AS curator_rating
            , ga.curator_topic_v2 AS curator_topic_v2
            , tx.curator_rating AS curator_rating_geo
            , tx.curator_topic_v2 AS curator_topic_v2_geo
            , d.model_version
            , d.pt AS model_pt
            -- Compute subreddit length b/c we need it to construct URLs
            -- , CHAR_LENGTH(subreddit_name_geo) AS name_len_geo
            -- , CHAR_LENGTH(subreddit_name_us) AS name_len_us

        FROM (
            SELECT
                d.* EXCEPT(similar_subreddit, mlflow_run_id, model_name)
                , n.subreddit_id AS subreddit_id_geo
                , n.subreddit_name AS subreddit_name_geo
                , n.cosine_similarity
            FROM `reddit-employee-datasets.david_bermejo.cau_similar_subreddits_by_text` AS d
                -- We need to UNNEST & join the field with nested JSON
                LEFT JOIN UNNEST(similar_subreddit) AS n
            WHERE
                -- Keep only pairs from latest model
                d.model_version = "v0.6.1" AND d.pt = "2022-11-22"
                AND d.subreddit_name IN UNNEST(TARGET_SUBREDDIT_NAMES)
        ) AS d
            -- Get only subreddits that are geo-relevant
            INNER JOIN subreddits_relevant_to_country AS ga
                ON d.subreddit_id_geo = ga.subreddit_id

            LEFT JOIN slo_pt AS slo
                ON d.subreddit_id = slo.subreddit_id
            -- Get topic & rating from QA table because it includes CURATOR labels, not just crowd
            LEFT JOIN `data-prod-165221.taxonomy.daily_export` AS tx
                ON d.subreddit_id_geo = tx.subreddit_id

            -- Get primary language
            -- LEFT JOIN `reddit-employee-datasets.david_bermejo.subreddit_language_rank_20230306` AS lan
            --     ON d.subreddit_id_us = lan.subreddit_id

        -- WHERE
            -- Filter out US subs that are not clean for recommendations
            -- AND COALESCE(tx.curator_rating, "") IN ("", 'Everyone', 'Mature 1')
        QUALIFY (
                rank_by_geo <= MIN_COUNTERPARTS_TO_SHOW
                OR cosine_similarity >= 0.50
            )
            AND rank_by_geo <= MAX_COUNTERPARTS_TO_SHOW

    )


SELECT
    *
    , cm.country_name
    -- Only add URLs if needed for curation
    -- , CASE WHEN name_len_geo <= 2 THEN CONCAT("https://reddit.com/r/", subreddit_name_geo)
    --     ELSE CONCAT("https://", subreddit_name_geo, ".reddit.com")
    -- END AS url_geo
    -- , CASE WHEN name_len_geo <= 2 THEN CONCAT("https://reddit.com/r/", subreddit_name_us)
    --     ELSE CONCAT("https://", subreddit_name_us, ".reddit.com")
    -- END AS url

FROM counterparts_geo AS g
    LEFT JOIN `reddit-employee-datasets.david_bermejo.countrycode_name_mapping` AS cm
        ON g.geo_country_code = cm.country_code
-- ORDER BY subreddit_name, geo_country_code, rank_by_geo
;
