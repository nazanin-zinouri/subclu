-- Initial filter for subreddit counterparts
--  Note that we filter out more subreddits in a colab notebook

-- %%time
-- %%bigquery df_distances --project data-science-prod-218515
-- Get distances for geo-relevant subs

DECLARE GEO_TARGET_COUNTRY_CODE_TARGET STRING DEFAULT "DE";
DECLARE GEO_TARGET_LANGUAGE STRING DEFAULT "German";

DECLARE MAX_COUNTERPARTS_TO_SHOW NUMERIC DEFAULT 9;
DECLARE STANDARDIZED_COUNTRY_THRESHOLD NUMERIC DEFAULT 3.0;

WITH
    subs_relevant_baseline AS (
        -- Use this query to prevent recommending geo-relevant subs (example: DE to DE)
        SELECT
            ga.subreddit_id
            , ga.subreddit_name
        FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220329` AS ga
        WHERE 1=1
            -- filters for geo-relevant country
            AND ga.geo_country_code = GEO_TARGET_COUNTRY_CODE_TARGET
            -- relevance filters
            AND (
                ga.geo_relevance_default = TRUE
                OR ga.users_percent_by_subreddit_l28 >= 0.3
            )
    ),

    subreddits_relevant_to_country AS (
        SELECT
            ga.subreddit_id
            , ga.subreddit_name

            , lan.language_name AS language_name_geo
            , lan.language_percent AS language_percent_geo
            , lan.language_rank AS language_rank_geo

        FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220329` AS ga
            -- Get primary language
            LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_language_rank` AS lan
                ON ga.subreddit_id = lan.subreddit_id

        WHERE 1=1
            -- filters for geo-relevant country
            AND (
                ga.geo_country_code = GEO_TARGET_COUNTRY_CODE_TARGET
                -- relevance filters
                AND (
                    ga.geo_relevance_default = TRUE
                    OR ga.relevance_percent_by_subreddit = TRUE
                    OR ga.users_percent_by_country_standardized >= STANDARDIZED_COUNTRY_THRESHOLD
                )
                -- language filters
                AND (
                    lan.language_name = GEO_TARGET_LANGUAGE
                    AND lan.language_rank IN (1, 2, 3, 4)
                    AND lan.thing_type = 'posts_and_comments'
                    AND lan.language_percent >= 0.05
                )
            )
    ),

    distance_lang_and_relevance_a AS (
        SELECT
            subreddit_id_a AS subreddit_id_geo
            , subreddit_id_b AS subreddit_id_us

            , subreddit_name_a AS subreddit_name_geo
            , subreddit_name_b AS subreddit_name_us
            , cosine_similarity
            , language_name_geo
            , language_percent_geo
            , language_rank_geo

        FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_distances_c_top_100` AS d
            -- Get geo-relevance scores
            INNER JOIN subreddits_relevant_to_country AS ga
                ON d.subreddit_id_a = ga.subreddit_id
            LEFT JOIN subs_relevant_baseline AS gb
                ON d.subreddit_id_b = gb.subreddit_id

        -- Exclude subreddits that are geo-relevant to the country
        WHERE gb.subreddit_id IS NULL
    ),
    distance_lang_and_relevance_a_and_b AS (
        SELECT
            a.* EXCEPT(language_name_geo, language_percent_geo, language_rank_geo)
            , ROW_NUMBER() OVER (PARTITION BY subreddit_id_GEO ORDER BY cosine_similarity DESC) AS rank_geo_to_us

            , slo.subscribers AS subscribers_us
            , language_name_geo, language_percent_geo, language_rank_geo
            , lan.language_name AS primary_language_name_us
            , lan.language_percent AS primary_language_percent_us
        FROM distance_lang_and_relevance_a AS a
            LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220329` AS g
                ON a.subreddit_id_us = g.subreddit_id

            -- Get primary language
            LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_language_rank` AS lan
                ON a.subreddit_id_us = lan.subreddit_id
            -- get subscribers
            LEFT JOIN `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup` AS slo
                ON a.subreddit_id_us = slo.subreddit_id

        WHERE 1=1
            AND slo.dt = (CURRENT_DATE() - 2)
            AND slo.subscribers >= 9000

            -- filters for US counterparts
            AND (
                g.geo_country_code = 'US'
                -- relevance filters
                AND (
                    g.geo_relevance_default = TRUE
                    OR g.users_percent_by_subreddit_l28 >= 0.16
                )
                -- language filters
                AND (
                    lan.language_name = 'English'
                    AND lan.language_rank = 1
                    AND lan.thing_type = 'posts_and_comments'
                    AND lan.language_percent >= 0.6
                )
            )
    )


SELECT
    *
FROM distance_lang_and_relevance_a_and_b
-- FROM distance_lang_and_relevance_a
-- FROM distance_for_country_subs
WHERE 1=1
    AND (
        rank_geo_to_us <= (MAX_COUNTERPARTS_TO_SHOW + 5)
        OR cosine_similarity >= 0.79
    )
    AND rank_geo_to_us <= MAX_COUNTERPARTS_TO_SHOW

    -- testing filters
    -- AND (
    --     subreddit_name_geo IN (
    --         '1fcnuernberg', 'bayernmunich', 'bundesliga', 'fcbayern', 'fussball',
    --         'finanzen',
    --         'germanrap', 'musik', 'rappers',
    --         'buecher', 'harrypotterde',
    --         -- 'filme',
    --         -- 'heutelernteich', 'ich_iel', 'augenbleiche', 'fotografie',
    --         'annode'

    --         -- dead/no longer active
    --         -- , 'lolde', 'kpopde', 'southparkde', 'diesimpsons',

    --     )
    -- )

ORDER BY subreddit_name_geo, cosine_similarity DESC
#  LIMIT 10000
;
