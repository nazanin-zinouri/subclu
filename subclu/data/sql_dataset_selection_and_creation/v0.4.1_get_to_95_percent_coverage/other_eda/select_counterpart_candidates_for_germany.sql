-- Initial candidate list for subreddit counterparts
DECLARE GEO_TARGET_COUNTRY_CODE_TARGET STRING DEFAULT "DE";
DECLARE GEO_TARGET_LANGUAGE STRING DEFAULT "German";

DECLARE MIN_COUNTERPARTS_TO_SHOW NUMERIC DEFAULT 8;
DECLARE MAX_COUNTERPARTS_TO_SHOW NUMERIC DEFAULT 15;
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
            LEFT JOIN `reddit-employee-datasets.david_bermejo.subreddits_no_recommendation` AS nr
                ON ga.subreddit_name = nr.subreddit_name

        WHERE 1=1
            -- remove subreddits flagged as sensitive
            AND nr.subreddit_name IS NULL

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
            , over_18 AS over_18_geo
            , slo.allow_discovery AS allow_discovery_geo
            , nt.rating_short AS rating_short_geo
            , primary_topic AS primary_topic_geo

        FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_distances_c_top_100` AS d
            -- Get geo-relevance scores
            INNER JOIN subreddits_relevant_to_country AS ga
                ON d.subreddit_id_a = ga.subreddit_id
            LEFT JOIN subs_relevant_baseline AS gb
                ON d.subreddit_id_b = gb.subreddit_id
            LEFT JOIN (
                SELECT * FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
                WHERE dt = (CURRENT_DATE() - 2)
            ) AS slo
                ON d.subreddit_id_a = slo.subreddit_id
            LEFT JOIN (
                SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
                WHERE pt = (CURRENT_DATE() - 2)
            ) AS nt
                ON ga.subreddit_id = nt.subreddit_id
            LEFT JOIN `reddit-employee-datasets.david_bermejo.subreddits_no_recommendation` AS nr
                ON d.subreddit_name_b = nr.subreddit_name

        WHERE 1=1
            -- Exclude subreddits that are geo-relevant to the country
            AND gb.subreddit_id IS NULL
            -- remove subreddits flagged as sensitive
            AND nr.subreddit_name IS NULL

            -- exclude subs with covid or corona in title
            AND subreddit_name_a NOT LIKE "%covid%"
            AND subreddit_name_a NOT LIKE "%coronavirus%"
            AND subreddit_name_b NOT LIKE "%covid%"
            AND subreddit_name_b NOT LIKE "%coronavirus%"

            -- exclude other subs
            AND COALESCE(verdict, 'f') <> 'admin-removed'
            AND COALESCE(is_spam, false) = false
            AND COALESCE(is_deleted, false) = false
            AND COALESCE(over_18, 'f') != 't'
    ),
    distance_lang_and_relevance_a_and_b AS (
        SELECT
            a.* EXCEPT(
                language_name_geo, language_percent_geo, language_rank_geo,
                over_18_geo, rating_short_geo, primary_topic_geo, allow_discovery_geo
            )
            , ROW_NUMBER() OVER (PARTITION BY subreddit_id_GEO ORDER BY cosine_similarity DESC) AS rank_geo_to_us

            , slo.subscribers AS subscribers_us

            , allow_discovery_geo
            , rating_short_geo
            , nt.rating_short AS rating_short_us
            , primary_topic_geo
            , primary_topic AS primary_topic_us

            , language_name_geo, language_percent_geo, language_rank_geo
            , lan.language_name AS primary_language_name_us
            , lan.language_percent AS primary_language_percent_us
            , over_18_geo
            , over_18 AS over_18_us

        FROM distance_lang_and_relevance_a AS a
            LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220329` AS g
                ON a.subreddit_id_us = g.subreddit_id

            -- Get primary language
            LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_language_rank` AS lan
                ON a.subreddit_id_us = lan.subreddit_id
            -- get subscribers
            LEFT JOIN `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup` AS slo
                ON a.subreddit_id_us = slo.subreddit_id
            LEFT JOIN (
                SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
                WHERE pt = (CURRENT_DATE() - 2)
            ) AS nt
                ON a.subreddit_id_us = nt.subreddit_id

        WHERE 1=1
            AND slo.dt = (CURRENT_DATE() - 2)

            -- filters for US counterparts
            AND slo.subscribers >= 9000
            AND COALESCE(verdict, 'f') <> 'admin-removed'
            AND COALESCE(is_spam, false) = false
            AND COALESCE(is_deleted, false) = false
            AND COALESCE(over_18, 'f') != 't'

            -- more filters for US counterparts
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

            -- Add filters for ratings & primary topics
            -- AND COALESCE(rating_short_geo, '') = 'E'
            -- AND COALESCE(nt.rating_short, '') = 'E'

            AND COALESCE(primary_topic_geo, '') NOT IN (
                'Place',
                'Celebrity',
                -- 'Mature Themes and Adult Content',
                'Sexual Orientation', 'Gender',
                'Medical and Mental Health', 'Medical and Mental Health', 'Addiction Support',
                'Politics', 'Military'
            )
            AND COALESCE(nt.primary_topic, '') NOT IN (
                'Celebrity',
                -- 'Mature Themes and Adult Content',
                'Sexual Orientation', 'Gender',
                'Medical and Mental Health', 'Medical and Mental Health', 'Addiction Support',
                'Politics', 'Military'
            )

    )


SELECT
    d.* EXCEPT(over_18_geo, over_18_us)
    , sc.model_sort_order AS model_sort_order_geo
FROM distance_lang_and_relevance_a_and_b AS d
    -- join with cluster table so we can sort similar subreddits together
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` AS sc
        ON d.subreddit_id_geo = sc.subreddit_id
WHERE 1=1
    -- Exclude geo-subreddits that don't want to be discovered
    -- AND COALESCE(allow_discovery_geo, '') != 'f'

    AND (
        rank_geo_to_us <= MIN_COUNTERPARTS_TO_SHOW
        OR cosine_similarity >= 0.79
    )
    AND rank_geo_to_us <= MAX_COUNTERPARTS_TO_SHOW

ORDER BY sc.model_sort_order DESC, subreddit_name_geo, subscribers_us DESC, cosine_similarity DESC
;
