-- Combine ambassador and core subreddits tables into a single view so we can simplify queries
-- There is no single table, but there are two sources from spreadsheets
--  So use this view/table to reduce the amount of custom queries

-- CREATE OR REPLACE VIEW `reddit-employee-datasets.david_bermejo.ambassador_subreddits_union_vw` AS
CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.ambassador_subreddits_union_20211216` AS
    WITH combined_ambassador_table AS (
        SELECT
            COALESCE(n.subreddit_id, o.subreddit_id) AS subreddit_id
            , LOWER(COALESCE(n.subreddit_name, o.subreddit_name)) AS subreddit_name

            , COALESCE(n.topic, o.topic)  AS i18n_topic

            , CASE
                WHEN (n.type IS NOT NULL) THEN n.type
                WHEN (o.subreddit_id IS NOT NULL) THEN "ambassador"
                ELSE NULL
                END AS i18n_type
            , CASE
                WHEN (n.country IS NOT NULL) THEN n.country
                WHEN (o.subreddit_id IS NOT NULL) THEN "DE"
                ELSE NULL
                END AS i18n_country_code

            , n.owner  AS i18n_owner
            , n.type_2 AS i18n_type_2

        FROM `reddit-employee-datasets.barbara_jun.amb_prog_communities` AS n
        FULL OUTER JOIN (
                -- Wacy's table pulls data from a spreadsheet that Alex updates
                SELECT
                    amb.subreddit_name
                    , slo.subreddit_id
                    , subreddit_info AS topic
                    , topic AS notes_old

                FROM (
                    SELECT
                        LOWER(subreddit_name) as subreddit_name
                        ,* EXCEPT(subreddit_name)
                    FROM `reddit-employee-datasets.wacy_su.ambassador_subreddits`
                ) AS amb
                LEFT JOIN (
                    SELECT
                        LOWER(name) AS subreddit_name
                        , subreddit_id
                    FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
                    WHERE dt = (CURRENT_DATE() - 2)
                ) AS slo
                    ON amb.subreddit_name = slo.subreddit_name
                WHERE amb.subreddit_name IS NOT NULL
            ) AS o
            ON LOWER(n.subreddit_name) = LOWER(o.subreddit_name) AND n.subreddit_id = o.subreddit_id
    )


SELECT
    *
    , CASE
        WHEN (i18n_country_code = 'DE') THEN TRUE
        ELSE FALSE
        END AS ambassador_or_default_sub_germany
    , CASE
        WHEN (i18n_country_code = 'FR') THEN TRUE
        ELSE FALSE
        END AS ambassador_or_default_sub_france

FROM combined_ambassador_table
ORDER BY i18n_type, subreddit_name
;
