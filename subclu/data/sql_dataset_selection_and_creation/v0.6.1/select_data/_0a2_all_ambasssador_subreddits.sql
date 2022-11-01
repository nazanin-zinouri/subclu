-- Combine ambassador and core subreddits tables into a single view so we can simplify queries
-- There is no single table, but there are two sources from spreadsheets
--  So use this view/table to reduce the amount of custom queries
DECLARE PARTITION_DATE DATE DEFAULT ${end_date};

CREATE OR REPLACE TABLE `reddit-relevance.${dataset}.ambassador_subreddits_union_${run_id}` AS
    WITH combined_ambassador_table AS (
        SELECT
            DISTINCT
            COALESCE(n.subreddit_id, o.subreddit_id) AS subreddit_id
            , LOWER(COALESCE(n.subreddit_name, o.subreddit_name)) AS subreddit_name

            , COALESCE(n.topic, o.topic)  AS i18n_topic

            , CASE
                WHEN (n.type IS NOT NULL) THEN n.type
                WHEN (o.subreddit_id IS NOT NULL) THEN "ambassador"
                ELSE NULL
                END AS i18n_type
            , n.type_2 AS i18n_type_2
            , CASE
                WHEN (n.country IS NOT NULL) THEN n.country
                WHEN (o.subreddit_id IS NOT NULL) THEN "DE"
                ELSE NULL
                END AS i18n_country_code

        FROM (
            -- Barbara's table has a duplicate so we need to remove it
            SELECT *
            FROM `reddit-employee-datasets.barbara_jun.amb_prog_communities`
            WHERE 1=1
                AND (
                    LOWER(subreddit_name) != 'vegetarischde'
                    AND LOWER(type) != 'ambassador'
                )
                AND subreddit_id IS NOT NULL
                AND subreddit_id != 'subreddit_id'
        ) AS n
        FULL OUTER JOIN (
                -- Wacy's table pulls data from a spreadsheet that Alex used to update
                SELECT
                    amb.subreddit_name
                    , slo.subreddit_id
                    , topic

                FROM (
                    SELECT
                        LOWER(subreddit_name) as subreddit_name
                        , * EXCEPT(subreddit_name)
                    FROM `reddit-employee-datasets.wacy_su.ambassador_subreddits`
                ) AS amb
                LEFT JOIN (
                    SELECT
                        LOWER(name) AS subreddit_name
                        , subreddit_id
                    FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
                    WHERE dt = PARTITION_DATE
                ) AS slo
                    ON amb.subreddit_name = slo.subreddit_name
                WHERE amb.subreddit_name IS NOT NULL
            ) AS o
            ON n.subreddit_id = o.subreddit_id
    )


SELECT
    *

FROM combined_ambassador_table

WHERE 1=1
    AND subreddit_id IS NOT NULL
    AND subreddit_id != 'subreddit_id'

ORDER BY i18n_country_code, i18n_type, subreddit_name
;
