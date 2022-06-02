-- Get geo-relevant subreddits for the US and for target countries
--  Use it to compare topic coverage

WITH
    geo_targets AS (
        SELECT
            t.subreddit_id
            , t.subreddit_name
            , g.country_name
            , g.geo_country_code
            , d_users_percent_by_country_rank AS subreddit_rank_in_country
            , geo_relevance_default
            , b_users_percent_by_subreddit AS users_percent_by_subreddit_l28
            , e_users_percent_by_country_standardized AS users_percent_by_country_standardized

        FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_score_standardized_20220212` AS g
            -- inner join to keep only subs that are in topic model
            INNER JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full` AS t
                ON g.subreddit_id = t.subreddit_id

        WHERE 1=1
            AND (
                geo_relevance_default = TRUE
                OR relevance_percent_by_subreddit = TRUE
                OR e_users_percent_by_country_standardized >= 3.0
            )
            AND geo_country_code IN ('DE', 'MX', 'FR')

          -- d_users_percent_by_country_rank = subreddit_rank_in_country
        -- ORDER BY country_name, d_users_percent_by_country_rank ASC
    )
    , us_baseline AS (
        SELECT
            t.subreddit_id
            , t.subreddit_name
            , g.country_name
            , g.geo_country_code
            , subreddit_rank_in_country
            , geo_relevance_default
            , users_percent_by_subreddit_l28
            , users_percent_by_country_standardized

        FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220329` AS g
            -- inner join to keep only subs that are in topic model
            INNER JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full` AS t
                ON g.subreddit_id = t.subreddit_id

        WHERE 1=1
            AND (
                geo_relevance_default = TRUE
            )
            AND geo_country_code IN ('US')

          -- d_users_percent_by_country_rank = subreddit_rank_in_country
        -- ORDER BY country_name, subreddit_rank_in_country ASC
    )
    , all_subs_union AS (
        SELECT * FROM geo_targets
        UNION ALL
        SELECT * FROM us_baseline

        ORDER BY country_name, subreddit_rank_in_country ASC
    )


SELECT * FROM all_subs_union
;
