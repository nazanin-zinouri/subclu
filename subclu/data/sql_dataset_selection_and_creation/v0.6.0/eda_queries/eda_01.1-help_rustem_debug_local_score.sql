-- [djb] When a subreddit is only visited by small countries, we need a defualt value
DECLARE STANDARD_VALUE_WHEN_STDEV_ZERO DEFAULT 9.0;


WITH
    -- Subreddit all countries CTEs
    users_in_subreddit_all_countries_l1_info AS (
        SELECT
            pt,
            subreddit_name,
            count(distinct user_id) as users_in_subreddit_all_countries_l1
        FROM `data-prod-165221.all_reddit.all_reddit_subreddits_daily` arsub
        WHERE pt = TIMESTAMP('2022-08-08')
            AND subreddit_name <> 'profile'
        GROUP BY 1, 2
    ),

    users_in_subreddit_all_countries_l28_info AS (
        SELECT
            subreddit_name,
            count(distinct user_id) as users_in_subreddit_all_countries_l28
        FROM `data-prod-165221.all_reddit.all_reddit_subreddits_daily` arsub
        WHERE pt BETWEEN TIMESTAMP_SUB('2022-08-08', INTERVAL 27 DAY) AND TIMESTAMP('2022-08-08')
            AND subreddit_name <> 'profile'
        GROUP BY 1
    ),

    users_in_subreddit_all_countries_l1_l28 AS (
        SELECT
            a.pt,
            a.subreddit_name,
            a.users_in_subreddit_all_countries_l1,
            b.users_in_subreddit_all_countries_l28
        FROM users_in_subreddit_all_countries_l1_info a
        LEFT JOIN users_in_subreddit_all_countries_l28_info b ON a.subreddit_name = b.subreddit_name
        WHERE pt = TIMESTAMP('2022-08-08')
    ),

    -- Subreddit per country CTEs
    users_in_subreddit_from_country_l1_info AS (
        SELECT
            pt,
            geo_country_code,
            subreddit_name,
            count(distinct user_id) as users_in_subreddit_from_country_l1
        FROM `data-prod-165221.all_reddit.all_reddit_subreddits_daily` arsub
        WHERE pt = TIMESTAMP('2022-08-08')
            AND subreddit_name <> 'profile'
        GROUP BY 1, 2, 3
    ),

    users_in_subreddit_from_country_l28_info AS (
        SELECT
            geo_country_code,
            subreddit_name,
            count(distinct user_id) as users_in_subreddit_from_country_l28
        FROM `data-prod-165221.all_reddit.all_reddit_subreddits_daily` arsub
        WHERE pt BETWEEN TIMESTAMP_SUB('2022-08-08', INTERVAL 27 DAY) AND TIMESTAMP('2022-08-08')
            AND subreddit_name <> 'profile'
        GROUP BY 1, 2
    ),

    users_in_subreddit_from_country_l1_l28 AS (
        SELECT
            a.pt,
            a.subreddit_name,
            a.geo_country_code,
            a.users_in_subreddit_from_country_l1,
            b.users_in_subreddit_from_country_l28
        FROM users_in_subreddit_from_country_l1_info a
        LEFT JOIN users_in_subreddit_from_country_l28_info b ON
            LOWER(a.subreddit_name) = LOWER(b.subreddit_name)
            AND a.geo_country_code = b.geo_country_code
        WHERE pt = TIMESTAMP('2022-08-08')
    ),

    -- Country info
    users_from_country_l28_info AS (
        SELECT
            geo_country_code,
            count(distinct user_id) as users_from_country_l28
        FROM `data-prod-165221.all_reddit.all_reddit_subreddits_daily` arsub
        WHERE pt BETWEEN TIMESTAMP_SUB('2022-08-08', INTERVAL 27 DAY) AND TIMESTAMP('2022-08-08')
            AND subreddit_name <> 'profile'
        GROUP BY 1
    )

    -- perc_by_country STD calculation CTEs
    -- [djb] With:
    --    - users_in_subreddit_all_countries_l28_info
    --    - users_in_subreddit_from_country_l28_info
    -- We can calculate:
    --    C) % of users L28 per sub AND per country
    --    D) mean & standardDev per subreddit (from C), for countries above threshold
    --    E) z-score (standardized score)
    -- For the standardized score (z-score) see definition here:
    --   https://docs.google.com/document/d/1rjAZBc8vbbV96rHcf-xHlKZNY14ew1tDMZP2YNLwEyE/edit#bookmark=id.aq5184tq3h4b
    , user_pct_by_sub_and_country AS (
        SELECT
            ua.subreddit_name
            , uc.geo_country_code

            , SAFE_DIVIDE(uc.users_in_subreddit_from_country_l28, ua.users_in_subreddit_all_countries_l28) AS sub_dau_perc_l28
            , SAFE_DIVIDE(uc.users_in_subreddit_from_country_l28, c.users_from_country_l28) AS perc_by_country
        FROM users_in_subreddit_all_countries_l28_info AS ua
            LEFT JOIN  users_in_subreddit_from_country_l28_info AS uc
                ON ua.subreddit_name = uc.subreddit_name
            LEFT JOIN users_from_country_l28_info AS c
                ON uc.geo_country_code = c.geo_country_code
    )

    , mean_and_std_per_sub_capped as (
        SELECT
            up.subreddit_name
            , AVG(sub_dau_perc_l28) as users_perc_by_country_avg
            , COALESCE(STDDEV(up.perc_by_country), 0) as users_perc_by_country_stdev
            , COUNT(distinct up.geo_country_code) AS num_countries_visited_sub
        FROM user_pct_by_sub_and_country AS up
            LEFT JOIN users_from_country_l28_info a
                ON up.geo_country_code = a.geo_country_code
        WHERE a.users_from_country_l28 >= 10100
        GROUP BY 1
    )

    , scores_per_sub_and_country_l28 AS (
        SELECT
            up.subreddit_name
            , up.geo_country_code
            , sub_dau_perc_l28
            , perc_by_country
            , CASE
                -- Zero if only one big country has visits above threshold
                -- NULL if only small countries visit the subreddit
                WHEN (ms.users_perc_by_country_stdev = 0) OR (ms.users_perc_by_country_stdev IS NULL) THEN STANDARD_VALUE_WHEN_STDEV_ZERO
                ELSE SAFE_DIVIDE(
                    (up.perc_by_country - ms.users_perc_by_country_stdev), ms.users_perc_by_country_stdev
                )
            END AS perc_by_country_sd
            , ms.users_perc_by_country_stdev
            , ms.users_perc_by_country_avg
            , ms.num_countries_visited_sub
        FROM user_pct_by_sub_and_country AS up
            LEFT JOIN mean_and_std_per_sub_capped AS ms
                ON up.subreddit_name = ms.subreddit_name
    )

-- --     -- Subreddits info CTEs
--     , subreddit_lookup_data AS (
--         SELECT DISTINCT
--             name AS subreddit_name,
--             subreddit_id,
--             over_18,
--             whitelist_status,
--             verdict,
--             is_deleted,
--             deleted,
--             is_spam,
--         FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
--         WHERE 1=1
--             AND CASE
--                 WHEN '2022-08-08' >= (select date(min(dt)) from `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`) THEN dt = '2022-08-08'
--                 ELSE dt = (select date(min(dt)) from `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`)
--             END
--     )

--     , active_subreddits_all_data AS (
--         SELECT DISTINCT
--             dt,
--             subreddit_name,
--             active,
--             activity_7_day
--         FROM `data-prod-165221.metrics_fact_tables.active_subreddits`
--         WHERE 1=1
--             AND dt >= '2021-01-01' -- BQ requires this partition filter, setting it to start date of the DAG
--     )

--     , active_subreddits_data AS (
--         SELECT
--             subreddit_name,
--             active,
--             activity_7_day
--         FROM active_subreddits_all_data
--         WHERE 1=1
--             AND CASE
--                 WHEN '2022-08-08' >= (select date(min(dt)) from active_subreddits_all_data) THEN dt = '2022-08-08'
--                 ELSE dt = (select date(min(dt)) from active_subreddits_all_data)
--             END
--     )

--     , final AS (
--         SELECT DISTINCT
--             g.pt,
--             s.subreddit_id,
--             g.subreddit_name,
--             g.geo_country_code,
--             users_in_subreddit_from_country_l1 AS sub_dau_l1,
--             users_in_subreddit_from_country_l28 AS sub_dau_l28,
--             SAFE_DIVIDE(users_in_subreddit_from_country_l1, users_in_subreddit_all_countries_l1) AS sub_dau_perc_l1,
--             SAFE_DIVIDE(users_in_subreddit_from_country_l28, users_in_subreddit_all_countries_l28) AS sub_dau_perc_l28,
--             SAFE_DIVIDE(users_in_subreddit_from_country_l28, users_from_country_l28) AS perc_by_country,
--             si.perc_by_country_sd,
--             NOT (
--                 COALESCE(s.verdict, 'f') <> 'admin_removed'
--                 AND COALESCE(s.is_deleted, FALSE) = FALSE
--                 AND s.deleted IS NULL
--             ) AS is_removed,
--             s.is_spam,
--             NOT (
--                 COALESCE(m.rating_short, 'E') = 'E'
--                 AND COALESCE(s.over_18, 'f') = 'f'
--                 AND COALESCE(s.whitelist_status, '') NOT IN ('no_ads', 'promo_adult_nsfw')
--             ) AS nsfw,
--             a.active,
--             a.activity_7_day
--         FROM users_in_subreddit_from_country_l1_l28 g
--         LEFT JOIN users_in_subreddit_all_countries_l1_l28 t ON
--             g.pt = t.pt
--             AND LOWER(g.subreddit_name) = LOWER(t.subreddit_name)
--         LEFT JOIN users_from_country_l28_info c ON g.geo_country_code = c.geo_country_code
--         LEFT JOIN std_info si ON LOWER(g.subreddit_name) = LOWER(si.subreddit_name) AND g.geo_country_code = si.geo_country_code
--         INNER JOIN subreddit_lookup_data s ON
--             LOWER(g.subreddit_name) = LOWER(s.subreddit_name)
--         LEFT JOIN active_subreddits_data a ON
--             LOWER(g.subreddit_name) = LOWER(a.subreddit_name)
--         LEFT JOIN `data-prod-165221.cnc.subreddit_metadata_lookup` m ON
--             LOWER(g.subreddit_name) = LOWER(m.subreddit_name)
--             AND CAST(g.pt AS date) = m.pt
--         WHERE 1=1
--             AND NOT REGEXP_CONTAINS(LOWER(g.subreddit_name), r'^u_.*')
--     )

-- SELECT
--     pt,
--     subreddit_id,
--     subreddit_name,
--     geo_country_code,
--     sub_dau_l1,
--     sub_dau_l28 * 1.0 AS sub_dau_l28,
--     sub_dau_perc_l1,
--     sub_dau_perc_l28,
--     perc_by_country,
--     perc_by_country_sd,
--     is_removed,
--     is_spam,
--     nsfw,
--     active,
--     case
--         when sub_dau_perc_l28 >= 0.25 then 'strict'
--         when (sub_dau_perc_l28 >= 0.20 or perc_by_country_sd > 3) then 'loose'
--         else 'not_local'
--     end as localness,
--     coalesce(activity_7_day, 0) as activity_7_day
-- FROM final


-- djb intermediate test
-- SELECT * FROM users_in_subreddit_from_country_l1_l28;
SELECT
    subreddit_name
    , geo_country_code
    , ROUND(sub_dau_perc_l28, 3) AS sub_dau_perc_l28
    , ROUND(perc_by_country, 3) AS perc_by_country
    , ROUND(perc_by_country_sd, 3) AS perc_by_country_sd
    , users_perc_by_country_stdev
    , users_perc_by_country_avg
    , num_countries_visited_sub
FROM scores_per_sub_and_country_l28
-- FROM user_pct_by_sub_and_country
WHERE 1=1
    AND geo_country_code IN ('DE', 'MX', 'US')
    AND subreddit_name IN (
        'mexico', 'india', 'askreddit', 'cdmx', 'monterrey', 'cancun'
        , 'de', 'meirl', 'ich_iel'
    )
ORDER BY subreddit_name, geo_country_code
;
