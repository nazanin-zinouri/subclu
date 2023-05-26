-- Check trending PN info for target users selected by user<>subreddit model
-- BUG/question to answer: Why do we go from 300k to 67k users by removing users who have PNs turned off?

SELECT
    sel.* EXCEPT(user_rank_by_sub_and_geo, user_geo_country_code)
    , r.* EXCEPT(user_id)
    , subscribed
    , legacy_user_cohort_ord
    , user_receives_pn_t7, user_clicks_pn_t7, user_clicks_trnd_t7
    , tos_30_pct, tos_30_sub_count
    , screen_view_count_14d_log
FROM `reddit-employee-datasets.david_bermejo.pn_ft_all_20230509` AS ft
    INNER JOIN `reddit-employee-datasets.david_bermejo.pn_zelda_target_users_20230511` AS sel
        ON ft.user_id = sel.user_id
            AND ft.target_subreddit = sel.target_subreddit
            AND ft.pt = sel.pt
    -- Join to get device IDs & suppressed receives
    LEFT JOIN (
        SELECT
            pr.user_id
            , device_id
            , app_name
            , is_suppressed

            , COUNT(pr.endpoint_timestamp) AS receive_count

        FROM channels.pn_receives AS pr
            INNER JOIN `reddit-employee-datasets.david_bermejo.pn_zelda_target_users_20230511` AS sel
                ON pr.user_id = sel.user_id
        WHERE DATE(pr.pt) >= (current_date() - 14)
            AND device_id IS NOT NULL
        GROUP BY 1,2,3,4
    ) AS r
        ON sel.user_id = r.user_id

ORDER BY is_suppressed, click_proba DESC, user_id, device_id
;
