-- Export inference data to GCS
--  ETA:  7 minutes for 3.4 Billion rows, ~5k files. Slot: 20 hours
EXPORT DATA OPTIONS(
    uri='gs://i18n-subreddit-clustering/pn_model/runs/inference/20230603/*.parquet',
    format='PARQUET',
    overwrite=true
) AS
SELECT
    -- Index columns for output (subscribed also used for input)
    pt, target_subreddit, target_subreddit_id, subscribed, user_geo_country_code, user_id

    -- Input columns for model:
    , screen_view_count_14d_log, user_receives_pn_subreddit_count_t7, user_receives_pn_t7
    , user_receives_pn_t14, user_receives_pn_t30, log_user_clicks_pn_t7, log_user_clicks_trnd_t7
    , log_post_consumes_30, log_num_post_consumes_home_30, pct_post_consumes_home_30
    , pct_post_consumes_community_30, pct_post_consumes_post_detail_30, pct_post_consumes_ios_30
    , pct_post_consumes_android_30, pct_post_consumes_nsfw_30, num_post_consumes_ios_30
    , num_post_consumes_android_30, sub_dau_perc_l28, perc_by_country_sd, us_screenviews_l14_log
    , us_distinct_posts_viewed_l14_log, us_post_screenviews_l14_log, us_trend_pn_receive_l14_log
    , us_days_active_ios_l30_pct, us_android_days_active_l30_pct, us_upvotes_l14, us_comments_l14
    , us_posts_l14, us_trend_pn_click_l14, us_sessions_l14, us_ios_days_active_l30
    , us_android_days_active_l30, us_days_active_l30, users_log_l28, seo_users_pct_l28
    , loggedin_users_pct_l28, ios_users_pct_l28, android_users_pct_l28, legacy_user_cohort_ord
FROM `reddit-employee-datasets.david_bermejo.pn_ft_all_20230530` AS f

WHERE pt = '2023-06-03'
;
