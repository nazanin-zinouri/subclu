-- This one used a lot of "custom" local metrics, so they are not stored in the experiment table
SELECT
    pt
    , experiment_id
    , experiment_version
    , experiment_name
    , experiment_variant
    , last_experiment_date
    , metric
    , breakout_category
    , breakout_value
    , SAFE_DIVIDE(te_winsorized, ctr_mean_winsorized) AS te_winsorized_pct
    , (ABS(z_score_winsorized) > 1.96) AS significant_at_p05
    , mean_winsorized
    -- , te_winsorized
    -- , ctr_mean_winsorized
    -- , z_score_winsorized

FROM `data-prod-165221.experiments.experiment_results_4950`
WHERE DATE(pt) = "2022-07-04"
    AND experiment_version = "3"
    AND is_control = false
    AND experiment_variant = 'en_fprs_add_to_existing'
    AND (
        breakout_value IN ('gb', 'uk')
        OR breakout_category = 'all'
    )
    AND (
        metric LIKE "%local%"
        OR metric IN (
            'total_local_screenviews_count'
            , 'eaadau'
            , 'performance_tti_avg_latency_over_valid_events_p90'
            , 'subscribe_rate'
            , 'distinct_subreddits_consumed'
            , 'video_view'
            , 'bad_content_interaction_rate'
            , 'good_visits_feeds'
            , 'd1_retention'
            , 'd4_retention'
            , 'd7_retention'
            , 'w0_retention'
            , 'local_post_consumes_double_avg'
            , 'total_local_subscribes_count'
            , 'total_local_posts_count'
            , 'total_local_comments_count'
        )
    )


ORDER BY metric, breakout_value
;
