-- Check # of potential targets for target subreddit (latest pt in prod table)
SELECT
    m.pt
    , m.target_subreddit
    , m.user_geo_country_code
    , m.subscribed
    , COUNT(DISTINCT t.user_id) AS user_count
FROM `reddit-growth-prod.pn_targeting.pn_model_subreddit_user_click_v1` AS m
    LEFT JOIN UNNEST(top_users) AS t

WHERE
    pt = (
        SELECT DATE(PARSE_TIMESTAMP("%Y%m%d", MAX(partition_id)))
        FROM `reddit-growth-prod.pn_targeting`.INFORMATION_SCHEMA.PARTITIONS
        WHERE table_name = "pn_model_subreddit_user_click_v1"
    )
    AND target_subreddit IN ('streetfighter')
    AND user_geo_country_code IN (
        'GB','FR','DE','MX','IN'
    )
GROUP BY 1,2,3,4
ORDER BY user_count DESC
;
