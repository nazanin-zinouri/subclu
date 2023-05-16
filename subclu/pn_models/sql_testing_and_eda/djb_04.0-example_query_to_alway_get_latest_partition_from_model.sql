-- Always select the latest partition from PN model output table
--  With this sub-query we can alaways get the latest partition
SELECT
    pt
    , target_subreddit
    , target_subreddit_id
    , user_geo_country_code

FROM `reddit-growth-prod.pn_targeting.pn_model_subreddit_user_click_v1`
WHERE
    pt = (
        SELECT
            DATE(PARSE_TIMESTAMP("%Y%m%d", MAX(partition_id)))
        FROM
          `reddit-growth-prod.pn_targeting`.INFORMATION_SCHEMA.PARTITIONS
        WHERE
          table_name = "pn_model_subreddit_user_click_v1"
    )
;
