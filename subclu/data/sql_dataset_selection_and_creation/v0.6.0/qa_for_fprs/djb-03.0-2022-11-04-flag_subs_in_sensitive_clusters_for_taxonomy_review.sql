-- Purpose:
-- After a chat with Spiros, he wanted to have a more targeted approach for i18n subreddits
--  that might be NSFW. With this query, I flag subreddits that are in a sensitive cluster
--  - might be unrated
--  - might be mis-rated
-- NOTE:
-- We expect some false positives -- especially for celebrity and some fashion subreddits

DECLARE latest_pt_date date;
-- Get the latest partition date in the table
set latest_pt_date = (
    select
        date(parse_timestamp('%Y%m%d', max(partition_id)))
    from `reddit-employee-datasets.david_bermejo.INFORMATION_SCHEMA.PARTITIONS`
    where 1=1
        AND table_name = "subclu_v0050_subreddit_clusters_c_qa_flags"
        AND COALESCE(partition_id, '') not in ("__NULL__", "__UNPARTITIONED__")
);


SELECT
    pt
    , subreddit_id
    , over_18
    , geo_relevant_countries
    , users_l7
    , subreddit_name
    , combined_filter
    , combined_filter_reason
    , rating_short
    , primary_topic
    , predicted_rating
    , predicted_topic
    , sensitive_cluster_filter
    , taxonomy_action

FROM `reddit-employee-datasets.david_bermejo.subclu_v0050_subreddit_clusters_c_qa_flags`
WHERE pt = latest_pt_date
    AND geo_relevant_country_codes LIKE "%DE%"
    -- AND combined_filter != 'recommend'
    AND COALESCE(sensitive_cluster_filter, '') = 'remove'
    AND (
        COALESCE(over_18, '') != 't'
        AND COALESCE(rating_short, '') != 'X'
    )

ORDER BY users_l7 DESC
;
