-- Pull latest subreddit tags
-- As of 2021-08-16 only pull the top-level topic, need to do more work to pull sub-topics

-- See new info on data tables here:
-- https://reddit.atlassian.net/wiki/spaces/SIG/pages/2122809839/Tagging+Data+Tables

SELECT
    nt.subreddit_id
    , slo.name AS subreddit_name
    , nt.rating_short
    , nt.rating_name
    , nt.rating_weight
    , nt.primary_topic
    -- , nt.secondary_topics  -- Exclude for now b/c it's nested
    -- , nt.mature_themes  -- Exclude for now b/c it's nested
    , nt.survey_version
    , nt.pt AS pt_new_topic

FROM `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsourced_topic_and_rating` AS nt
LEFT JOIN (
    SELECT * FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
    # Look back 2 days because looking back 1-day could be an empty partition
    WHERE dt = (CURRENT_DATE() - 2)
) AS slo
    ON nt.subreddit_id = slo.subreddit_id

WHERE nt.pt = (CURRENT_DATE) -- nt.pt = (CURRENT_DATE() - 1)
    AND nt.primary_topic IS NOT NULL

ORDER BY nt.survey_version DESC, nt.rating_short ASC, nt.subreddit_id ASC
-- LIMIT 1000
;
