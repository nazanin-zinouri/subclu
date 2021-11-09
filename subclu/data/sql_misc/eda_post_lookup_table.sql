
-- See wiki:
-- https://reddit.atlassian.net/wiki/spaces/DataScience/pages/2172878898/EDA+post+lookup+table

DECLARE lookup_pt_date DATE DEFAULT '2021-09-07';
DECLARE post_created_start DATE DEFAULT '2021-09-01';
DECLARE post_created_end DATE DEFAULT '2021-09-07';

-- SELECT
--     post_id
--     , upvotes
--     , downvotes
--     , (upvotes - downvotes) AS net_votes
--     , created_timestamp
--     , deleted
--     , neutered
--     , over_18
--     , selftext_is_richtext
--     , promoted  # we'd like to exclude promoted ads from topic clustering for now
--     , author_id
--     , subreddit_id
--     , language_preference
--     , verdict
    -- , flair_text

-- FROM `data-prod-165221.ds_v2_postgres_tables.post_lookup` AS plo

-- WHERE DATE(plo._PARTITIONTIME) = lookup_pt_date
--     AND DATE(created_timestamp) BETWEEN post_created_start AND post_created_end
-- ;

-- Get count of posts by
-- neutered = true -> marked as spam
-- verdict = was it flagged?
-- reported = reason for post being reported ??
-- content_category = category assigned to post (not sure about source)
SELECT
    -- neutered
    verdict
    , promoted
    -- flair_text

    -- , COUNT(*) AS row_count
    , COUNT(DISTINCT post_id) AS post_id_unique_count

FROM `data-prod-165221.ds_v2_postgres_tables.post_lookup` AS plo

WHERE DATE(plo._PARTITIONTIME) = lookup_pt_date
    AND DATE(created_timestamp) BETWEEN post_created_start AND post_created_end

-- 2 categories
GROUP BY 1, 2
ORDER BY 1, 2, 3 DESC

-- one category
-- GROUP BY 1
-- ORDER BY 2 DESC
;


-- Get unique count of flair-text labels
-- SELECT
--     COUNT(DISTINCT post_id) AS post_id_unique_count
--     , SUM(CASE WHEN flair_text IS NULL THEN 0 ELSE 1 END) posts_with_flair_text
--     , COUNT(DISTINCT flair_text) AS flair_text_unique_count

-- FROM `data-prod-165221.ds_v2_postgres_tables.post_lookup` AS plo

-- WHERE DATE(plo._PARTITIONTIME) = lookup_pt_date
--     AND DATE(created_timestamp) BETWEEN post_created_start AND post_created_end
-- ;

