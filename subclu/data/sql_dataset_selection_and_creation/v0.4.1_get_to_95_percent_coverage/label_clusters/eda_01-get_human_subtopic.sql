-- SEO team seems to have a manually labeled table
--  Source is a google sheet (check bigquery)

WITH ratings_topics AS(
  SELECT
    a.*,
    -- b.* EXCEPT (subreddit_name)
  FROM(
    SELECT
      *,
      CASE
        WHEN is_reliable IS TRUE AND is_sensitive IS FALSE THEN TRUE
        ELSE FALSE
      END AS is_acceptable
    FROM
      `data-prod-165221.ds_subreddit_whitelist_tables.alpha_subreddit_topics`
  ) a
  -- FULL OUTER JOIN(
  --   SELECT
  --     *
  --   FROM
  --     `data-prod-165221.ds_v2_subreddit_tables.subreddit_ratings`
  --   WHERE
  --     DATE(pt) = DATE(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))
  -- ) b
  -- ON LOWER(a.subreddit_name) = LOWER(b.subreddit_name)
)

-- Get all the subtopics
SELECT
    DISTINCT
        -- primary_topic,
        subtopic_1
FROM ratings_topics
WHERE 1=1
    -- AND primary_topic IS NOT NULL
    AND subtopic_1 IS NOT NULL
    -- AND COALESCE(primary_topic, '') != COALESCE(subtopic_1, '')
ORDER BY 1
;
