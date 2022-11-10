-- Create view combining taxonomy snapshots & fixing topic names
-- NOTE: this uses old snapshots (2022-08-05 or earlier)

WITH
curator_labels_raw AS (
    -- NOTE1: We pick the most recent label when available
    -- Note2: Some topics are misnamed, unclear if it's a problem with the raw data or datadump
    SELECT
        COALESCE(s2.subreddit_id, s1.Subreddit_ID) AS subreddit_id
        , COALESCE(s2.curator_rating_short, s1.Curator_Rating) AS curator_rating
        , COALESCE(s2.curator_topic, s1.Curator_Topic) AS curator_topic
        , COALESCE(s2.date_retrieved, CAST("2022-08-05" AS DATE)) AS date_retrieved

        -- NOTE: s2 doesn't have blocklist status
        , s1.Blocklist_Status AS blocklist_status
        , s1.Blocklist_Reason AS blocklist_reason
        , CASE
            WHEN s1.Blocklist_Status IS NULL THEN NULL
            ELSE "2022-08-05"
        END AS blocklist_dt
    FROM `reddit-employee-datasets.amy_jeffrey.taxonomy_2022_08_05_v0` AS s1
        FULL OUTER JOIN (
            SELECT *
            FROM `reddit-employee-datasets.david_bermejo.taxonomy_curated_snapshot_20220817`
            WHERE
                -- Drop subreddits that don't have ANY labels
                curator_topic IS NOT NULL
                AND curator_rating IS NOT NULL
        ) AS s2
            ON s2.subreddit_id = s1.Subreddit_ID
)
, curator_labels_fix AS (
    SELECT
        * EXCEPT(curator_topic)
        , CASE
            WHEN (curator_topic = "N/A") THEN NULL
            WHEN (curator_topic = "admin test sub") THEN NULL
            WHEN (curator_topic = "test") THEN NULL

            WHEN (curator_topic = ",Business, Economics, and Finance") THEN "Business, Economics, and Finance"
            WHEN (curator_topic = ",Mature Themes and Adult Content") THEN "Mature Themes and Adult Content"
            WHEN (curator_topic = ",Music") THEN "Music"
            WHEN (curator_topic = "Animals And Pets") THEN "Animals and Pets"
            WHEN (curator_topic = "Internet Culture And Memes") THEN "Internet Culture and Memes"

            ELSE curator_topic
        END AS curator_topic
    FROM curator_labels_raw
)


-- Subreddit name can change, so only pull it as last step
SELECT
    l.subreddit_id
    , LOWER(slo.name) AS subreddit_name
    , l.curator_rating
    , l.curator_topic
    , l.* EXCEPT(subreddit_id, curator_rating, curator_topic)
FROM curator_labels_fix AS l
    LEFT JOIN (
      SELECT
          subreddit_id
          , name
          , subscribers
      FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
      WHERE dt = (CURRENT_DATE() - 2)
    ) AS slo
        ON l.subreddit_id = slo.subreddit_id
ORDER BY slo.subscribers DESC
;
