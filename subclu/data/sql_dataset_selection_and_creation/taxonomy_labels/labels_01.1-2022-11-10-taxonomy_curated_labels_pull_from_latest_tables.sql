-- Create view combining taxonomy snapshots & fixing topic names
-- NOTE: 2022-11-10 this view pulls from latest daily snapshot provided by taxonomy

WITH
curator_labels_fix AS (
    SELECT
        * EXCEPT(
            dau, posts_7d, x_rated_percentage, x_rated_posts_7d
            , crowd_topic, crowd_rating, crowd_mature_themes
            , curator_topic, curator_mature_themes
            , geo, ml_rating, ml_topic
            , ads_allowlist_override_reason, ads_allowlist_override_status
        )
        , curator_rating AS curator_rating_name
        , CASE
            WHEN (curator_rating = "Everyone") THEN 'E'
            WHEN (curator_rating = "Sexually Explicit") THEN 'X'
            WHEN (curator_rating = "Mature") THEN 'M'
            -- M1 & M2 replace M. New v6 (around 2022-11)
            WHEN (curator_rating = "Mature 1") THEN 'M1'
            WHEN (curator_rating = "Mature 2") THEN 'M2'
            WHEN (curator_rating = "Violence & Gore") THEN 'V'
            WHEN (curator_rating = "High-Risk Drug Use") THEN 'D'
            ELSE NULL
        END AS curator_rating_short
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
    FROM `data-prod-165221.taxonomy.daily_export`
)

-- Subreddit name can change, so only pull it as last step
SELECT
    l.subreddit_id
    , LOWER(slo.name) AS subreddit_name
    , l.curator_rating_short
    , l.curator_topic
    , l.curator_topic_v2
    , l.curator_rating_name
    , l.* EXCEPT(
        subreddit_id, curator_rating_short, curator_topic
        , curator_topic_v2, curator_rating_name
    )
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
-- ORDER BY slo.subscribers DESC
;

-- Because view can take 20+ seconds to run, create a snapshot table
-- CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.taxonomy_curated_labels` AS (
--     SELECT
--         *
--         , CURRENT_DATE() AS date_retrieved
--     FROM `reddit-employee-datasets.david_bermejo.taxonomy_curated_labels_vw`
-- )
-- ;
