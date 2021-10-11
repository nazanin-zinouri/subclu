-- These queries help us identify subreddits that are rated as E, but have NSFW content
--  example: rating=E & nt.primary_topic = 'Mature Themes and Adult Content'

-- EDA subs classified as E, but NSFW
DECLARE partition_date DATE DEFAULT '2021-10-09';

WITH
    verified_subreddits AS (
        select
            subreddit_id
            , verification_time
            , survey_version
            , tag_type
            , status
        FROM
        `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsource_verification_status`
        WHERE
        pt = partition_date
        AND status = 'verified'
        AND tag_type = 'rating'
        and date(verification_time) >= '2021-07-25'
    )

SELECT
    acs.subreddit_id
    , COALESCE(acs.subreddit_name, asr.subreddit_name) AS subreddit_name

    -- Rating info
    , vs.status AS verification_status
    , rating_short
    , rating_name
    , primary_topic
    , array_to_string(secondary_topics,", ") as secondary_topics
    , array_to_string(mature_themes,", ") as mature_themes
    , nt.pt AS new_rating_pt

    , vs.* EXCEPT (subreddit_id, status)

    , nt.* EXCEPT (
        liveness_ts, subreddit_id, pt
        , rating_short
        , rating_name
        , primary_topic
        , secondary_topics
        , mature_themes
    )

FROM (
    SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
    WHERE DATE(pt) = partition_date
) AS asr
    LEFT JOIN (
        SELECT * FROM `data-prod-165221.ds_subreddit_whitelist_tables.active_subreddits`
        WHERE DATE(_PARTITIONTIME) = partition_date
    ) AS acs
        ON asr.subreddit_name = acs.subreddit_name

    LEFT JOIN (
        SELECT * FROM `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsourced_topic_and_rating`
        WHERE pt = partition_date
    ) AS nt
        ON acs.subreddit_id = nt.subreddit_id
    LEFT JOIN verified_subreddits AS vs
        ON vs.subreddit_id = nt.subreddit_id

    -- LEFT JOIN (
    --     SELECT * FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
    --     WHERE dt = partition_date
    -- ) AS slo
    --     ON asr.subreddit_name = LOWER(slo.name)

WHERE 1=1
    -- AND acs.activity_7_day > 1

    -- Results with E + mature themes = 3,873
    AND nt.rating_short = 'E'
    AND nt.primary_topic = 'Mature Themes and Adult Content'

    -- adding `verified` flag: 1,491
    -- AND vs.status = 'verified'

-- WHERE 1=1
--     AND nt.rating_short = 'E'
--     AND (
--         'sex_porn' IN UNNEST(mature_themes)
--         OR 'sex_content_arousal' IN UNNEST(mature_themes)
--         OR 'nudity_explicit' IN UNNEST(mature_themes)
--         OR 'nudity_full' IN UNNEST(mature_themes)
--     )

ORDER BY 2 ASC
;
