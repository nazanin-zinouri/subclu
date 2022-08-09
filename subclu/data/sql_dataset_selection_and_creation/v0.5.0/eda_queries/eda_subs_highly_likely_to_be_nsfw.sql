-- Query to find subreddits likely to NOT amplify
--  HOLD OFF: will use primary topic & rating from CA model for now
SELECT
    subreddit_name
    , users_l7
    , posts_not_removed_l28
    , over_18
    , rating_short
    , whitelist_status
    , primary_topic
    , rating_name
    , geo_relevant_countries
    , subreddit_id
FROM `reddit-relevance.tmp.subclu_subreddits_for_modeling_20220705`
WHERE 1=1
    -- Exclude subs where we don't have enough signal (posts)
    AND posts_not_removed_l28 >= 3

    -- Start by not promoting over_18 or no_ads
    AND (
        COALESCE(whitelist_status, '') = 'no_ads'
        OR COALESCE(over_18, '') = 't'
    )

    AND (
        (
            -- High confidence: not E, but exclude some topics that have a lot of false positives
            --  e.g., tv shows or movies rated as V
            COALESCE(rating_short, '') != 'E'
            AND COALESCE(primary_topic, '') NOT IN (
                'Anime', 'Television', 'Movies', 'Gaming'
            )
        )
        OR (
            -- Sensitive topics
            COALESCE(rating_short, '') = 'E'
            AND COALESCE(primary_topic, '') IN (
                'Trauma Support', "Women's Health", 'Trauma Support'
                , "Sexual Orientation", "Addiction Support"
            )
        )

    )

ORDER BY rating_name, whitelist_status, primary_topic, users_L7 DESC
;
