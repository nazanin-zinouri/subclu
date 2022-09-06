-- Test getting NSFW ->SFW recommendations using top-100 ANN
--  This approach DOES NOT work! NSFW clusters can have 3k+ subreddits
--  So we would need to strop the top-4k+ ANN to _maybe_ get an SFW subreddit
--  TODO(djb): Instead we should:
--    NSFW -> General SFW:
--    - Create an ANN index for only SFW subreddits
--    - Use NSFW subs as input & find the top-100 ANN SFW subs
--    NSFW -> Local SFW:
--    - Create an ANN index for subs that are SFW *and* Local to a country
--    - Use NSFW subs as input & find top-100 ANN SFW *and* local subs

DECLARE TEST_NSFW_SUBREDDITS DEFAULT [
    'thongs', 'yoga_babes'
    -- , 'indiancelebhotscenes'
    -- , 'hottestfemaleathletes', 'sexysportsbabes', 'crossfitgirls'
];

WITH
distance_lang_and_relevance_a AS (
        SELECT
            subreddit_id_a AS subreddit_id_nsfw
            , subreddit_id_b AS subreddit_id_sfw

            , subreddit_name_a AS subreddit_name_nsfw
            , subreddit_name_b AS subreddit_name_sfw
            , cosine_similarity
            , distance_rank
            -- , language_name_geo
            -- , language_percent_geo
            -- , language_rank_geo
            , slo.over_18 AS over_18_nsfw
            , slob.over_18 AS over_18_sfw
            , slo.allow_discovery AS allow_discovery_nsfw
            , nt.rating_short AS rating_short_nsfw
            , primary_topic AS primary_topic_nsfw

        FROM `reddit-employee-datasets.david_bermejo.subclu_v0050_subreddit_distances_c_top_100` AS d
            -- Get geo-relevance scores
            LEFT JOIN (
                SELECT * FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
                WHERE dt = (CURRENT_DATE() - 2)
            ) AS slo
                ON d.subreddit_id_a = slo.subreddit_id
            LEFT JOIN (
                SELECT * FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
                WHERE dt = (CURRENT_DATE() - 2)
            ) AS slob
                ON d.subreddit_id_a = slob.subreddit_id
            LEFT JOIN (
                SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
                WHERE pt = (CURRENT_DATE() - 2)
            ) AS nt
                ON d.subreddit_id_a = nt.subreddit_id
            LEFT JOIN `reddit-employee-datasets.david_bermejo.subreddits_no_recommendation` AS nr
                ON d.subreddit_name_b = nr.subreddit_name

        WHERE 1=1
            -- Exclude subreddits that are geo-relevant to the country
            -- AND gb.subreddit_id IS NULL

            -- exclude subs with covid or corona in title
            -- AND subreddit_name_a NOT LIKE "%covid%"
            -- AND subreddit_name_a NOT LIKE "%coronavirus%"
            -- AND subreddit_name_b NOT LIKE "%covid%"
            -- AND subreddit_name_b NOT LIKE "%coronavirus%"

            -- exclude spam & deleted
            -- AND COALESCE(slo.verdict, 'f') <> 'admin-removed'
            -- AND COALESCE(slo.is_spam, false) = false
            -- AND COALESCE(slo.is_deleted, false) = false
            -- AND COALESCE(slo.quarantine, false) != true

            -- keep only NSFW subs
            -- AND COALESCE(slo.over_18, 'f') = 't'

            -- keep only SFW subs
            -- AND COALESCE(slob.over_18, 'f') != 't'

            -- Test NSFW subreddits
            AND subreddit_name_a IN UNNEST(TEST_NSFW_SUBREDDITS)
    )

SELECT *
FROM distance_lang_and_relevance_a
;
