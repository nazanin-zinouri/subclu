-- Also try to keep queries in wiki so they're easier for people to use & find them:
--   https://reddit.atlassian.net/wiki/spaces/DataScience/pages/2113142796/How+to+Query+Model+Outputs

-- Get counts of subs by manual label
SELECT
    manual_topic_and_rating
    , COUNT(DISTINCT subreddit_name) AS unique_subreddit_count
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v031_a`
GROUP BY 1
ORDER BY 2 DESC
;

-- Get most similar subs, given an input sub name
DECLARE SUB_NAME STRING;
SET SUB_NAME = 'bundesliga';

-- Get most similar subs, given an input sub name
SELECT * EXCEPT(subreddit_id_a, subreddit_id_b)
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_distance_v0031_german_c_posts_and_comments_and_meta`
WHERE subreddit_name_a = SUB_NAME
;


-- Get the cluster ID & cluster subs, given an input sub name
DECLARE SUB_NAME STRING;
SET SUB_NAME = 'bundesliga';

WITH cluster_for_selected_sub AS(
SELECT
    cluster_id_agg_ward_cosine_35
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v031_a`
WHERE subreddit_name = SUB_NAME
)

SELECT
    subreddit_name
    , manual_topic_and_rating
    , lbl.cluster_id_agg_ward_cosine_35
    , post_median_word_count
    , German_posts_percent
    , subreddit_language
    , posts_l28
    , subscribers
    , users_l7
    , users_l28
    , subreddit_title
    , subreddit_public_description

    , svd_0
    , svd_1
    , svd_2

FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v031_a` AS lbl
INNER JOIN cluster_for_selected_sub AS sel
    ON lbl.cluster_id_agg_ward_cosine_35 = sel.cluster_id_agg_ward_cosine_35

WHERE manual_topic_and_rating != 'over18_nsfw'
ORDER BY cluster_id_agg_ward_cosine_35 ASC, users_l28 DESC
;


-- Find out which subs have been rated
--  For now, it's static labels, but will need to join with live data (maybe a view?)
-- SELECT
--     whitelist_status
--     , COUNT(DISTINCT subreddit_name) AS unique_subreddit_count
-- FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v031_a`
-- GROUP BY 1
-- ORDER BY 2 DESC
-- ;

SELECT
    rating
    -- , whitelist_status
    , COUNT(DISTINCT subreddit_name) AS unique_subreddit_count
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v031_a`
GROUP BY 1
ORDER BY 2 DESC
;

SELECT
    rating
    , whitelist_status
    , COUNT(DISTINCT subreddit_name) AS unique_subreddit_count
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v031_a`
GROUP BY 1, 2
ORDER BY 1 ASC, 3 DESC
;


-- Select key cols for subs that are in the allow-list for Ads
SELECT
    subreddit_name
    , subreddit_id
    , manual_topic_and_rating
    , lbl.cluster_id_agg_ward_cosine_35
    , post_median_word_count
    , German_posts_percent
    , subreddit_language
    , posts_l28
    , subscribers
    , users_l7
    , users_l28
    , subreddit_title
    , subreddit_public_description

    , svd_0
    , svd_1
    , svd_2

FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v031_a` AS lbl

WHERE whitelist_status IN ('all_ads', 'some_ads')
ORDER BY cluster_id_agg_ward_cosine_35 ASC, users_l28 DESC
;


-- Join with latest ratings information
-- As of 2021-08-10, it doesn't look like ratings changed but maybe
-- that's because there's a new table/format for new ratings
SELECT
    lbl.subreddit_name
    , lbl.subreddit_id
    , rt.rating
    , rt.version    AS rating_version
    , lbl.rating AS rating_from_july
    , (rt.rating != lbl.rating)  AS rating_change
    , manual_topic_and_rating
    , lbl.cluster_id_agg_ward_cosine_35
    , post_median_word_count
    , German_posts_percent
    , subreddit_language
    , posts_l28
    , subscribers
    , users_l7
    , users_l28
    , subreddit_title
    , subreddit_public_description

    , svd_0
    , svd_1
    , svd_2

FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v031_a` AS lbl

LEFT JOIN (
    SELECT * FROM `data-prod-165221.ds_v2_subreddit_tables.subreddit_ratings`
    WHERE DATE(pt) = (CURRENT_DATE() - 2)
) AS rt
    ON lbl.subreddit_name = rt.subreddit_name

WHERE rt.rating IN ('g', 'pg', 'pg13')
ORDER BY rating_change ASC, cluster_id_agg_ward_cosine_35 ASC, users_l28 DESC
;


-- Query that adds latest ratings from new tagging/rating table!
WITH cluster_for_selected_sub AS(
SELECT
    cluster_id_agg_ward_cosine_200
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v032_a`
WHERE subreddit_name = 'bundesliga'
)

SELECT
    subreddit_name
    , subreddit_title
    , rating AS old_rating
    , topic AS old_topic
    , nt.rating_short
    , nt.rating_name
    , nt.rating_weight
    , nt.primary_topic
    , nt.survey_version
    , nt.pt AS pt_new_topic

    , lbl.cluster_id_agg_ward_cosine_200

    , lbl.primary_post_language  -- Source: ML model predicts language for each post
    , lbl.primary_post_language_percent
    , lbl.primary_post_type
    , lbl.primary_post_type_percent


FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v032_a` AS lbl
INNER JOIN cluster_for_selected_sub AS sel
    ON lbl.cluster_id_agg_ward_cosine_200 = sel.cluster_id_agg_ward_cosine_200
LEFT JOIN `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsourced_topic_and_rating` AS nt
    ON nt.subreddit_id = lbl.subreddit_id

WHERE 1=1
    AND nt.pt = (CURRENT_DATE() - 1)
ORDER BY cluster_id_agg_ward_cosine_200 ASC, users_l28 DESC
LIMIT 25
;


-- Use new QA table to filter subreddits by their cluster
SELECT
    cluster_id_agg_ward_cosine_200
    , qa_cluster_is_nsfw  -- if "yes", 90%+ sure that cluster is NSFW
    , subs_in_cluster_count
    , german_subs_in_cluster_count
    , german_subs_in_cluster_percent
    , subs_over18_nsfw_in_cluster_percent

FROM `reddit-employee-datasets.david_bermejo.subclu_cluster_summary_v032a`
LIMIT 1000
;


-- WIP/ scratch:
-- there are too many communities with "E" rating, but mature topics
--  Best bet is to use ads-allow-list column (all ads, some ads) because that gets manually verified
-- Diffs from previous query:
-- - Pull latest ratings from new tagging table
-- - Pull latest activity (users, posts, etc.)

SELECT
    subreddit_name
    , lbl.subreddit_id
    , lbl.cluster_id_agg_ward_cosine_35

    , rating AS old_rating
    , topic AS old_topic

    , nt.rating_short
    , nt.rating_name
    , nt.rating_weight
    , nt.primary_topic
    , nt.survey_version
    , nt.pt AS pt_new_topic


    , post_median_word_count    -- From posts used in ML clustering
    , German_posts_percent      -- From posts used in ML clustering

    , subreddit_language        -- This language is set by the moderators
    , posts_l28
    , subscribers
    , users_l7
    , users_l28


FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v031_a` AS lbl
LEFT JOIN `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsourced_topic_and_rating` AS nt
    ON nt.subreddit_id = lbl.subreddit_id

WHERE 1=1
    AND (
        nt.rating_short = 'E'
        -- OR lbl.rating IN ('g', 'pg', 'pg13')
    )
    AND nt.pt = (CURRENT_DATE() - 1)

    -- Exclude clusters that are NSFW
    AND NOT lbl.cluster_id_agg_ward_cosine_35 IN ('4')

ORDER BY cluster_id_agg_ward_cosine_35 ASC, users_l28 DESC
;


-- WIP / scratch: check status & rating
-- Still need to add ads-allow-list, because tagging system rating "E" is not right
with verified_subreddits as
(
select
    subreddit_id
    , verification_time
    , survey_version
    , tag_type
    , status
FROM
  `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsource_verification_status`
WHERE
  pt = (CURRENT_DATE() - 1)
  AND status = 'verified'
  AND tag_type = 'rating'
  and date(verification_time) >= '2021-07-25'
)

SELECT
    lbl.subreddit_id
    , lbl.cluster_id_agg_ward_cosine_35
    , users_l28

    , rating AS old_rating
    , topic AS old_topic

    , lbl.subreddit_name
    , vs.*

    , rating_short
    , rating_name
    , primary_topic
    , array_to_string(secondary_topics,", ") as secondary_topics
    , array_to_string(mature_themes,", ") as mature_themes
    , nt.pt AS new_rating_pt
    , nt.* EXCEPT (
        liveness_ts, subreddit_id, pt
        , rating_short
        , rating_name
        , primary_topic
        , secondary_topics
        , mature_themes
    )

FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v031_a` AS lbl
LEFT JOIN `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsourced_topic_and_rating` AS nt
    ON nt.subreddit_id = lbl.subreddit_id
LEFT JOIN verified_subreddits as vs
    ON vs.subreddit_id = nt.subreddit_id

WHERE 1=1
    AND nt.rating_short = 'E'
    AND nt.pt = (CURRENT_DATE() - 1)
    AND nt.primary_topic = 'Mature Themes and Adult Content'

ORDER BY users_l28 DESC, cluster_id_agg_ward_cosine_35 ASC
;


-- WIP: check for 'nudity' or 'sex' in the mature_theme field
--  Need to use "UNNEST" because each subreddit can have multiple themes
with verified_subreddits as
(
select
    subreddit_id
    , verification_time
    , survey_version
    , tag_type
    , status
from
  `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsource_verification_status`
WHERE
  pt = (CURRENT_DATE() - 1)
  AND status = 'verified'
  AND tag_type = 'rating'
  and date(verification_time) >= '2021-07-25'
)

SELECT
    slo.name    AS subreddit_name
    , slo.subscribers
    , vs.*
    , rating_short
    , rating_name
    , primary_topic
    , rating_weight
    , nt.survey_version  AS tag_survey_version
    , nt.pt AS new_rating_pt
    , array_to_string(secondary_topics,", ") as secondary_topics
    , array_to_string(mature_themes,", ") as mature_themes_list
    -- , nt.* EXCEPT (
    --     liveness_ts, subreddit_id, pt
    --     , rating_short
    --     , rating_name
    --     , primary_topic
    --     , secondary_topics
    --     , mature_themes
    -- )
FROM `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsourced_topic_and_rating` AS nt
LEFT JOIN verified_subreddits as vs
    ON vs.subreddit_id = nt.subreddit_id

LEFT JOIN `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup` AS slo
    ON nt.subreddit_id = slo.subreddit_id

WHERE slo.dt = (CURRENT_DATE() - 1)
    AND nt.pt = (CURRENT_DATE() - 1)
    AND nt.rating_short = 'E'
    AND nt.primary_topic = 'Mature Themes and Adult Content'
    AND 'sex' in UNNEST(mature_themes)

ORDER BY slo.subscribers DESC
;

