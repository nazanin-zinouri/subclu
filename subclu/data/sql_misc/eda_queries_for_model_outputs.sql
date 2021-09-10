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
    qa.cluster_id_agg_ward_cosine_200  -- cluster column to JOIN on
    , qa.subs_in_cluster_count
    , qa.german_subs_in_cluster_count
    , qa.german_subs_in_cluster_percent
    , qa.subs_over18_nsfw_in_cluster_percent
    , qa.qa_topic_rating   -- rating for the MAJORITY os subreddits in a cluster
    , qa.qa_topic_rating_name
    , qa.qa_topic_tier_1
    , qa.qa_topic_tier_2
    , qa.qa_cluster_notes

FROM `reddit-employee-datasets.david_bermejo.subclu_cluster_summary_v032a`
ORDER BY german_subs_in_cluster_count DESC, german_subs_in_cluster_percent DESC
;


-- Merge QA table with subreddit + cluster-labels table
-- Use new QA table to filter subreddits by their cluster
SELECT
    lbl.subreddit_name
    , lbl.subreddit_id
    , rating
    , topic

    , qa.cluster_id_agg_ward_cosine_200
    , qa.qa_topic_rating   -- rating for the MAJORITY os subreddits in a cluster
    , qa.qa_topic_rating_name
    , qa.qa_topic_tier_1
    , qa.qa_topic_tier_2
    -- , qa.qa_cluster_notes

    , lbl.primary_post_language  -- Source: ML model predicts language for each post
    , lbl.primary_post_language_percent
    , lbl.primary_post_type
    , lbl.primary_post_type_percent

    , qa.subs_in_cluster_count
    , qa.german_subs_in_cluster_count
    , qa.german_subs_in_cluster_percent
    , qa.subs_over18_nsfw_in_cluster_percent

FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v032_a` AS lbl
LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_cluster_summary_v032a` AS qa
    ON lbl.cluster_id_agg_ward_cosine_200 = qa.cluster_id_agg_ward_cosine_200

WHERE 1=1
    AND (
        qa.qa_topic_rating IS NULL
        OR qa.qa_topic_rating NOT IN ("X", "D")
        )

ORDER BY german_subs_in_cluster_count DESC, german_subs_in_cluster_percent DESC
;


-- Merge QA table with distance table (pairwise)
-- Use new QA table to filter subreddits by their cluster
SELECT
    dst.cosine_distance
    , dst.subreddit_name_a
    , dst.subreddit_name_b
    , dst.subreddit_id_a
    , dst.subreddit_id_b

    , qaa.cluster_id_agg_ward_cosine_200 AS cluster_id_agg_ward_cosine_200_a
    , qaa.qa_topic_rating    AS qa_topic_rating_a  -- rating for the MAJORITY os subreddits in a cluster

    , qaa.qa_topic_rating_name   AS qa_topic_rating_name_a
    , qaa.qa_topic_tier_1    AS qa_topic_tier_1_a
    , qaa.qa_topic_tier_2    AS qa_topic_tier_2_a

FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_distance_v0032_c_posts_and_comments_and_meta` AS dst
LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v032_a` AS lbla
    ON lbla.subreddit_id = dst.subreddit_id_a
LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_cluster_summary_v032a` AS qaa
    ON lbla.cluster_id_agg_ward_cosine_200 = qaa.cluster_id_agg_ward_cosine_200

WHERE 1=1
    AND (
        qaa.qa_topic_rating IS NULL
        OR qaa.qa_topic_rating NOT IN ("X", "D")
        )
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

-- ========================
-- Add geo-relevant countries to cluster labels AND new RATINGS
-- ===
WITH geo_subs_raw AS (
SELECT
    geo.*
    , cm.country_name
    , cm.country_code
    , RANK () OVER (PARTITION BY subreddit_id, country ORDER BY pt desc) as sub_geo_rank_no
FROM `data-prod-165221.i18n.geo_sfw_communities` AS geo
LEFT JOIN `data-prod-165221.ds_utility_tables.countrycode_region_mapping` AS cm
    ON geo.country = cm.country_code
WHERE DATE(pt) BETWEEN (CURRENT_DATE() - 28) AND (CURRENT_DATE() - 2)

-- Order by country name here so that the aggregation sorts the names alphabetically
ORDER BY subreddit_name, cm.country_name
),

geo_subs_agg AS (
SELECT
    geo.subreddit_id
    , geo.subreddit_name
    , STRING_AGG(geo.country_name, ', ') AS geo_relevant_countries
    , COUNT(geo.country_code) AS geo_relevant_country_count

FROM geo_subs_raw AS geo

-- Drop repeated country names
WHERE geo.sub_geo_rank_no = 1

GROUP BY 1, 2
ORDER BY subreddit_name
)

SELECT
    lbl.subreddit_name

    , lbl.primary_post_language  -- Source: ML model predicts language for each post
     , lbl.primary_post_language_percent

    , geo.geo_relevant_countries
    , geo.geo_relevant_country_count
    , lbl.primary_post_type

    , rating_short
    , rating_name
    , primary_topic
    , rating_weight
    , nt.survey_version  AS tag_survey_version
    , nt.pt AS new_rating_pt
    , array_to_string(secondary_topics,", ") as secondary_topics
    , array_to_string(mature_themes,", ") as mature_themes_list

    , lbl.cluster_id_agg_ward_cosine_200
    , rating
    , topic
    , subreddit_title


    , lbl.primary_post_type_percent

    , subreddit_language  -- This language is set by the Moderators
    -- , subreddit_public_description

FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v032_a` AS lbl
LEFT JOIN geo_subs_agg AS geo
    ON geo.subreddit_id = lbl.subreddit_id
LEFT JOIN `reddit-protected-data.cnc_taxonomy_cassandra_sync.shredded_crowdsourced_topic_and_rating` AS nt
    ON lbl.subreddit_id = nt.subreddit_id


WHERE 1=1
    -- 81= soccer, futbol
    -- 49= dating, meeting online & in real life (irl)
    -- 22= finanze /finanzen
    AND lbl.cluster_id_agg_ward_cosine_200 = 81
    AND nt.pt = (CURRENT_DATE() - 2)
;
