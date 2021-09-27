-- noinspection SqlResolveForFile, SqlNoDataSourceInspectionForFile

CREATE TABLE `reddit-employee-datasets.david_bermejo.eda_post_counts`
PARTITION BY submit_date
AS (
    -- Get post counts to get ideas for content volume
    SELECT
        geo.*
        , sp.submit_date
        # , sp.subreddit_name
        , sp.subreddit_id

        , COUNT(DISTINCT(sp.post_id))   AS post_ids_not_removed_unique_count
        , SUM(sp.comments)              AS comments_to_posts_not_removed_sum  # This number might include "unsuccessful" comments
    -- FROM `reddit-employee-datasets.lisa_guo.geo_relevant_subreddits_intl` AS geo
    FROM `reddit-employee-datasets.wacy_su.geo_relevant_subreddits_2021` AS geo
    LEFT JOIN `data-prod-165221.cnc.successful_posts` AS sp
        ON geo.subreddit_name = sp.subreddit_name
    # LEFT JOIN `data-prod-165221.ds_subreddit_whitelist_tables.subreddit_whitelist_metrics` AS wl
    #     ON sp.subreddit_id = wl.sub

    WHERE geo.geo_country_code = 'DE'
        AND sp.dt >= "2021-04-15"
        AND sp.removed = 0

    GROUP BY 1, 2, 3, 4, 5, 6, sp.submit_date, sp.subreddit_id

    # ORDER BY sp.submit_date ASC, geo.rank_no ASC
)
;

-- Create table that besides geo-relevance includes number of active v. nsfw subs
WITH tot_subreddit AS
(select pt, subreddit_name, sum(l1) as users
from data-prod-165221.all_reddit.all_reddit_subreddits_daily arsub
where pt >= '2021-03-01'
group by 1, 2),
geo_sub AS
(select tot.pt, tot.subreddit_name, geo_country_code, tot.users, sum(l1) as users_country
  from data-prod-165221.all_reddit.all_reddit_subreddits_daily arsub
    left join tot_subreddit tot on tot.subreddit_name = arsub.subreddit_name and tot.pt = arsub.pt
    where arsub.pt >= '2021-03-01'
    group by 1,2,3, 4),
final_touches AS
(select pt, geo_sub.subreddit_name, geo_country_code, users, users_country,
        users_country/users as pct_sv_country
from geo_sub
  where geo_country_code = 'DE'
   --and users_country >= 100
group by 1,2,3,4, 5
),
ranked as (
select *, RANK () OVER (PARTITION BY geo_country_code ORDER BY users_country desc) as rank_no
    from final_touches ft
    where ft.pct_sv_country >= 0.4),
active as (select
pt,
count(distinct name) as num_active_sub
from ranked r inner join ds_v2_postgres_tables.subreddit_lookup s
on lower(r.subreddit_name) = lower(s.name) and cast(r.pt as date) = s.dt
inner join data-prod-165221.ds_subreddit_whitelist_tables.active_subreddits a
on lower(r.subreddit_name) = lower(a.subreddit_name) and cast(r.pt as date) = a.dt
where s.dt >= '2021-03-01'
and coalesce(verdict,'f') <> 'admin_removed'
and coalesce(is_spam,false) = false
and coalesce(over_18,'f') = 'f'
and coalesce(is_deleted,false) = false
and deleted is NULL
and type in ('public','private','restricted')
and not REGEXP_CONTAINS(lower(s.name), r'^u_.*')
and active = true
group by 1
),
all_geo as (
select
pt,
count(distinct name) as num_safe_sub
from ranked r inner join ds_v2_postgres_tables.subreddit_lookup s
on lower(r.subreddit_name) = lower(s.name) and cast(r.pt as date) = s.dt
where s.dt >= '2021-03-01'
and coalesce(verdict,'f') <> 'admin_removed'
and coalesce(is_spam,false) = false
and coalesce(over_18,'f') = 'f'
and coalesce(is_deleted,false) = false
and deleted is NULL
and type in ('public','private','restricted')
and not REGEXP_CONTAINS(lower(s.name), r'^u_.*')
group by 1
)

select
ag.pt,
num_active_sub,
num_safe_sub
from all_geo ag left join active av
on ag.pt = av.pt
;


-- Query to see some duplicated post_ids in clv2
SELECT
DISTINCT * EXCEPT (text)

FROM `reddit-protected-data.language_detection.comment_language_v2`
WHERE DATE(_PARTITIONTIME) = "2021-04-15"
    AND thing_type = 'post'
    AND post_id = "t3_mrp2n5"
    # in doesn't work because the other dupes happen in other dates
    # AND post_id IN ("t3_mrp2n5", "t3_mtqj0m", "muaycq")

# GROUP BY 1  #, post_id, subreddit_id, user_id
ORDER BY post_id ASC

LIMIT 100
;

-- check SP table for uniques
--  In this one we still see some duplicates but not as many as in
--  language detection
SELECT
    COUNT(*)                    AS total_rows
    , COUNT(DISTINCT post_id)   AS post_id_uniques
    , COUNT(DISTINCT uuid)      AS uuid_uniques
FROM `data-prod-165221.cnc.successful_posts` AS sp
WHERE sp.dt = "2021-04-15"
    # AND post_id = 't3_mrp2n5'
    # AND uuid = '31b58f37-7c38-4427-9e6b-7bbf8dfdd2c9'
# LIMIT 100

-- Output:
-- Row	total_rows	post_id_uniques	uuid_uniques
-- 1	1021597     1009079         1009079
;


-- Example  to remove duplicates
select * except(row_num)
from (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY Firstname, Lastname
            ORDER BY creation_date desc
        ) row_num
    FROM
        dataset.table_name
) t
WHERE row_num=1
;


-- Get all geo-relevant subs for DE + add flag to check which ones
-- were used for POC
-- 2021-06-14
-- Used in colab notebook:
-- https://colab.research.google.com/drive/1Fo9UIV1BoQ6EfL42lTdrAYbpb9A3Ueu4#scrollTo=SFM61kb5Gkzn
DECLARE partition_date DATE DEFAULT '2021-05-18';
DECLARE start_date DATE DEFAULT '2021-04-01';
DECLARE end_date DATE DEFAULT '2021-05-19';

WITH all_de_geo_subs_and_training_subs AS
(
    SELECT
        COALESCE(geo.subreddit_name, subs.subreddit_name) AS subreddit_name
        , geo.geo_country_code
        , geo.pct_sv_country
        , geo.rank_no
        , IF(subs.subreddit_name IS NOT NULL, True, False) AS subreddit_used_for_clustering_poc
        , subs.subreddit_info_ambassador

    FROM (
        SELECT *
        FROM `reddit-employee-datasets.wacy_su.geo_relevant_subreddits_2021`
        WHERE geo_country_code = "DE"
    ) AS geo

    LEFT JOIN (
        -- Using sub-selection in case there are subs that haven't been registered in asr table
        SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
        WHERE DATE(pt) = partition_date
            AND users_l7 >= 100
    ) AS asr
        ON geo.subreddit_name = asr.subreddit_name

    FULL OUTER JOIN `reddit-employee-datasets.david_bermejo.subclu_selected_subs_20210519` AS subs
        ON geo.subreddit_name = subs.subreddit_name
)

SELECT
    gs.subreddit_name
    , gs.subreddit_used_for_clustering_poc
    , gs.geo_country_code

    , sp.post_id
    , CASE
        WHEN rt.rating IN ("x", "nc17") THEN "over18_nsfw"
        WHEN dst.topic = "Mature Themes and Adult Content" THEN "over18_nsfw"
        WHEN slo.over_18 = "t" THEN "over18_nsfw"
        ELSE COALESCE (
            gs.subreddit_info_ambassador,
            LOWER(dst.topic),
            "uncategorized"
        )
        END         AS combined_topic_and_rating

FROM all_de_geo_subs_and_training_subs AS gs

# Get posts for subreddit
LEFT JOIN `data-prod-165221.cnc.successful_posts` AS sp
    ON gs.subreddit_name = sp.subreddit_name

LEFT JOIN (
    SELECT * FROM `data-prod-165221.ds_v2_subreddit_tables.subreddit_ratings`
    WHERE DATE(pt) = partition_date
) AS rt
    ON gs.subreddit_name = rt.subreddit_name
LEFT JOIN(
    SELECT * FROM `data-prod-165221.ds_v2_subreddit_tables.subreddit_topics`
    WHERE DATE(pt) = partition_date
) AS dst
    ON gs.subreddit_name = dst.subreddit_name

LEFT JOIN (
    SELECT *
    FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
    WHERE dt = end_date
)AS slo
    ON gs.subreddit_name = LOWER(slo.name)

WHERE 1=1
    # AND subreddit_in_cluster_training = True  # check that we get ~111k posts for clustered subs
        # got 114k, maybe some got dropped because they're too short in last steps?
    AND sp.dt BETWEEN start_date AND end_date
    AND sp.removed = 0

;

-- Get screenviews by country
# Get screen views
# LEFT JOIN (
#     SELECT
#         user_id
#         , geo_country_code
#         , subreddit_name
#         , screenviews
#     FROM `data-prod-165221.ds_v2_component_tables.user_subreddit_daily_screenviews`
#     WHERE DATE(_PARTITIONTIME) = partition_date
#         AND dt BETWEEN start_date AND end_date
#     # LIMIT 500
# ) AS dsv
#     ON gs.subreddit_name = dsv.subreddit_name


-- 2021-06-16
-- Many of the top US/world subs do not appear to be labeled by the same people/process
--  that had many labels for German subs. Might need to use something like
-- `normalized topics`, but these have multiple labels per sub & they're not
-- normalized
SELECT *
FROM `data-prod-165221.community_topic_data_resources.normalized_topics`
WHERE DATE(pt) = "2021-06-15"
    AND subreddit_name IN ("nfl", "cosplaygirls", "nba", "amihot", "memes")

ORDER BY subreddit_name ASC, community_topic ASC
LIMIT 500
;

-- Get count of posts with flair text
-- Could use flair text for training OR for validation
--  Need to figure out a better approach to integrating with existing weights...
SELECT
    pl.flair_text
    # , COUNT(*)    AS total_rows
    , COUNT(DISTINCT (pl.post_id))          AS post_ids_unique_count

FROM `reddit-employee-datasets.david_bermejo.posts_for_germany_topic_clustering_20210519` AS sp

LEFT JOIN `data-prod-165221.ds_v2_postgres_tables.post_lookup` AS pl
    ON sp.post_id = pl.post_id

WHERE DATE(pl._PARTITIONTIME) = "2021-05-30"
    AND sp.submit_date BETWEEN "2021-04-01" AND "2021-05-19"
    AND sp.subreddit_name = 'de'

GROUP BY 1
ORDER BY 2 DESC
;

SELECT
    pl.language_preference
    , pl.flair_text
    , CASE
         WHEN (pl.language_preference = sp.language) THEN 1
         ELSE 0
     END AS language_pref_matches_predicted_language
    , CASE
         WHEN (pl.language_preference = sp.weighted_language) THEN 1
         ELSE 0
     END AS language_pref_matches_predicted_weighted_language
    , sp.*

    # COUNT(*)    AS total_rows
    # , COUNT(DISTINCT (pl.language_preference))  AS language_pref_distinct
    # , COUNT(DISTINCT (sp.language))             AS predicted_language_distinct
    # , COUNT(DISTINCT (sp.weighted_language))    AS predicted_weighted_language_distinct
    # , SUM(
    #     CASE
    #         WHEN (pl.language_preference = sp.language) THEN 1
    #         ELSE 0
    #         END
    # ) AS language_pref_matches_predicted_language_count
    # , SUM(
    #     CASE
    #         WHEN (pl.language_preference = sp.weighted_language) THEN 1
    #         ELSE 0
    #         END
    # ) AS language_pref_matches_predicted_weighted_language_count
    # , COUNT(DISTINCT (pl.flair_text))          AS flair_text_distinct


FROM `reddit-employee-datasets.david_bermejo.posts_for_germany_topic_clustering_20210519` AS sp

LEFT JOIN `data-prod-165221.ds_v2_postgres_tables.post_lookup` AS pl
    ON sp.post_id = pl.post_id

WHERE DATE(_PARTITIONTIME) = "2021-05-30"
    AND sp.submit_date BETWEEN "2021-05-01" AND "2021-05-10"
    AND sp.subreddit_name = 'de'

LIMIT 200
;


-- There are two different "post-types":
-- a) how the post was created
-- b) how it was viewed
SELECT
  type_1,
  type_2,
  count(*) AS posts
FROM
  (
    SELECT
      a.post_id,
      a.post_type AS type_1,
      b.post_type AS type_2
    FROM
      (
        SELECT
          post_id,
          post_type
        FROM
          `data-prod-165221.events_v2.analytics`
        WHERE
          pt = '2021-07-19'
          AND source = 'post'
          AND ACTION = 'view'
          AND noun = 'post'
        GROUP BY
          1,
          2
      ) a
      INNER JOIN (
        SELECT
          post_id,
          post_type
        FROM
          cnc.successful_posts
        WHERE
          dt = '2021-07-19'
          AND removed = 0
        GROUP BY
          1,
          2
      ) b ON a.post_id = b.post_id
  )
GROUP BY
  1,
  2
;


-- Map country codes to country names
-- The "regions" are not that helpful because they put
-- Europe, Middle East, and Africa together
SELECT
    subreddit_name
    , cm.country_name
    , cm.region
    , cm.country_code
FROM `data-prod-165221.i18n.geo_sfw_communities` AS geo
LEFT JOIN `data-prod-165221.ds_utility_tables.countrycode_region_mapping` AS cm
    ON geo.country = cm.country_code
WHERE DATE(pt) = (CURRENT_DATE() - 2)
    AND (cm.region = 'LATAM' OR cm.country_name = 'Spain')
    AND cm.country_name != 'Brazil'
;

-- Get OCR Text for German-related communities (!)
-- It looks like in this OCR table we could get multiple rows for a post ID:
-- * because a post could have links to multiple images and the current behavior
--     saves & scans each page for OCR content
-- Limit to only post_type = 'image' for QA
WITH geo_subs_raw AS (
SELECT
    geo.*
    , cm.country_name
    , cm.country_code
    , RANK () OVER (PARTITION BY subreddit_id, country ORDER BY pt desc) as sub_geo_rank_no
FROM `data-prod-165221.i18n.geo_sfw_communities` AS geo
LEFT JOIN `data-prod-165221.ds_utility_tables.countrycode_region_mapping` AS cm
    ON geo.country = cm.country_code

WHERE DATE(pt) BETWEEN (CURRENT_DATE() - 5) AND (CURRENT_DATE() - 2)
    AND cm.country_code = 'DE'

-- Order by country name here so that the aggregation sorts the names alphabetically
ORDER BY subreddit_name, cm.country_name
),
geo_subs_agg AS (
SELECT
    geo.subreddit_id
    , geo.subreddit_name
    , STRING_AGG(geo.country_code, ', ') AS geo_relevant_country_codes
    , STRING_AGG(geo.country_name, ', ') AS geo_relevant_country_names
    , COUNT(geo.country_code) AS geo_relevant_country_count

FROM geo_subs_raw AS geo

-- Drop repeated country names
WHERE geo.sub_geo_rank_no = 1

GROUP BY 1, 2
ORDER BY subreddit_name
),
ocr_text_agg AS (
-- We need to agg the text because one post could have multiple images
SELECT
    ocr.post_id
    , pt
    , STRING_AGG(inferred_text, '. ') AS inferred_text_agg
    , COUNT(media_url) AS images_in_post_count

FROM `data-prod-165221.swat_tables.image_ocr_text` AS ocr

WHERE DATE(ocr.pt) = (CURRENT_DATE() - 3)

GROUP BY 1, 2
)

SELECT
    -- COUNT(*)
    sp.subreddit_name
    , geo.geo_relevant_country_codes
    , geo.geo_relevant_country_names
    , geo.geo_relevant_country_count
    , sp.post_id
    , sp.removed
    , sp.submit_date
    , sp.post_title
    -- , sp.post_body_text
    , sp.post_type
    , ocr.images_in_post_count
    , ocr.inferred_text_agg

FROM `data-prod-165221.cnc.successful_posts` AS sp
INNER JOIN geo_subs_agg AS geo
    ON geo.subreddit_id = sp.subreddit_id
LEFT JOIN ocr_text_agg AS ocr
    ON ocr.post_id = sp.post_id AND DATE(sp.dt) = DATE(ocr.pt)

WHERE 1=1
    AND sp.post_type = 'image'
    AND sp.dt = (CURRENT_DATE() - 3)
    AND sp.removed = 0

ORDER BY sp.subreddit_name, sp.post_id
LIMIT 1000
;



-- Get subreddits that have geo-relevance overlap for Spain, Mexico, & Argentina
-- Seems like only ~20 subs and most of these are small
WITH geo_subs_raw AS (
SELECT
    geo.*
    , cm.country_name
    , cm.country_code
    , RANK () OVER (PARTITION BY subreddit_id, country ORDER BY pt desc) as sub_geo_rank_no
FROM `data-prod-165221.i18n.geo_sfw_communities` AS geo
LEFT JOIN `data-prod-165221.ds_utility_tables.countrycode_region_mapping` AS cm
    ON geo.country = cm.country_code
WHERE DATE(pt) BETWEEN (CURRENT_DATE() - 60) AND (CURRENT_DATE() - 2)
    AND cm.country_name IN ('Spain', 'Mexico', 'Argentina')

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

-- Only keep one country name
WHERE geo.sub_geo_rank_no = 1

GROUP BY 1, 2
ORDER BY subreddit_name
)  -- close 2nd subselection


SELECT * FROM geo_subs_agg AS geo
WHERE geo.geo_relevant_country_count > 1
;


-- 2021-09-02; more debugging on tryint to filter out NSFW (sex-related) subreddits
SELECT
    slo.name    AS subreddit_name
    , slo.subscribers
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

LEFT JOIN `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup` AS slo
    ON nt.subreddit_id = slo.subreddit_id

WHERE slo.dt = (CURRENT_DATE() - 1)
    AND nt.pt = (CURRENT_DATE() - 1)
    -- AND nt.rating_short = 'E'
    AND nt.rating_short != 'X'
    -- AND nt.primary_topic = 'Mature Themes and Adult Content'
    AND (
        'sex' in UNNEST(mature_themes)
        OR 'sex_porn' in UNNEST(mature_themes)
        OR 'nudity_explicit' in UNNEST(mature_themes)
    )

ORDER BY slo.subscribers DESC
;
