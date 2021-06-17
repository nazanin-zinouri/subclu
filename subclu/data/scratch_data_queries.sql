-- noinspection SqlNoDataSourceInspectionForFile

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
