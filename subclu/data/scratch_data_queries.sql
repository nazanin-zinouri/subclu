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
