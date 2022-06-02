-- Get most active countries and their current tier
DECLARE regex_cleanup_country_name_str STRING DEFAULT r" of Great Britain and Northern Ireland| of America|";

WITH
  base as (
    SELECT
      date(pt) as dt
      , app_name
      , CASE
          when country is null then 'NULL'
          when country = 'all' then 'All'
          when country IN ('US','GB','CA','AU','DE','FR','IT','ES','IN','BR','MX','RO','NL') then country
          else 'ROW'
        END as country_code_group
      , COALESCE(country, "NULL") AS country_code
      , CASE
          when country IN ('GB','AU','CA') then 'tier_0'
          when country IN ('DE','FR','BR','MX','IN') then 'tier_1'
          when country IN ('IT','ES','JP','KR','PH','NL','TR','RO','DK','SE','FI','PL','ID','RU') then 'tier_2'
          when country IN ('US') then 'US'
          else 'ROW'
        END as country_tier

      -- Split to remove long official names like:
      --   Tanzania, United Republic of; Bolivia, Plurinational State of
      , CASE
          WHEN country IS NULL THEN 'NULL'
          ELSE REGEXP_REPLACE(
            SPLIT(cm.country_name, ', ')[OFFSET(0)],
            regex_cleanup_country_name_str, ""
          )
          END AS country_name
      , cm.region AS geo_region

      , sum(users) as DAU

    from `data-prod-165221.metrics_fact_tables.dau_app_geo_reporting` AS g
      LEFT JOIN `data-prod-165221.ds_utility_tables.countrycode_region_mapping` AS cm
        ON g.country = cm.country_code

    WHERE
      date(pt) >= date_sub(current_date(), interval 1 week)
      and app_name not in (
        'ads_platform',
        'guest1'
      )
    GROUP BY 1,2,3,4, 5, 6, 7
  ),
  week_agg AS (
    SELECT
      geo_region
      , country_code
      , country_name
      , country_tier

      , SUM(DAU) AS active_users
      , DATE_DIFF(MAX(dt), MIN(dt), DAY) AS prev_days_counted
      , DATE(MAX(dt)) date_pulled
    FROM
      base
    GROUP BY 1, 2, 3, 4
  )


select
    *
from week_agg
ORDER BY active_users DESC
LIMIT 50
;
