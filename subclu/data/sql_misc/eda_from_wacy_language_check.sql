-- Query from Wacy to find language for each user?

with lang_data as(
SELECT
pt,
  user_id,
  app_name,
  CONCAT(LOWER(REGEXP_EXTRACT(platform_primary_language, r"^([a-zA-Z]+)-[a-zA-Z]+")), '-', UPPER(REGEXP_EXTRACT(platform_primary_language, r"^[a-zA-Z]+-([a-zA-Z]+)"))) AS platform_primary_language,
  geo_country_code
FROM
  `data-prod-165221.events_v2.analytics`
WHERE
  1=1
  AND pt >= '2021-07-01'
  AND source = 'global'
  AND action = 'view'
  AND noun = 'screen'
  AND (inferred_user_agent_web_crawler IS FALSE
    OR REGEXP_CONTAINS(request_user_agent, "www.google.com/mobile/adsbot.html") IS FALSE)
  AND (session_anonymous_browsing_mode IS FALSE
    OR session_anonymous_browsing_mode IS NULL)
  -- AND platform_primary_language IS NOT NULL
)

select
pt,
geo_country_code,
count(distinct user_id) as num_users
from lang_data
where platform_primary_language = 'de-DE'
and geo_country_code != 'DE'
group by 1, 2
order by 3 desc
limit 100
