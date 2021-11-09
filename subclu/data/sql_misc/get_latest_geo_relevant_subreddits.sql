-- In here I'm just exporting the data that Wacy already created

-- Step 1: get country scores from Wacy's Tables
-- Step 2: join with geo-table to get country NAMES (because codes can mean different things)

-- TODO(djb)

SELECT *
FROM `reddit-employee-datasets.wacy_su.geo_relevant_subreddits_2021`

ORDER BY geo_country_code ASC, users DESC
LIMIT 1000
;
