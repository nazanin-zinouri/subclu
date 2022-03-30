

-- View custom countrycode mapping table to make sure joins/overwrites
--  work as expected (e.g., North & South Korea Names)
SELECT *
FROM `reddit-relevance.tmp.countrycode_name_mapping`
ORDER BY country_code ASC
;


-- Check relevant countries for a specific subreddit
SELECT *
FROM `reddit-relevance.tmp.subclu_subreddit_geo_relevance_standardized_20220323`
WHERE subreddit_name IN ('futbol')

ORDER BY relevance_combined_score DESC
;
