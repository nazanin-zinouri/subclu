

-- View custom countrycode mapping table to make sure joins/overwrites
--  work as expected (e.g., North & South Korea Names)
SELECT *
FROM `reddit-relevance.tmp.countrycode_name_mapping`
ORDER BY country_code ASC
;


-- Check relevant countries for a specific subreddit
SELECT *
FROM `reddit-relevance.tmp.subclu_subreddit_geo_relevance_standardized_202200404`
WHERE subreddit_name IN ('futbol')

ORDER BY relevance_combined_score DESC
;

-- ============
-- Check how many subs qualify b/c they're i18n core/ambassador subs
-- ===
SELECT *
FROM `reddit-relevance.tmp.subclu_subreddit_geo_selected_20220404`
WHERE 1=1
    AND i18n_type IS NOT NULL
    OR  i18n_type_2 IS NOT NULL
LIMIT 1000;

SELECT *
FROM `reddit-relevance.tmp.subclu_subreddit_candidates_20220404`
WHERE 1=1
    AND i18n_type IS NOT NULL
    OR  i18n_type_2 IS NOT NULL
LIMIT 1000;
