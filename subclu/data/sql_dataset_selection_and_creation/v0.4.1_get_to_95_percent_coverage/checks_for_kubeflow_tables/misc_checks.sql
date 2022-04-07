

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


-- check how many subs in final selection are relevant to a country
SELECT *
FROM `reddit-relevance.tmp.subclu_subreddits_for_modeling_20220406`
WHERE 1=1
    AND geo_relevant_countries LIKE "%Netherlands%"
    AND COALESCE(over_18, 'f') != 't'
    AND COALESCE(rating_short, '') NOT IN ('X', 'D')
;


-- Check relevant countries for specific subreddits
SELECT *
FROM `reddit-relevance.tmp.subclu_subreddit_geo_relevance_standardized_20220406`
WHERE 1=1
    AND relevance_combined_score >= 0.11
    AND subreddit_name IN (
        'soccer'
        , 'cricket'
        , 'place'
        , 'starwarsplace'
        , 'formula1'
    )

ORDER BY subreddit_name, relevance_combined_score DESC
;
