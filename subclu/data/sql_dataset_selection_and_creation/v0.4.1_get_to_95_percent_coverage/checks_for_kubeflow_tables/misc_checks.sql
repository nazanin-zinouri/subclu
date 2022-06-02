

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


-- Investigate subreddits that were no longer geo-relevant
--  after a change in standard-dev country filter
--  Most subs were porn and relevant to small countries like Puerto Rico & Netherlands
SELECT
    -- o.*
    COALESCE(nc.subreddit_name, o.subreddit_name) as subreddit_name
    , nc.users_l7
    , nc.posts_not_removed_l28
    , nc.activity_7_day
    , nc.active

    , n.geo_relevant_countries
    , o.geo_relevant_countries

    , o.users_l7
    , o.posts_not_removed_l28
    , o.activity_7_day
    , o.active
FROM `reddit-relevance.tmp.subclu_subreddit_candidates_20220527` AS nc
    LEFT JOIN `reddit-relevance.tmp.subclu_subreddits_for_modeling_20220527` AS n
        ON nc.subreddit_id = n.subreddit_id
    FULL OUTER JOIN `reddit-relevance.tmp.subclu_subreddits_for_modeling_20220526` AS o
        ON nc.subreddit_id = o.subreddit_id

WHERE 1=1
    -- subs not in the new selected list
    AND n.subreddit_id IS NULL

    -- and were in the old list
    AND o.subreddit_id IS NOT NULL

    -- that were country relevant in the old list + over min activity thresholds
    AND (
        o.geo_relevant_country_count IS NOT NULL
        AND nc.activity_7_day >= 5
        AND nc.posts_not_removed_l28 >= 8
    )

    -- that weren't country relevant in the old list
    -- AND o.geo_relevant_country_count IS NULL

ORDER BY nc.users_l7 DESC
