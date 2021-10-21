-- Query to check DE to DE pairs by distance
-- SELECT *
-- FROM `reddit-employee-datasets.david_bermejo.subclu_v0040_subreddit_distances_c_top_200`
-- WHERE 1=1
--     AND (
--         primary_post_language_a = 'German'
--         OR geo_relevant_countries_a LIKE '%Germany%'
--         OR ambassador_subreddit_a = True
--     )
--     AND (
--         primary_post_language_b = 'German'
--         OR geo_relevant_countries_b LIKE '%Germany%'
--         OR ambassador_subreddit_b = True
--     )

-- ORDER BY subreddit_id_a, cosine_distance_rank_all
-- ;

-- Query to check subreddits in new (v0.4.0) models
--  Need to add filters to check only German/Germany subreddits
SELECT
    s.subreddit_id
    , s.subreddit_name
    , s.geo_relevant_countries
    , sl.posts_for_modeling_count
    , sl.primary_post_language
    , sl.primary_post_language_percent
    , sl.secondary_post_language
    , sl.secondary_post_language_percent
    , s.ambassador_subreddit
    , s.geo_relevant_country_count
    , s.geo_relevant_subreddit

FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20210924` AS s
INNER JOIN `reddit-employee-datasets.david_bermejo.subclu_v0040_subreddit_languages` as sl
    ON s.subreddit_id = sl.subreddit_id AND s.subreddit_name = sl.subreddit_name

WHERE 1=1
    AND (
        (
            -- Limit to 40% of posts to be in German to remove false positives
            --  and make it more of a German experience
            sl.primary_post_language= 'German'
            AND sl.primary_post_language_percent >= 0.4
        )
        OR s.geo_relevant_countries LIKE '%Germany%'
        OR s.ambassador_subreddit = True
    )

ORDER BY posts_for_modeling_count DESC
;
