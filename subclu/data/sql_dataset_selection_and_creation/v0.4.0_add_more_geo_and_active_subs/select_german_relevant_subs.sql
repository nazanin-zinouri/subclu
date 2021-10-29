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


-- SELECT German relevant subs AND new ratings
-- select DE subreddits + get latest rating

SELECT
    sl.subreddit_id
    , sl.subreddit_name
    , r.rating
    -- , r.subrating
    , r.version

    , slo.verdict
    , slo.quarantine

    , geo.country_name
    , geo.users_percent_in_country
    -- , sl.geo_relevant_countries
    , ambassador_subreddit
    , posts_for_modeling_count

    , primary_post_language
    , primary_post_language_percent
    , secondary_post_language
    , secondary_post_language_percent

    , geo_relevant_country_count
    , geo_relevant_country_codes
    , geo_relevant_subreddit

FROM `reddit-employee-datasets.david_bermejo.subclu_v0040_subreddit_languages` sl
LEFT JOIN (
    SELECT * FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
    # Look back 2 days because looking back 1-day could be an empty partition
    WHERE dt = (CURRENT_DATE() - 2)
) AS slo
    ON slo.subreddit_id = sl.subreddit_id
LEFT JOIN (
    SELECT * FROM `reddit-employee-datasets.david_bermejo.subclu_geo_subreddits_20210922`
    WHERE country_name = 'Germany'
) AS geo
    ON sl.subreddit_id = geo.subreddit_id
LEFT JOIN (
    SELECT * FROM ds_v2_subreddit_tables.subreddit_ratings
    WHERE pt = '2021-10-24'
) AS r
    ON r.subreddit_id = sl.subreddit_id

WHERE 1=1
    -- AND r.version = 'v2'
    -- AND COALESCE(r.rating, '') IN ('pg', 'pg13', 'g')
    AND COALESCE(slo.verdict, '') != 'admin-removed'
    AND COALESCE(slo.quarantine, false) != true
    AND (
        sl.geo_relevant_countries LIKE '%Germany%'
        OR sl.ambassador_subreddit = True
    )

ORDER BY users_percent_in_country ASC -- subreddit_name, ambassador_subreddit
;
