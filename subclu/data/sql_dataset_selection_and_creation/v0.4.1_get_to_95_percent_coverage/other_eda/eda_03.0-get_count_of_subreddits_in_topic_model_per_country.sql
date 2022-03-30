-- Get geo-relevant subs in model v0.4.1 to a specific country

SELECT
    geo.geo_country_code
    , geo.country_name

    , COUNT(DISTINCT geo.subreddit_id) AS relevant_subreddits_count
    , SUM(
        CASE WHEN (COALESCE(nt.rating_short, '') = 'E') THEN 1
        ELSE 0
        END
    ) AS relevant_subreddits_E_rated_count

FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220314` AS geo
    INNER JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` AS sc
        ON geo.subreddit_id = sc.subreddit_id
    LEFT JOIN (
        -- New view should be visible to all, but still comes from cnc_taxonomy_cassandra_sync
        SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
        WHERE pt = (CURRENT_DATE() - 2)
    ) AS nt
        ON geo.subreddit_id = nt.subreddit_id
WHERE 1=1
    AND geo.geo_country_code IN ('NL', 'RO')
    AND (
        geo.geo_relevance_default = TRUE
        OR geo.relevance_percent_by_subreddit = TRUE
        OR geo.e_users_percent_by_country_standardized >= 3.0
    )

GROUP BY 1, 2
;
