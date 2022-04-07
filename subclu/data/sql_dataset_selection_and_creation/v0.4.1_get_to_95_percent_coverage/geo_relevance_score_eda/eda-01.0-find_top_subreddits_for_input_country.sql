-- Find most relevant subreddits for a given country
SELECT
    nt.primary_topic
    , nt.rating_name
    , nt.rating_short
    , rel.* EXCEPT (subreddit_id, geo_country_code)
FROM `reddit-relevance.tmp.subclu_subreddit_geo_relevance_standardized_20220406` AS rel
    LEFT JOIN (
        SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
        WHERE pt = (CURRENT_DATE() - 2)
    ) AS nt
        ON rel.subreddit_id = nt.subreddit_id
WHERE 1=1
    AND (
        geo_relevance_default = TRUE
        OR relevance_percent_by_subreddit = TRUE
        OR relevance_percent_by_country_standardized = TRUE
    )
    -- AND country_name LIKE "%Nether%"
    AND country_name IN (
        'Vietnam'
    )
    AND COALESCE(rating_short, '') != 'X'
    -- AND subreddit_name LIKE "%pop%"
-- ORDER BY country_name ASC, subreddit_rank_in_country ASC
ORDER BY country_name ASC, relevance_combined_score DESC
-- LIMIT 300
;
