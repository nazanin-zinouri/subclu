-- Check decision/QA actions for specific countries
SELECT
    pt, subreddit_id, geo_relevant_countries, users_l7, subreddit_name, primary_topic, rating_short, predicted_rating, predicted_topic
    , combined_filter, combined_filter_reason, taxonomy_action
FROM `reddit-employee-datasets.david_bermejo.subclu_v0050_subreddit_clusters_c_qa_flags`
WHERE pt = "2022-07-25"
    AND geo_relevant_countries IS NOT NULL
    AND combined_filter = 'review'
    AND combined_filter_reason IN (
        'missing_topic'
        -- , 'missing_rating'
        -- , 'missing_rating_and_topic'
        , 'review_rating_and_topic'
        , 'review_topic'
    )
ORDER BY users_l7 DESC
;



-- Check predictions for subreddits that were previously blocked
-- Check subs that were flagged before
SELECT
    * EXCEPT(taxonomy_action, combined_filter_detail)
FROM `reddit-employee-datasets.david_bermejo.subclu_v0050_subreddit_clusters_c_qa_flags`
WHERE 1=1
    AND pt = "2022-08-01"
    -- AND combined_filter IN ('review')
    -- AND combined_filter_reason IN ('review_topic')
    AND subreddit_name IN (
        'de'
        , 'ich_iel'
        , 'france'
        , 'rance'
        , 'india'
        , 'mexico'
        -- , 'brasil'
        -- , 'brazil'
        -- , 'brasilancap'
        -- , 'danklatam'
        -- , 'portugalcaralho'
        -- , 'circojeca'
        -- , 'brasillivre'
        , 'brasilivre'
        , 'bolsonaro'
        , 'brasildob'
        -- , 'comunismodob'
        , 'anarquia_brasileira'
        , 'france6'
        , 'punjabiactresses'
        , 'covidmx'
        , 'auscovid19'
    )


ORDER BY combined_filter_detail, users_l7 DESC
;

