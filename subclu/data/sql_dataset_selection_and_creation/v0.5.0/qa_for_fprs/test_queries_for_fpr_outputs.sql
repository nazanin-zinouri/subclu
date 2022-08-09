WITH lists_as_str AS (
    -- It's easier to convert the lists in a CTE & then join later
    --  b/c the groupby can get messy
    SELECT
        run_id
        , geo_country_code
        , cluster_label

        , STRING_AGG(sl.item, ', ') AS seed_subreddit_names
        , STRING_AGG(sr.item, ', ') AS recommend_subreddit_names

    FROM `reddit-employee-datasets.david_bermejo.subclu_v0050_fpr_cluster_summary`
        LEFT JOIN UNNEST(seed_subreddit_names_list.list) AS sl
        LEFT JOIN UNNEST(recommend_subreddit_names_list.list) AS sr

    GROUP BY 1, 2, 3

)

SELECT *
FROM lists_as_str
WHERE 1=1
    -- AND geo_country_code = 'BR'
ORDER BY 1, 2, 3
;


-- SELECT
--     run_id
--     , geo_country_code
--     , country_name
--     , cluster_label
--     , cluster_topic_mix
--     , seed_subreddit_count
--     , recommend_subreddit_count
--     -- , seed_subreddit_names_list
--     -- , recommend_subreddit_names_list
--     -- , ARRAY_CONCAT_AGG(seed_subreddit_names_list.list) AS seed_subreddit_names_list

-- FROM `reddit-employee-datasets.david_bermejo.subclu_v0050_fpr_cluster_summary`
--     LEFT JOIN UNNEST(seed_subreddit_names_list.list) AS sl
-- WHERE 1=1
--     AND geo_country_code IN ('BR')
--     AND orphan_clusters = FALSE
-- -- GROUP BY 1,2,3,4,5,6,7
-- ORDER BY cluster_label
-- ;



-- Dynamic clusters
SELECT
    -- pt, qa_pt, run_id
    cluster_label
    , subreddit_name
    , primary_topic
    , rating_short
    , predicted_rating
    , predicted_topic
    , combined_filter_detail
    , taxonomy_action

    , dc.* EXCEPT (
        pt, qa_pt, run_id, qa_table, geo_relevance_table
        ,cluster_label
        , subreddit_name
        , primary_topic
        , rating_short
        , predicted_rating
        , predicted_topic
        , combined_filter_detail
        , taxonomy_action
    )
FROM `reddit-employee-datasets.david_bermejo.subclu_v0050_fpr_dynamic_clusters` AS dc
WHERE 1=1
    AND geo_country_code = 'BR'
    -- AND combined_filter = 'recommend'
    AND subreddit_name IN (
        'brasil'
        , 'brazil'
        , 'brasilancap'
        , 'libertarianismo'
        , 'danklatam'
        , 'portugalcaralho'
        , 'circojeca'
        , 'brasillivre'
        , 'brasilivre'
        , 'bolsonaro'
        , 'brasildob'
        , 'comunismodob'
        , 'anarquia_brasileira'
    )
ORDER BY cluster_label, users_l7 DESC, subreddit_name


-- Check FPR outputs
SELECT
    pt, qa_pt, run_id
    , geo_country_code, country_name
    , subreddit_name_seed
    , cluster_subreddit_names_list
    , cluster_label, cluster_label_k

FROM `reddit-employee-datasets.david_bermejo.subclu_v0050_fpr_outputs`
WHERE 1=1
    AND geo_country_code = 'BR'
ORDER BY cluster_label, subreddit_name_seed
