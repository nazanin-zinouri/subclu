-- Get the cluster ID & cluster subs, given an input sub names
-- With this query we're trying to find good cut-offs to find the
--  k-value (cluster number) where known sports leagues are different
--  We need them to be different so that we can create human labels at a level
--   that is helpful for onboarding and other surfaces.

WITH clusters_for_selected_subs AS(
    SELECT
        DISTINCT k_0150_label
    FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full`
    WHERE subreddit_name IN (
        'soccer', 'nba', 'formula1', 'nhl', 'nfl', 'baseball'
    )
)


-- Get summary of distinct clusters
-- SELECT
--     lbl.k_0100_label
--     , lbl.k_0150_label
--     , lbl.k_0400_label

--     , COUNT(DISTINCT subreddit_id) AS subreddits_count

-- FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full` AS lbl
-- INNER JOIN clusters_for_selected_subs AS sel
--     ON lbl.k_0150_label = sel.k_0150_label

-- GROUP BY 1, 2, 3

-- ORDER BY 1, 2, 3
-- ;


-- Check Top subreddits in each cluster
SELECT
    lbl.k_0100_label
    , lbl.k_0150_label
    , lbl.k_0400_label
    , subreddit_name
    , posts_for_modeling_count

FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full` AS lbl
INNER JOIN clusters_for_selected_subs AS sel
    ON lbl.k_0150_label = sel.k_0150_label

WHERE 1=1
    AND posts_for_modeling_count >= 500

ORDER BY 1, 2, 3, posts_for_modeling_count DESC
;


-- Check for v0.5.0 model -- looks like we need to go deeper (~k=1000 clusters)
-- Find "optimal" k value for sports-related clusters
-- With this query we're trying to find good cut-offs to find the
--  k-value (cluster number) where known sports leagues are different
--  We need them to be different so that we can create human labels at a level
--   that is helpful for onboarding and other surfaces.

WITH seed_subs AS(
    SELECT *
    FROM `reddit-employee-datasets.david_bermejo.subclu_v0050_subreddit_clusters_c_full`
    WHERE subreddit_name IN (
        -- Sports
        'formula1'
        , 'nba', 'nhl', 'nfl', 'baseball', 'soccer', 'chicagobears'

        -- NSFW & SFW counterparts
        -- , 'pornid', 'sex'
        -- , 'tinder', 'ama'
        -- , 'lgbt', 'relationship_advice'
        , 'kpopfap', 'kpop'
        , 'bollywood', 'bollyblindsngossip', 'bollywoodmilfs'
        , 'askreddit', 'fragreddit', 'asklatinamerica'

        -- food & drinks
        , 'vegetarian', 'vegande', 'carnivore', 'keto', 'cooking', 'fasting', 'bulimia', 'anorexia'
        , 'cocktails', 'tea', 'boba', 'wine', 'energydrinks', 'soda'
        , 'steak', 'coffee', 'pizza'

        -- drugs & diy
        , 'mushroomgrowers', 'cultivonha', 'gardening', 'trees', 'steroids', 'supplements'
        , 'vaping'

        -- Politics & covid
        , 'coronavirus', 'covid19', 'conspiracy', 'askthe_donald'
        , 'politics', 'news', 'politicalcompassmemes', 'conspiracy', 'qult_headquarters'
        , 'mensrights', 'theredpill', 'exredpill'
    )
)
, clusters_for_selected_subs AS(
    SELECT DISTINCT k_0400_label
    FROM seed_subs
)
, top_n_subs_for_clusters AS (
    SELECT
        sa.k_0400_label
        , sa.subreddit_id
        , sa.subreddit_name
        , users_l7_rank_400
    FROM `reddit-employee-datasets.david_bermejo.subclu_v0050_subreddit_activity` AS sa
    INNER JOIN clusters_for_selected_subs AS s
        ON sa.k_0400_label = s.k_0400_label
    WHERE 1=1
        AND users_l7_rank_400 <= 14
)

-- Get summary of distinct clusters
-- SELECT
--     lbl.k_0100_label
--     , lbl.k_0150_label
--     , lbl.k_0400_label

--     , COUNT(DISTINCT subreddit_id) AS subreddits_count

-- FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full` AS lbl
-- INNER JOIN clusters_for_selected_subs AS sel
--     ON lbl.k_0150_label = sel.k_0150_label

-- GROUP BY 1, 2, 3

-- ORDER BY 1, 2, 3
-- ;


-- Check Top subreddits in each cluster
SELECT
    lbl.k_0100_label
    , lbl.k_0400_label
    , lbl.k_0600_label
    , lbl.k_0800_label
    , lbl.k_1000_label
    , lbl.model_sort_order
    , lbl.subreddit_name
    , lbl.primary_topic
    , lbl.posts_for_modeling_count
    , IF(s.subreddit_id IS NOT NULL, 1, 0) AS subreddit_seed_check

FROM `reddit-employee-datasets.david_bermejo.subclu_v0050_subreddit_clusters_c_full` AS lbl
INNER JOIN clusters_for_selected_subs AS sel
    ON lbl.k_0400_label = sel.k_0400_label
LEFT JOIN seed_subs as s
    ON s.subreddit_id = lbl.subreddit_id
LEFT JOIN top_n_subs_for_clusters as ts
    ON lbl.subreddit_id = ts.subreddit_id

WHERE 1=1
    AND (
        -- lbl.posts_for_modeling_count >= 400
        ts.subreddit_id IS NOT NULL
        OR s.subreddit_id IS NOT NULL
    )

ORDER BY 1, 2, 3, 4, 5, posts_for_modeling_count DESC
;
