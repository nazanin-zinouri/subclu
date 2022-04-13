-- Get the cluster ID & cluster subs, given an input sub name
DECLARE SUB_NAME STRING DEFAULT 'mexico';

WITH cluster_for_selected_sub AS(
    SELECT
        k_3145_label
    FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a`
    WHERE subreddit_name = SUB_NAME
)

SELECT
    model_sort_order
    , posts_for_modeling_count
    , subreddit_name
    , lbl.k_3145_label
    , k_3145_majority_primary_topic

FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` AS lbl
INNER JOIN cluster_for_selected_sub AS sel
    ON lbl.k_3145_label = sel.k_3145_label

WHERE 1=1

ORDER BY lbl.k_3145_label ASC, posts_for_modeling_count DESC
;
