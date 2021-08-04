-- Also try to keep queries in wiki so they're easier for people to use & find them:
--   https://reddit.atlassian.net/wiki/spaces/DataScience/pages/2113142796/How+to+Query+Model+Outputs

-- Get counts of subs by manual label
SELECT
    manual_topic_and_rating
    , COUNT(DISTINCT subreddit_name) AS unique_subreddit_count
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v031_a`
GROUP BY 1
ORDER BY 2 DESC
;

-- Get most similar sub, given an input sub name
SELECT * EXCEPT(subreddit_id_a, subreddit_id_b)
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_distance_v0031_german_c_posts_and_comments_and_meta`
WHERE subreddit_name_a = 'bundesliga'
;


-- Get the cluster ID & cluster subs, given an input sub name
WITH cluster_for_selected_sub AS(
SELECT
    cluster_id_agg_ward_cosine_35
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v031_a`
WHERE subreddit_name = 'bundesliga'
)

SELECT
    subreddit_name
    , manual_topic_and_rating
    , lbl.cluster_id_agg_ward_cosine_35
    , post_median_word_count
    , German_posts_percent
    , subreddit_language
    , posts_l28
    , subscribers
    , users_l7
    , users_l28
    , subreddit_title
    , subreddit_public_description

    , svd_0
    , svd_1
    , svd_2

FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v031_a` AS lbl
INNER JOIN cluster_for_selected_sub AS sel
    ON lbl.cluster_id_agg_ward_cosine_35 = sel.cluster_id_agg_ward_cosine_35

WHERE manual_topic_and_rating != 'over18_nsfw'
ORDER BY cluster_id_agg_ward_cosine_35 ASC, users_l28 DESC
;



