-- Get distance (nearest neighbors) for selected subreddits.
SELECT *
FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_distances_c_top_100`
WHERE 1=1
    AND subreddit_name_a IN (
        '1fcnuernberg'
    )
    AND distance_rank <= 15
;
