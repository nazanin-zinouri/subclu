-- The ANN for ireland is fine
-- The problem is that when I create dynamic clusters, I end up with ireland -> Aldi / McDonalds
-- Fix:
--  after creating dynamic clusters, add new constraint:
--   - only keep recommendations where the a -> b distance is in the top 100

-- Top recommendations by distance are fine:
SELECT *
FROM `reddit-employee-datasets.david_bermejo.subclu_v0050_subreddit_distances_c_top_100`
WHERE 1=1
  AND subreddit_name_a = 'ireland'
LIMIT 1000
;


-- The problem is that the dynamic clusters are too broad.
--  The distance b/n some of these is way beyond the top 100.
SELECT *
FROM `reddit-employee-datasets.david_bermejo.subclu_v0050_fpr_outputs`
WHERE 1=1
  AND subreddit_name_seed = "ireland"
LIMIT 1000
;
