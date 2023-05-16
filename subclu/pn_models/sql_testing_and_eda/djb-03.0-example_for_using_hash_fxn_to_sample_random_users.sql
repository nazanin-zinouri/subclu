-- Example for sampling users by using a hash function
-- Idea from:
--   https://www.oreilly.com/content/repeatable-sampling-of-data-sets-in-bigquery-for-machine-learning/
--   https://stackoverflow.com/questions/50443096/in-bigquery-how-to-random-split-query-results

SELECT
    user_geo_country_code
    , COUNT(*) AS user_count
FROM `reddit-employee-datasets.david_bermejo.pn_zelda_target_users_20230511`

WHERE
    -- Add this clause to sample 80% of user-ids from the table!
    --  This 80% (8 / 10) are the users we SEND the PN to
    MOD(ABS(FARM_FINGERPRINT(user_id)), 10) < 8
GROUP BY 1
;
