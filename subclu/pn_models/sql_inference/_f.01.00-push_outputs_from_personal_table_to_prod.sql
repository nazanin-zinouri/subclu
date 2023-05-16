-- Create prod table for PN subreddit<>user model
DECLARE PT_TARGET DATE DEFAULT "2023-05-07";

-- ==================
-- Only need to create the first time we run the script
-- === OR REPLACE
CREATE TABLE `reddit-growth-prod.pn_targeting.pn_model_subreddit_user_click_v1`
PARTITION BY pt
AS (

-- ==================
-- After table is created, we can delete a partition & update it
-- ===
-- DELETE
--     `reddit-growth-prod.pn_targeting.pn_model_subreddit_user_click_v1`
-- WHERE
--     pt = PT_TARGET
-- ;

-- -- Insert latest data
-- INSERT INTO `reddit-growth-prod.pn_targeting.pn_model_subreddit_user_click_v1`
-- (



SELECT *
FROM `reddit-employee-datasets.david_bermejo.pn_model_output_20230510`
WHERE pt = PT_TARGET
);  -- Close CREATE/INSERT parens
