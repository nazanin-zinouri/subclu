-- Check coverage of content engine language detection
-- This table is supposed to be the new "source of truth"
--   TODO(djb): I haven't had time to actually check it, so I need time
--   to compare what it returns v. what I get from the cld3 tables


DECLARE PT_END DATE DEFAULT "2023-03-04";
DECLARE POST_PT_START DATE DEFAULT PT_END - 1;

-- check counts by subreddit
-- SELECT
--   subreddit_name
--   , COUNT(*) AS row_count
--   , COUNT(DISTINCT post_id) AS unique_post
-- FROM `data-prod-165221.fact_tables.content_engine_post_language_detection`
-- WHERE DATE(pt) BETWEEN POST_PT_START AND PT_END
--   -- Only posts from seed subreddits (optional/testing)
--   AND LOWER(subreddit_name) IN (
--       'de', 'mexico', 'meirl', 'ich_iel'
--       , 'india'
--       , 'france', 'rance'
--       , 'czech', 'prague', 'sweden'
--       , 'japan', 'china_irl', 'newsokunomoral'
--       , 'ligamx', 'absoluteunits', 'aww'
--   )
-- GROUP BY 1
-- ORDER BY unique_post DESC
-- ;

-- check counts by language
SELECT
  JSON_EXTRACT_SCALAR(ml_model_prediction_scores, "$.language") language_code
  , COUNT(*) AS row_count
  , COUNT(DISTINCT post_id) AS unique_post
FROM `data-prod-165221.fact_tables.content_engine_post_language_detection`
WHERE DATE(pt) BETWEEN POST_PT_START AND PT_END
  AND subreddit_name IN (
      'de', 'mexico', 'meirl', 'ich_iel'
      , 'india'
      , 'france', 'rance'
      , 'czech', 'prague', 'sweden'
      , 'japan', 'china_irl', 'newsokunomoral'
      , 'ligamx', 'absoluteunits', 'aww'
  )
GROUP BY 1
ORDER BY unique_post DESC
;
