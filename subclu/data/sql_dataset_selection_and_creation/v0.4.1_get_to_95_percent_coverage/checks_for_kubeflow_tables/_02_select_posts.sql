-- ========================
-- Checks for CTEs
-- ===
-- Check post-language. row_num() gets rid of some dupes, but not all
--  See notes in query for dupe info
SELECT
  COUNT(*) as row_count
  , SUM(IF(post_thing_user_row_num = 1, 1, 0)) AS posts_with_rownum_1
  , COUNT(DISTINCT post_id) as post_id_count_unique
FROM post_language
;
-- Test counts (2 days)
--  total rows  row_num()=1 rows    unique post IDs
--  27,454,218  18,873,674          4,059,408


-- Check posts not removed counts. Here we expect row_num()=1 to give us unique post_ids
SELECT
  COUNT(*) as row_count
  , SUM(IF(row_num = 1, 1, 0)) AS posts_with_rownum_1
  , COUNT(DISTINCT post_id) as post_id_count_unique
FROM posts_not_removed
;
-- Test counts (2 days)
-- row_count	posts_with_rownum_1 post_id_count_unique
-- 1,795,553    1,795,457           1,795,457


-- Check posts_lang_and_meta,
--  We expect fewer posts here because we're removing some spam
SELECT
  COUNT(*) as row_count
  , COUNT(DISTINCT post_id) as post_id_count_unique
FROM posts_lang_and_meta
;
-- Test counts (2 days) [no post limit per subreddit]
-- row_count	post_id_count_unique    subreddit_count_unique
-- 1,573,476    1,573,476               61,071


-- Check `posts_lang_and_meta_top`
SELECT
  COUNT(*) as row_count
  , COUNT(DISTINCT post_id) as post_id_count_unique
  , COUNT(DISTINCT subreddit_id) as subreddit_count_unique
FROM posts_lang_and_meta_top
;
-- Test counts (2 days). ~3 min 14 sec @ 2,000 posts per subreddit
-- row_count    post_id_count_unique	subreddit_count_unique
-- 1,470,955    1,470,955               61,071


-- Check `posts_final_clean_top`
--  This table should have the same counts as the table before
--  The diff is that it should have new cols with clean text
SELECT
  COUNT(*) as row_count
  , COUNT(DISTINCT subreddit_id) as subreddit_count_unique
  , COUNT(DISTINCT post_id) as post_id_count_unique
FROM posts_final_clean_top
;
-- Test counts (2 days). ~3 min 16 sec @ 2,000 posts per subreddit
-- row_count    post_id_count_unique	subreddit_count_unique
-- 1,470,955    1,470,955               61,071



-- Check `ocr_text_agg`
-- We expect fewer posts (not all posts have images) & for them to be UNIQUE
SELECT
  COUNT(*) as row_count
  , COUNT(DISTINCT post_id) as post_id_count_unique
  , SUM(ocr_images_in_post_count) AS images_with_ocr
FROM ocr_text_agg
;
-- Test counts (2 days)
-- row_count	post_id_count_unique    images_with_ocr
-- 142,736      142,736                 157,885




-- ========================
-- Check for final table
-- ===
