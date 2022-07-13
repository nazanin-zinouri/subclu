-- Check regexes for a) post URL, b) post URL slub, c) Title & body
SELECT
  * EXCEPT(
    post_id, submit_date
    , post_type
    , subreddit_name
    , flair_text
    , post_url
    , post_url_domain
    , post_url_to_concat
    , post_title_and_body_text_clean_len
    , post_flair_title_body_url_ocr_text_clean
    , text_from_lang_detection
    , post_title_and_body_text_clean
    , post_title_and_body_text
    , ocr_inferred_text_agg_clean
  )
  , post_id, submit_date
  , post_type
  , post_url
  , post_url_domain
  , post_url_to_concat
  , post_title_and_body_text_clean_len
  , subreddit_name
  , flair_text
  , post_flair_title_body_url_ocr_text_clean
  , text_from_lang_detection
  , post_title_and_body_text_clean
  , post_title_and_body_text
  , ocr_inferred_text_agg_clean

FROM `reddit-relevance.tmp.subclu_posts_for_modeling_20220501`
WHERE 1=1
  -- AND post_url_for_standalone_embedding IS NOT NULL
  -- AND post_url IS NOT NULL
  -- AND COALESCE(post_nsfw, false) = false
  -- AND post_title_and_body_text_raw_same_as_clean = false
  AND post_title_and_body_text_clean_len >= 1000

LIMIT 5000
;


-- alternate query
-- Check post clean up
SELECT
  rank_post_in_sub
  , post_id
  , removed
  , is_deleted
  , content_category, net_upvotes_lookup
  , weighted_language
  , post_url
  , post_url_domain
  , post_url_to_concat
  , post_url_path_raw
  , flair_text
  , flair_text_clean
  , sexually_explicit_image_pred_text
  , post_nsfw
  , post_type
  , subreddit_name
  , post_text_for_embeddings
  , ocr_inferred_text_agg
  , post_title_and_body_text_clean
  , ocr_inferred_text_agg_clean, post_title_and_body_text_clean_len

FROM `reddit-relevance.tmp.subclu_posts_for_modeling_20220624`
WHERE 1=1
    -- AND COALESCE(sexually_explicit_image_pred_text, '') != ''
    -- AND COALESCE(post_nsfw, FALSE) = TRUE
    -- AND ocr_inferred_text_agg_clean IS NOT NULL
    -- AND COALESCE(flair_text_clean, '') != ""
    -- AND post_title_and_body_text_clean_len >= 900

    -- Check URLs
    -- AND post_url_to_concat IS NOT NULL
    AND post_url_domain LIKE "%spoti%"

    -- AND subreddit_name NOT IN ('freekarma4u', 'gonewild')
    -- AND subreddit_name IN (
    --     'formula1'
    -- )

LIMIT 3000
;
