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
