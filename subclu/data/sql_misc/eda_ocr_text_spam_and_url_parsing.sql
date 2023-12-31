-- EDA - how to clean up image and video links where
--  URL is not meaningful
-- examples:
--  * if post type=image and URL is LIKE "*/i.redd.it*, then ignore/null
--  * if post type=video and URL is LIKE "*/v.redd.it*, then ignore/null
DECLARE regex_remove_post_url STRING DEFAULT r"http[s]{0,1}://|www.|.html|utm|source=url";
DECLARE regex_replace_with_space_post_url STRING DEFAULT  r"/u/|/r/|/comments/|/|-|_+|\?|\&utm|\&|=|\+";

SELECT
    -- counts only
    -- COUNT(*)        AS row_count
    -- , COUNT(DISTINCT geo_p.post_id) AS post_id_unique_count
    -- , COUNTIF(plo.neutered = true)  AS neutered_count
    -- , COUNTIF(plo.deleted = true)   AS deleted_count

    -- query to check URLs
    -- slo.*
    geo_p.subreddit_name
    , geo_p.subreddit_id
    , geo_p.post_id
    , geo_p.post_type
    , geo_p.neutered
    , geo_p.verdict
    , LEFT(post_url, 100) AS post_url_left_
    -- , CASE
    --     WHEN STARTS_WITH(post_url, 'https://i.redd.it') THEN NULL
    --     WHEN STARTS_WITH(post_url, 'https://v.redd.it') THEN NULL
    --     WHEN REGEXP_INSTR(
    --         post_url,
    --         ARRAY_REVERSE(SPLIT(geo_p.post_id, "_"))[SAFE_OFFSET(0)]
    --         ) > 0 THEN NULL
    --     ELSE TRIM(REGEXP_REPLACE(REGEXP_REPLACE(post_url, regex_remove_post_url, ""), regex_replace_with_space_post_url, " "))
    -- END AS post_url_for_embeddings
    , post_url_for_embeddings

    , geo_relevant_countries
    , geo_relevant_subreddit
    , ocr_images_in_post_count
    , geo_p.flair_text
    , LEFT(text, 100)   AS text_left_
    , LEFT(ocr_inferred_text_agg_clean, 100)    AS ocr_text_left_
    -- , geo_posts.*

FROM `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20210927` AS geo_p
-- LEFT JOIN `data-prod-165221.ds_v2_postgres_tables.post_lookup` AS plo
--     ON plo.post_id = geo_p.post_id
WHERE 1=1
    -- AND DATE(plo._PARTITIONTIME) = (CURRENT_DATE() - 2)

    -- AND submit_date = "2021-09-21"
    AND post_url_for_embeddings IS NOT NULL
    AND STARTS_WITH(post_url, 'https://i.redd.it') = False

    -- posts that might be spam
    -- AND geo_p.post_id IN ('t3_ph6m81')
    AND geo_p.neutered = true

LIMIT 1000
;


-- Check neutered v. verdict post counts
SELECT
    -- groupby counts
    neutered
    , verdict
    -- , content_category

    , COUNT(DISTINCT post_id) post_id_unique_count
    -- inspect
    -- *
FROM `data-prod-165221.ds_v2_postgres_tables.post_lookup`
WHERE DATE(_PARTITIONTIME) = (CURRENT_DATE() - 2)
    -- AND content_category IS NOT NULL

GROUP BY 1, 2
    -- , 3

ORDER BY post_id_unique_count DESC, 2

LIMIT 1000
;


-- ========================
-- Cleaner query to check OCR text from r/de
-- ===
--  use it to check how well the system does at extracting
--  text for non-English languages
DECLARE start_date DATE DEFAULT '2021-10-09';
DECLARE end_date DATE DEFAULT '2021-10-11';

WITH ocr_text_agg AS (
        -- We need to agg the text because one post could have multiple images
        SELECT
            ocr.post_id
            , COUNT(media_url) AS ocr_images_in_post_count
            , TRIM(STRING_AGG(inferred_text, '. '))  AS ocr_inferred_text_agg
            , pt

        FROM `data-prod-165221.swat_tables.image_ocr_text` AS ocr

        WHERE DATE(ocr.pt) BETWEEN start_date AND end_date

        GROUP BY post_id, pt
    )


SELECT
    slo.subreddit_id
    , slo.name AS subreddit_name
    , plo.upvotes
    , ocr.*

FROM ocr_text_agg AS ocr
INNER JOIN (
        SELECT *
        FROM `data-prod-165221.ds_v2_postgres_tables.post_lookup`
        WHERE DATE(_PARTITIONTIME) = end_date
    ) AS plo
    ON ocr.post_id = plo.post_id
INNER JOIN (
        SELECT *
        FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
        WHERE DATE(dt) = end_date
    ) AS slo
    ON plo.subreddit_id = slo.subreddit_id

WHERE LOWER(slo.name) = 'de'

ORDER BY upvotes DESC
;
