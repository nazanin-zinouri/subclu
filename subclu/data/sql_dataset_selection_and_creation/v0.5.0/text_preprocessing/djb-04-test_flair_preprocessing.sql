-- Test flair text pre-processing
DECLARE REGEX_REPLACE_WITH_SPACE_FLAIR STRING DEFAULT
    r"(?i)\"|\||\.com|:snoo_?|post_flair|post_|the_|[:_-]|\(|\)|\[|\]|\/|\^|\*|\\";
DECLARE REGEX_REMOVE_2ND_PASS_FLAIR STRING DEFAULT
    r"-";


SELECT
    -- Need to coalesce in case the regexes return an empty string
    COALESCE(TRIM(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    TRIM(flair_text)
                    , REGEX_REPLACE_WITH_SPACE_FLAIR, ' '
                ), REGEX_REMOVE_2ND_PASS_FLAIR, ' ' -- replace common items with space
            ), r"\s{2,}", r" " -- remove extra spaces
          )
    ), NULL) AS flair_text_clean
    , COUNT(DISTINCT post_id) AS posts_count
FROM `reddit-relevance.tmp.subclu_posts_for_modeling_20220606`
WHERE flair_text IS NOT NULL

GROUP BY 1

ORDER BY 2 DESC
;
