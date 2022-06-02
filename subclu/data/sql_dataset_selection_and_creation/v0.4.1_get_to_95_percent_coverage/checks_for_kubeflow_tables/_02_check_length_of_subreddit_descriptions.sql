-- Use this query to check distribution of subreddit description length (text)
-- Observed:
--   ~ 50 percentile:  298 characters (median)
--   ~       Average:  772
--   ~ 75 percentile:  914
--   ~ 85 percentile: 1560
--   ~ 90 percentile: 2135
--   ~ 95 percentile: 3156

SELECT
    COUNT(*) as row_count
    , COUNT(DISTINCT subreddit_id) AS subreddit_id_unique_count
    , COUNT(DISTINCT subreddit_name) AS subreddit_name_unique_count
    , SUM(
        CASE WHEN (active = TRUE) THEN 1
        ELSE 0
        END
    ) AS active_subreddit_count

    , MIN(subreddit_name_title_related_subs_and_clean_descriptions_len) AS sub_desc_len_min
    , APPROX_QUANTILES(subreddit_name_title_related_subs_and_clean_descriptions_len, 100)[OFFSET(50)] AS sub_desc_len_median
    , AVG(subreddit_name_title_related_subs_and_clean_descriptions_len) AS sub_desc_len_avg
    , APPROX_QUANTILES(subreddit_name_title_related_subs_and_clean_descriptions_len, 100)[OFFSET(75)] AS sub_desc_len_p75
    , APPROX_QUANTILES(subreddit_name_title_related_subs_and_clean_descriptions_len, 100)[OFFSET(85)] AS sub_desc_len_p85
    , APPROX_QUANTILES(subreddit_name_title_related_subs_and_clean_descriptions_len, 100)[OFFSET(90)] AS sub_desc_len_p90
    , APPROX_QUANTILES(subreddit_name_title_related_subs_and_clean_descriptions_len, 100)[OFFSET(95)] AS sub_desc_len_p95

FROM `reddit-relevance.tmp.subclu_subreddits_for_modeling_20220413`

WHERE 1=1
;
