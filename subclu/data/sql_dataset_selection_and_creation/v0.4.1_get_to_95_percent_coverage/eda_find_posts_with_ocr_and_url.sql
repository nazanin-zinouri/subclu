-- Use this query as baseline to find
-- examples where model uses multiple forms of input
SELECT
    *
FROM `reddit-employee-datasets.david_bermejo.subclu_posts_top_no_geo_20211214`

WHERE ocr_inferred_text_agg_clean IS NOT NULL
    AND post_url_for_embeddings IS NOT NULL
    AND comments > 5
    AND subreddit_name IN ('kendricklamar', 'hiphopcirclejerk', 'hiphopheads', 'hiphop101', 'rap')
    -- AND subreddit_name IN ('edm', 'lofi', 'punk')


ORDER BY net_upvotes_lookup DESC
LIMIT 1000
;
