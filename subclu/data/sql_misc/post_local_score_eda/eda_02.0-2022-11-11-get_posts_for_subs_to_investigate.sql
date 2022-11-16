-- Use this query to check the score distribution for post-local EDA
-- Pick some subs+countries where we expect
--   - most posts to be STRICT local (e.g., r/de -> Germany)
--   - SOME posts to be STRICT or LOOSE local (e.g., r/bundesliga -> Germany)
--   - most posts to NOT be local (e.g., r/askReddit -> Germany)

-- Note: cnc.successful_post seems to be on a 48hr delay, but the post-score is on a 24hr delay.
--  If we need to get additional post-info, we'll need to get it somewhere else, ex:
--    data-prod-165221.andrelytics_ez_schema.post_submit
--    NVM: data-prod-165221.i18n.post_stats (pulls from: `data-prod-165221.fact_tables.post_consume_post_detail_view_events`)
--      - geo here is for VIEWS, not for poster

DECLARE PT_DATE DATE DEFAULT '2022-11-10';
DECLARE POST_CREATED_DT DATE DEFAULT '2022-11-09';

-- Pull all posts
SELECT
    pl.subreddit_id
    , geo_country_code
    , DATE(pl.pt) AS pt
    , pl.post_create_dt
    , pl.post_id
    , poster_geo_country_code
    , subreddit_name
    , cm.country_name
    , post_dau_24hr
    , ROUND(100.0 * post_dau_perc_24hr, 1) AS post_dau_perc_24hr
    , ROUND(100.0 * perc_by_country, 3) AS perc_by_country
    , ROUND(perc_by_country_z_score, 2) AS perc_by_country_z_score
    , localness
    , is_removed
    , is_spam

FROM `data-prod-165221.i18n.post_local_scores` AS pl
    LEFT JOIN `reddit-employee-datasets.david_bermejo.countrycode_name_mapping` AS cm
        ON pl.geo_country_code = cm.country_code
    LEFT JOIN (
        SELECT
            -- Use row_number to get the latest edit as row=1, (a post_id can have multiple rows when edited)
            ROW_NUMBER() OVER (
                PARTITION BY post_id
                ORDER BY endpoint_timestamp DESC
            ) AS row_num
            , post_id
            , user_id
            , subreddit_id
            , geo_country_code AS poster_geo_country_code
        FROM `data-prod-165221.andrelytics_ez_schema.post_submit`
        WHERE DATE(_PARTITIONTIME) = POST_CREATED_DT
        QUALIFY row_num = 1
    ) as ps
        ON pl.post_id = ps.post_id AND pl.subreddit_id = ps.subreddit_id
WHERE DATE(pl.pt) = PT_DATE
    AND pl.post_create_dt = POST_CREATED_DT

    AND (
        geo_country_code IN (
            'MX'
            , 'DE'
            , 'US'
            -- , 'FR'
            -- , 'BR'
            -- , 'CA', 'GB', 'IN', 'IE'
        )
        OR post_dau_perc_24hr >= 0.15
        OR localness != 'not_local'
    )
    AND subreddit_name IN (
        'mexico'
    )
ORDER BY subreddit_name, post_id, post_dau_24hr DESC
;

