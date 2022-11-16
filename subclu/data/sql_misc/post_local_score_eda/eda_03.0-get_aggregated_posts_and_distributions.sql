-- Check current results (24 hours) & get distributions in sample subs

-- Get localness count per sub & country
--  Make a different cut later to include location of POSTER
DECLARE PT_DATE DATE DEFAULT '2022-11-10';
DECLARE POST_CREATED_DT DATE DEFAULT '2022-11-09';
DECLARE TEST_SUBS DEFAULT [
    'mexico'
    , 'askreddit'
    , 'mapporn'
    , 'fragreddit'
    , 'de'
    , 'france'
    , 'bundesliga'
    , 'ligamx'
    , 'casualuk'
    , 'personalfinancecanada'
    , 'dataisbeautiful'
];

DECLARE TEST_COUNTY_CODES DEFAULT [
    'MX'
    , 'DE'
    , 'US'
    , 'FR'
    , 'GB'
    , 'CA'
    , 'IN'
    , 'AU'
    , 'AT'
    , 'CH'
];


WITH
total_posts_per_sub AS (
    SELECT
        pl.subreddit_id
        , COUNT(DISTINCT post_id) AS post_total_count
    FROM `data-prod-165221.i18n.post_local_scores` AS pl
    WHERE DATE(pl.pt) = PT_DATE
        AND pl.post_create_dt = POST_CREATED_DT
        AND subreddit_name IN UNNEST(TEST_SUBS)

    GROUP BY 1
)
, agg_per_country AS (
    SELECT
        pl.subreddit_id
        , pl.geo_country_code
        , pl.subreddit_name
        , cm.country_name AS geo_country_name

        -- , poster_geo_country_code
        , localness

        , COUNT(DISTINCT pl.post_id) AS post_count

        -- Use post_dau_pct to think of new threshold for localness
        , ROUND(
            100.0 * APPROX_QUANTILES(post_dau_perc_24hr, 100)[OFFSET(5)], 2
        ) AS post_dau_pct_p05
        , ROUND(
            100.0 * APPROX_QUANTILES(post_dau_perc_24hr, 100)[OFFSET(10)], 2
        ) AS post_dau_pct_p10
        , ROUND(100 * AVG(post_dau_perc_24hr)) AS post_dau_pct_mean
        , ROUND(
            100.0 * APPROX_QUANTILES(post_dau_perc_24hr, 100)[OFFSET(90)], 2
        ) AS post_dau_pct_p90
        , ROUND(
            100.0 * APPROX_QUANTILES(post_dau_perc_24hr, 100)[OFFSET(95)], 2
        ) AS post_dau_pct_p95


        -- Use z-score to think of new threshold for localness
        , ROUND(AVG(perc_by_country_z_score), 2) AS z_score_mean

        -- Use DAU counts to think about filters for minimum # of DAU (no local score below a threshold)
        -- , ROUND(
        --     APPROX_QUANTILES(post_dau_24hr, 100)[OFFSET(5)], 2
        -- ) AS post_dau_p05
        -- , ROUND(
        --     APPROX_QUANTILES(post_dau_24hr, 100)[OFFSET(10)], 2
        -- ) AS post_dau_p10
        , ROUND(
            APPROX_QUANTILES(post_dau_24hr, 100)[OFFSET(15)], 2
        ) AS post_dau_p15
        , ROUND(
            APPROX_QUANTILES(post_dau_24hr, 100)[OFFSET(50)], 2
        ) AS post_dau_p50
        , ROUND(100 * AVG(post_dau_24hr), 2) AS post_dau_mean
        -- , ROUND(
        --     APPROX_QUANTILES(post_dau_24hr, 100)[OFFSET(80)], 2
        -- ) AS post_dau_p80
        -- , ROUND(
        --     APPROX_QUANTILES(post_dau_24hr, 100)[OFFSET(90)], 2
        -- ) AS post_dau_p90
        -- , ROUND(
        --     APPROX_QUANTILES(post_dau_24hr, 100)[OFFSET(95)], 2
        -- ) AS post_dau_p95


        -- , SUM(COUNT(DISTINCT pl.post_id)) OVER ()
        -- , ROUND( -- use MAX() because we only need to count the distinct posts ONCE, not sum them
        --     100.0 * COUNT(DISTINCT pl.post_id) / (MAX(COUNT(DISTINCT pl.post_id)) OVER (PARTITION BY pl.subreddit_id)),
        --     2
        -- ) AS pct_posts_in_sub

        -- , (MAX(COUNT(DISTINCT pl.post_id)) OVER (PARTITION BY pl.subreddit_id)) AS total_posts_in_sub


        -- , COUNT(DISTINCT pl.post_id) / (COUNT(DISTINCT pl.post_id) OVER (PARTITION BY pl.subreddit_id)) AS pct_of_posts
        -- , COUNT(DISTINCT pl.post_id) / (SUM(post_total_count) OVER (PARTITION BY pc.subreddit_id)) AS pct_of_posts
        -- , post_dau_24hr
        -- , ROUND(100.0 * post_dau_perc_24hr, 1) AS post_dau_perc_24hr
        -- , ROUND(100.0 * perc_by_country, 3) AS perc_by_country
        -- , ROUND(perc_by_country_z_score, 2) AS perc_by_country_z_score

    FROM `data-prod-165221.i18n.post_local_scores` AS pl
        LEFT JOIN `reddit-employee-datasets.david_bermejo.countrycode_name_mapping` AS cm
            ON pl.geo_country_code = cm.country_code
    WHERE DATE(pl.pt) = PT_DATE
        AND pl.post_create_dt = POST_CREATED_DT
        AND subreddit_name IN UNNEST(TEST_SUBS)
    GROUP BY 1,2,3,4,5
)


SELECT
    geo_country_code
    , ag.subreddit_id
    , ag.subreddit_name
    , geo_country_name
    , localness
    , post_count
    , ROUND(
        100.0 * (post_count / tp.post_total_count)
        , 2
    ) AS pct_posts_in_sub
    , ag.* EXCEPT(
        subreddit_id
        , subreddit_name
        , geo_country_code
        , geo_country_name
        , post_count
        , localness
    )


FROM agg_per_country AS ag
    LEFT JOIN total_posts_per_sub AS tp
        ON ag.subreddit_id = tp.subreddit_id
WHERE 1=1
    AND (
        geo_country_code IN UNNEST(TEST_COUNTY_CODES)
        -- OR geo_country_code IS NULL
        -- OR localness != 'not_local'
    )
    -- AND post_count >= 6

ORDER BY subreddit_name, geo_country_name, post_count DESC, post_dau_mean DESC # , country_name
;

