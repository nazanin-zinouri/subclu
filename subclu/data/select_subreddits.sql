-- noinspection SqlNoDataSourceInspectionForFile

-- Create table with selected subs for initial clustering in German
-- Expected output: ~180 subreddits
DECLARE partition_date DATE DEFAULT '2021-05-17';

CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_selected_subs_2021_05_19`
AS

WITH selected_subs AS
(
(
    SELECT
        geo.subreddit_name
        , geo.geo_country_code
        , geo.pct_sv_country
        , geo.rank_no
        , NULL AS subreddit_info_ambassador
        , NULL as subreddit_topic_ambassador

    FROM `reddit-employee-datasets.wacy_su.geo_relevant_subreddits_2021` AS geo
    -- option: "approved"/sfw list: geo_relevant_subreddits_intl_20200818_approved

    LEFT JOIN (
        -- Using sub-selection in case there are subs that haven't been registered in asr table
        SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
        WHERE DATE(pt) = partition_date
    ) AS asr
        ON geo.subreddit_name = asr.subreddit_name

    -- besides geo.rank_no, select based on number of users w/ screen views
    -- D A CH = German, Austria, & Switzerland
    WHERE (
        (
            geo.geo_country_code = "DE"
            AND (geo.rank_no <= 30 OR asr.users_l28 >= 30000)
        )
        OR (
            geo.geo_country_code = "AT"
            AND (geo.rank_no <= 5 OR asr.users_l28 >= 30000)
        )
        OR (geo.geo_country_code = "CH"
            AND (geo.rank_no <= 5 OR asr.users_l28 >= 30000)
        )
    )
)

UNION ALL
-- Wacy's table pulls data from a spreadsheet that Alex updates
(
    SELECT
        LOWER(amb.subreddit_name)
        , geo.geo_country_code
        , geo.pct_sv_country
        , geo.rank_no
        , TRIM(LOWER(amb.subreddit_info))     AS subreddit_info_ambassador
        , TRIM(LOWER(amb.topic))              AS subreddit_topic_ambassador

    FROM `reddit-employee-datasets.wacy_su.ambassador_subreddits` AS amb
    LEFT JOIN `reddit-employee-datasets.wacy_su.geo_relevant_subreddits_2021` AS geo
        ON LOWER(amb.subreddit_name) = geo.subreddit_name
    WHERE amb.subreddit_name IS NOT NULL
)

UNION ALL
-- Other subs I've found interesting
(
    SELECT
        s.subreddit_name
        , geo.geo_country_code
        , geo.pct_sv_country
        , geo.rank_no
        , NULL AS subreddit_info_ambassador
        , NULL as subreddit_topic_ambassador
    FROM (
        SELECT
            asr.subreddit_name
        FROM `data-prod-165221.all_reddit.all_reddit_subreddits` asr
        WHERE DATE(asr.pt) = partition_date
            AND asr.subreddit_name IN(
                "bier",
                "coronavirusdach",
                "dachschaden",
                "doener",
                "einfach_posten",
                "garten",
                "geschichtsmaimais",
                "haustiere",  -- pets!
                "keinstresskochen",
                "kochen",
                "nachrichten",
                -- "pietsmiet",    -- twitch streamer (gamer)
                "spabiergang"  -- walk w/ bier lol
                -- "schland"  -- private sub like 'murica(?)
                -- "vegande" -- qualifies with 30k users
            )
    ) AS s
    LEFT JOIN `reddit-employee-datasets.wacy_su.geo_relevant_subreddits_2021` AS geo
        ON s.subreddit_name = geo.subreddit_name

)
)


SELECT
    sel.*

    , rt.rating
    , rt.version AS rating_version

    , dst.topic
    , dst.version   AS topic_version

    , dsc.category
    , dsc.subcategory
    , dsc.ads_prod_iab

    , asr.first_screenview_date
    , asr.last_screenview_date
    , asr.users_l7
    , asr.users_l28
    , asr.posts_l7
    , asr.posts_l28
    , asr.comments_l7
    , asr.comments_l28

FROM selected_subs AS sel

LEFT JOIN (
    -- Using sub-selection in case there are subs that haven't been registered in asr table
    SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
    WHERE DATE(pt) = partition_date
) AS asr
    ON sel.subreddit_name = asr.subreddit_name

LEFT JOIN (
    SELECT * FROM `data-prod-165221.ds_v2_subreddit_tables.subreddit_ratings`
    WHERE DATE(pt) = partition_date
) AS rt
    ON sel.subreddit_name = rt.subreddit_name

LEFT JOIN(
    SELECT * FROM `data-prod-165221.ds_v2_subreddit_tables.subreddit_topics`
    WHERE DATE(pt) = partition_date
) AS dst
    ON sel.subreddit_name = dst.subreddit_name

-- subreddit_categories_view is a mix of:
--  subreddit_categories_inferred + subreddit_categories
--  where the manual labels are supposed to replace the inferred categories
LEFT JOIN `data-prod-165221.ds_subreddit_whitelist_tables.subreddit_categories_view` AS dsc
    ON sel.subreddit_name = dsc.subreddit_name


ORDER BY subreddit_name ASC, rank_no ASC
;
