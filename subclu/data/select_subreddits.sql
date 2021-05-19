-- noinspection SqlNoDataSourceInspectionForFile

-- Create table with selected subs for initial clustering in German
-- Expected output: ~180 subreddits
DECLARE partition_date DATE DEFAULT '2021-05-18';

CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_selected_subs_20210519`
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

    -- Besides geo.rank_no, select based on posts + number of users w/ screen views
    -- D A CH = German, Austria, & Switzerland
    WHERE asr.posts_l28 >= 3
        AND (
            (
                geo.geo_country_code = "DE"
                AND (geo.rank_no <= 30 OR asr.users_l28 >= 29000)
            )
            OR (
                geo.geo_country_code = "AT"
                AND (geo.rank_no <= 5 OR asr.users_l28 >= 29000)
            )
            OR (
                geo.geo_country_code = "CH"
                AND (geo.rank_no <= 5 OR asr.users_l28 >= 29000)
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

    , COALESCE (
        sel.subreddit_info_ambassador,
        LOWER(dst.topic),
        "uncategorized"
    ) AS combined_topic
    , CASE
        WHEN rt.rating IN ("x", "nc17") THEN "over18_nsfw"
        WHEN dst.topic = "Mature Themes and Adult Content" THEN "over18_nsfw"
        ELSE COALESCE (
            sel.subreddit_info_ambassador,
            LOWER(dst.topic),
            "uncategorized"
        )
        END         AS combined_topic_and_rating

    , rt.rating
    , rt.version    AS rating_version

    , dst.topic
    , dst.version   AS topic_version

    -- Exclude subreddit_categories_view for now
    -- this table might be deprecated
    -- , dsc.category
    -- , dsc.subcategory
    -- , dsc.ads_prod_iab

    , asr.first_screenview_date
    , asr.last_screenview_date
    , asr.users_l7
    , asr.users_l28
    , asr.posts_l7
    , asr.posts_l28
    , asr.comments_l7
    , asr.comments_l28

    , CURRENT_DATE() as pt

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
-- Turns out this info is probably deprecated so don't use it for now
-- LEFT JOIN `data-prod-165221.ds_subreddit_whitelist_tables.subreddit_categories_view` AS dsc
--     ON sel.subreddit_name = dsc.subreddit_name


ORDER BY subreddit_name ASC, rank_no ASC
;


-- count posts & comments for last 28 days (l28)
-- Unclear if this includes removed posts & comments
-- Row	row_count	unique_subreddits_count	total_posts_l28	total_comments_l28
-- 1	174         174                     88725           808371
# SELECT
#     COUNT(*) AS row_count
#     , COUNT(DISTINCT subreddit_name)  AS unique_subreddits_count
#     , SUM(posts_l28)    AS total_posts_l28
#     , SUM(comments_l28) AS total_comments_l28
# FROM `reddit-employee-datasets.david_bermejo.subclu_selected_subs_2021_05_19`
