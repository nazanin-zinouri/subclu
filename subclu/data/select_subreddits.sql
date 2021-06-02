-- noinspection SqlNoDataSourceInspectionForFile

-- Create table with selected subs for initial clustering in German
-- Expected output: ~180 subreddits
DECLARE partition_date DATE DEFAULT '2021-05-30';
DECLARE regex_remove_str STRING DEFAULT r"https://|http://|www\.|/r/|\.html|reddit|\.com|\.org";
DECLARE regex_replace_with_space_str STRING DEFAULT r"wiki/|/|-|_|\?|&nbsp;";
DECLARE min_num_users_l28 NUMERIC DEFAULT 20000;

CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_selected_subs_20210601`
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
                AND (geo.rank_no <= 30 OR asr.users_l28 >= min_num_users_l28)
            )
            OR (
                geo.geo_country_code = "AT"
                AND (geo.rank_no <= 5 OR asr.users_l28 >= min_num_users_l28)
            )
            OR (
                geo.geo_country_code = "CH"
                AND (geo.rank_no <= 5 OR asr.users_l28 >= min_num_users_l28)
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
),

subreddit_lookup AS (
    SELECT
        *
        , COALESCE(array_length(regexp_extract_all(clean_description, r"\b\w+\b")), 0)      AS subreddit_clean_description_word_count
        , array_length(regexp_extract_all(subreddit_name_title_public_description, r"\b\w+\b"))       AS subreddit_name_title_public_description_word_count
        # do word count for full concat column on final query
        , CASE
            WHEN (description = public_description) THEN subreddit_name_title_public_description
            ELSE CONCAT(subreddit_name_title_public_description, ". \n", COALESCE(clean_description, ""))
            END AS subreddit_name_title_and_clean_descriptions

    FROM (
        SELECT
            *
            , TRIM(REGEXP_REPLACE(REGEXP_REPLACE(description, regex_remove_str, ""), regex_replace_with_space_str, " ")) AS clean_description
            , CONCAT(
                name, ". \n", COALESCE(title, ""), ". \n",
                COALESCE(
                    TRIM(REGEXP_REPLACE(REGEXP_REPLACE(public_description, regex_remove_str, ""), regex_replace_with_space_str, " ")),
                    "")
                ) AS subreddit_name_title_public_description

        FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`

        # Look back 2 days because looking back 1-day could be an empty partition
        WHERE dt = (CURRENT_DATE() - 2)
    )
),

final_table AS (
SELECT
    slo.subreddit_id
    , sel.*

    , COALESCE (
        sel.subreddit_info_ambassador,
        LOWER(dst.topic),
        "uncategorized"
    ) AS combined_topic
    , CASE
        WHEN rt.rating IN ("x", "nc17") THEN "over18_nsfw"
        WHEN dst.topic = "Mature Themes and Adult Content" THEN "over18_nsfw"
        WHEN slo.over_18 = "t" THEN "over18_nsfw"
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

    -- Meta from lookup
    , slo.over_18
    , slo.allow_top
    , slo.video_whitelisted
    , slo.lang      AS subreddit_language
    , slo.whitelist_status
    , slo.subscribers

    , asr.first_screenview_date
    , asr.last_screenview_date
    , asr.users_l7
    , asr.users_l28
    , asr.posts_l7
    , asr.posts_l28
    , asr.comments_l7
    , asr.comments_l28

    , CURRENT_DATE() as pt

    -- Text from lookup
    , slo.subreddit_clean_description_word_count
    , array_length(regexp_extract_all(subreddit_name_title_and_clean_descriptions, r"\b\w+\b")) subreddit_name_title_and_clean_descriptions_word_count
    , slo.title     AS subreddit_title
    , slo.public_description AS subreddit_public_description
    , slo.description AS subreddit_description
    # , slo.clean_description AS subreddit_clean_description
    , slo.subreddit_name_title_and_clean_descriptions

-- Use distinct in case a sub qualifies for more than 1 reason
FROM (SELECT DISTINCT * FROM selected_subs) AS sel

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

LEFT JOIN subreddit_lookup AS slo
    ON sel.subreddit_name = LOWER(slo.name)
)


SELECT DISTINCT * FROM final_table
ORDER BY users_l28 DESC, subscribers DESC
;


-- Count BEFORE creating table:
# SELECT
#     COUNT(*) AS row_count
#     , COUNT(DISTINCT subreddit_name)  AS unique_subreddits_count
#     , SUM(posts_l28)    AS total_posts_l28
#     , SUM(comments_l28) AS total_comments_l28
# FROM final_table
# ;

-- count posts & comments for last 28 days (l28) AFTER CREATING TABLE
-- Unclear if this includes removed posts & comments
-- Row	row_count	unique_subreddits_count	total_posts_l28	total_comments_l28
-- 1	174         174                     88725           808371
# SELECT
#     COUNT(*) AS row_count
#     , COUNT(DISTINCT subreddit_name)  AS unique_subreddits_count
#     , SUM(posts_l28)    AS total_posts_l28
#     , SUM(comments_l28) AS total_comments_l28
# FROM `reddit-employee-datasets.david_bermejo.subclu_selected_subs_20210519`


-- Export data to google cloud storage (GCS)
# EXPORT DATA OPTIONS(
#   uri='gs://i18n-subreddit-clustering/subreddits/2021-06-01/*.parquet',
#   format='PARQUET',
#   overwrite=true
#   ) AS
# SELECT *
# FROM `reddit-employee-datasets.david_bermejo.subclu_selected_subs_20210601`
# ;
