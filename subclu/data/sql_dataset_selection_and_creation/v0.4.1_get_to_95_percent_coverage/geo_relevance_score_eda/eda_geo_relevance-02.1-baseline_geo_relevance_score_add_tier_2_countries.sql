-- Create table with default geo-relevance subreddits
-- This updated table includes default geo-relevance for top 50 countries
--  so that it includes tier 1 & tier 2 countries in a single location
DECLARE PARTITION_DATE DATE DEFAULT '2022-02-20';
DECLARE GEO_PT_START DATE DEFAULT PARTITION_DATE - 29;
DECLARE GEO_PT_END DATE DEFAULT PARTITION_DATE;
DECLARE RATING_DATE DATE DEFAULT PARTITION_DATE;

DECLARE MIN_USERS_L7 NUMERIC DEFAULT 100;
DECLARE MIN_POSTS_L28_NOT_REMOVED NUMERIC DEFAULT 4;


DECLARE regex_cleanup_country_name_str STRING DEFAULT r" of Great Britain and Northern Ireland| of America|";


CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_score_default_daily_20220222`
AS (
WITH
    subs_geo_default_raw AS (
        SELECT
            LOWER(geo.subreddit_name) AS subreddit_name
            , geo.subreddit_id
            , geo.country AS geo_country_code
            -- Split to remove long official names like:
            --   Tanzania, United Republic of; Bolivia, Plurinational State of
            -- Regex replace long names w/o a comma
            , REGEXP_REPLACE(
                SPLIT(cm.country_name, ', ')[OFFSET(0)],
                regex_cleanup_country_name_str, ""
            ) AS country_name
            , cm.region AS geo_region
            , asr.users_l7
            , ROW_NUMBER() OVER (PARTITION BY subreddit_id, country ORDER BY geo.pt desc) as sub_geo_rank_no

        FROM `data-prod-165221.i18n.all_geo_relevant_subreddits` AS geo
        INNER JOIN `data-prod-165221.ds_utility_tables.countrycode_region_mapping` AS cm
            ON geo.country = cm.country_code
        LEFT JOIN (
            SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
            WHERE DATE(pt) = PARTITION_DATE
        ) AS asr
            ON asr.subreddit_name = LOWER(geo.subreddit_name)

        WHERE DATE(geo.pt) BETWEEN GEO_PT_START AND GEO_PT_END
            -- Enforce definition that requires 100+ users in l7
            AND asr.users_l7 >= MIN_USERS_L7
            AND (
                -- tier 0
                geo.country IN ('GB','AU','CA')

                -- tier 1
                OR geo.country IN ('DE','FR','BR','MX','IN')

                -- tier 2
                OR geo.country IN ('IT','ES','JP','KR','PH','NL','TR','RO','DK','SE','FI','PL','ID','RU')

                -- other countries in top 50
                OR geo.country IN (
                    'SG', 'NZ', 'MY', 'NO', 'BE', 'IE', 'AR', 'AT', 'CH', 'PT',
                    'CZ', 'HU', 'ZA', 'CL', 'VN', 'HK', 'TH', 'CO', 'GR', 'UA',
                    'IL', 'AE', 'TW', 'SA', 'PE', 'RS', 'HR'
                )
          )
    ),
    subs_geo_w_post_count AS (
        SELECT
            c.*
            , COUNT(DISTINCT sp.post_id) as posts_not_removed_l28
        FROM subs_geo_default_raw AS c
            LEFT JOIN (
                    SELECT *
                    FROM `data-prod-165221.cnc.successful_posts`
                    WHERE (dt) BETWEEN GEO_PT_START AND PARTITION_DATE
                        AND removed = 0
                ) AS sp
                    ON c.subreddit_id = sp.subreddit_id AND c.subreddit_name = sp.subreddit_name
        WHERE
            -- Keep only one row per subreddit, even if it's relevant on multiple days
            c.sub_geo_rank_no = 1
        GROUP BY 1, 2, 3, 4, 5, 6, 7
    )


SELECT
    nt.rating_short
    , nt.rating_name
    , nt.primary_topic
    , TRUE AS geo_relevance_default
    , a.* EXCEPT(sub_geo_rank_no)
    , PARTITION_DATE AS pt
    , GEO_PT_START AS dt_start_post_count

FROM subs_geo_w_post_count AS a
    LEFT JOIN (
        SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
        WHERE pt = rating_date
    ) AS nt
        ON a.subreddit_id = nt.subreddit_id

WHERE
    posts_not_removed_l28 >= MIN_POSTS_L28_NOT_REMOVED
ORDER BY users_l7 DESC, subreddit_name
);  -- close CREATE TABLE parens
