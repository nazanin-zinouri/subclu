-- Create table with default geo-relevance subreddits
DECLARE PARTITION_DATE DATE DEFAULT ${end_date};
DECLARE GEO_PT_START DATE DEFAULT PARTITION_DATE - 29;
DECLARE GEO_PT_END DATE DEFAULT PARTITION_DATE;

DECLARE MIN_USERS_L7 NUMERIC DEFAULT 45;
DECLARE MIN_POSTS_L28_NOT_REMOVED NUMERIC DEFAULT 4;


CREATE OR REPLACE TABLE `reddit-relevance.${dataset}.subclu_subreddit_geo_score_default_${run_id}`
AS (
WITH
    subs_geo_default_raw AS (
        SELECT
            geo.subreddit_id
            , LOWER(geo.subreddit_name) AS subreddit_name
            , geo.country AS geo_country_code
            , ssc.users_l7
            , ssc.posts_not_removed_l28
            , ssc.pt
            , ROW_NUMBER() OVER (PARTITION BY geo.subreddit_id, geo.country ORDER BY geo.pt desc) as sub_geo_rank_no

        FROM `data-prod-165221.i18n.all_geo_relevant_subreddits` AS geo

        -- This table should have:
        --  verdict & other info from subreddit_lookup
        --  calculated usersL7 & posts_not_removed_L28 from ars & sucessful_posts
        INNER JOIN `reddit-relevance.${dataset}.subclu_subreddit_candidates_${run_id}` AS ssc
            ON geo.subreddit_id = ssc.subreddit_id

        WHERE DATE(geo.pt) BETWEEN GEO_PT_START AND GEO_PT_END
            -- Enforce definition that requires N+ users in l7
            AND ssc.users_l7 >= MIN_USERS_L7
            AND ssc.posts_not_removed_l28 >= MIN_POSTS_L28_NOT_REMOVED
            AND geo.subreddit_id != 't5_4vo55w'  -- "r/profile" subreddit generates weird results

            -- Exclude user-profiles + spam & sketchy subs
            AND COALESCE(ssc.verdict, 'f') <> 'admin_removed'
            AND COALESCE(ssc.is_spam, FALSE) = FALSE
            AND COALESCE(ssc.is_deleted, FALSE) = FALSE
            AND ssc.deleted IS NULL
            AND NOT REGEXP_CONTAINS(LOWER(geo.subreddit_name), r'^u_.*')

            -- Keep only subs relevant to target countries
            AND (
                -- tier 0
                geo.country IN ('GB','AU','CA')

                -- tier 1
                OR geo.country IN ('DE','FR','BR','MX','IN')

                -- tier 2.  RU=Russia
                OR geo.country IN (
                    'IT', 'ES'
                    , 'NL', 'RO'
                    , 'DK', 'SE', 'FI'
                    , 'PH', 'TR', 'PL', 'RU'
                    -- Exclude, no support expected in 2022
                    , 'ID','JP','KR'
                )

                -- the US & other top 50/companion countries.  UA=Ukraine
                OR geo.country IN (
                    'US'
                    , 'PK'          -- India companion
                    , 'AT', 'CH'    -- Germany
                    , 'PT'          -- Brazil
                    , 'AR', 'CO'    -- Mexico

                    -- Exclude, no support expected in 2022
                    , 'SG', 'NZ', 'MY', 'NO', 'BE', 'IE'
                    , 'CZ', 'HU', 'ZA', 'CL', 'VN', 'HK', 'TH', 'GR', 'UA'
                    , 'IL', 'AE', 'TW', 'SA', 'PE', 'RS', 'HR'
                )
                -- Latin America: choose countries individually b/c region includes
                --  many small islands that add noise
                OR geo.country IN (
                    'AR', 'CL', 'CO', 'PE', 'PR'
                    -- Too small, no support expected
                    , 'BZ', 'BO', 'CR', 'CU', 'SV', 'DO', 'EC', 'GT'
                    , 'HN', 'NI', 'PA', 'PY',  'UY', 'VE'
                    , 'JM'
                )
          )
    )


SELECT
    a.* EXCEPT(sub_geo_rank_no)
    , TRUE AS geo_relevance_default
FROM subs_geo_default_raw AS a

WHERE 1=1
    AND a.sub_geo_rank_no = 1

ORDER BY a.geo_country_code ASC, a.users_l7 DESC, subreddit_name
);  -- close CREATE TABLE parens
