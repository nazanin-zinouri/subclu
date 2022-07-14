-- Get BOTH: old geo-relevance AND new geo-relevance (cultural relevance)
--  And add latest rating & over_18 flags to get best estimate of SFW subs for clustering
DECLARE PARTITION_DATE DATE DEFAULT '2022-02-12';

-- Set minimum thresholds for scores: b & e
--  These thresholds are lower than the final definition, but use them to check what it would take
--  to make some subs relevant to some countries
DECLARE B_MIN_USERS_PCT_BY_SUB DEFAULT 0.12;
DECLARE E_MIN_USERS_PCT_BY_COUNTRY_STANDARDIZED DEFAULT 1.0;


SELECT
    nt.rating_name
    , nt.primary_topic
    , nt.rating_short
    , slo.over_18
    , CASE
        WHEN(COALESCE(slo.over_18, 'f') = 't') THEN 'over_18_or_X_M_D_V'
        WHEN(COALESCE(nt.rating_short, '') IN ('X', 'M', 'D', 'V')) THEN 'over_18_or_X_M_D_V'
        ELSE 'unrated_or_E'
    END AS grouped_rating
    , CASE
        WHEN(COALESCE(tm.subreddit_id, '') != '') THEN 'subreddit_in_model'
        ELSE 'subreddit_missing'
    END AS subreddit_in_v041_model
    , s.* EXCEPT(over_18, pt, verdict)

FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_score_standardized_20220212` AS s
    -- Add rating so we can get an estimate for how many we can actually use for recommendation
    LEFT JOIN (
        SELECT *
        FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
        WHERE dt = DATE(PARTITION_DATE)
    ) AS slo
    ON s.subreddit_id = slo.subreddit_id
    LEFT JOIN (
        SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
        WHERE pt = PARTITION_DATE
    ) AS nt
        ON s.subreddit_id = nt.subreddit_id

    -- Exclude popular US subreddits
    -- Can't query this table from local notebook because of errors getting google drive permissions. smh, excludefor now
    -- LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_us_to_exclude_from_relevance` tus
    --     ON s.subreddit_name = LOWER(tus.subreddit_name)

    -- Add latest table for v0.4.1 model so we can count which ones are missing
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` tm
        ON s.subreddit_id = tm.subreddit_id

WHERE 1=1
    AND s.subreddit_name != 'profile'
    AND COALESCE(s.type, '') = 'public'
    AND COALESCE(s.verdict, 'f') <> 'admin-removed'

    AND(
        s.geo_relevance_default = TRUE
        OR s.b_users_percent_by_subreddit >= B_MIN_USERS_PCT_BY_SUB
        OR s.e_users_percent_by_country_standardized >= E_MIN_USERS_PCT_BY_COUNTRY_STANDARDIZED
    )
    AND (
        country_name IN (
            'Germany', 'Austria', 'Switzerland', 'India', 'France', 'Brazil', 'Portugal', 'Italy',
            'Spain', 'Mexico', 'Argentina', 'Chile'
        )
        -- OR geo_region = 'LATAM' -- LATAM is noisy, focus on top countries instead
        -- eng-i18n =  Canada, UK, Australia
        OR geo_country_code IN ('CA', 'GB', 'AU')
    )

    -- AND (
    --     -- Exclude subs that are top in US but we want to exclude as culturally relevant
    --     --  For simplicity, let's go with the English exclusion (more relaxed) than the non-English one
    --     COALESCE(tus.english_exclude_from_relevance, '') <> 'exclude'
    -- )

ORDER BY users_l7 DESC, subreddit_name, e_users_percent_by_country_standardized DESC
;
