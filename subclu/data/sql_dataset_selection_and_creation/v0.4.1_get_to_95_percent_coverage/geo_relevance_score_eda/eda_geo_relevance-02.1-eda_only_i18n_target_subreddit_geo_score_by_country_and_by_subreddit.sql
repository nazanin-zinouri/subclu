-- Select only target i18n countries for EDA in colab notebook
DECLARE MIN_USERS_L7 NUMERIC DEFAULT 2;
DECLARE MIN_POSTS_L28_NOT_REMOVED NUMERIC DEFAULT 2;


SELECT
    act.* EXCEPT (subreddit_name, subreddit_id, GEO_PT_START, GEO_PT_END)
    , geo.* EXCEPT (pt)
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_score_pct_of_country_20211214` geo
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_geo_subreddit_candidates_posts_no_removed_20211214` act
        ON geo.subreddit_id = act.subreddit_id
WHERE 1=1
    AND (
        country_name IN ('Germany', 'Austria', 'Switzerland', 'India', 'France', 'Spain', 'Brazil', 'Portugal', 'Italy')
        OR geo_region = 'LATAM'
        -- eng-i18n =  Canada, UK, Australia
        OR geo_country_code IN ('CA', 'GB', 'AU')
    )
    -- TODO(djb): explore r/profile later but exclude for now limit to subs above threshold
    AND users_l7 >=MIN_USERS_L7
    AND posts_not_removed_l28 >= MIN_POSTS_L28_NOT_REMOVED
;
