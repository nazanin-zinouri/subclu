-- Select subs with new definition (16% from L14 days + % by COUNTRY)
DECLARE MIN_POSTS_L28_NOT_REMOVED NUMERIC DEFAULT 4;

-- All other i18n countries
--  From these, only India is expected to have a large number of English-language subreddits
--  Some i18n subs (like 1fcnuernberg) are only really active once a week b/c of game schedule
--   so they have few posts, but many comments. Add post + comment filter instead of only post
DECLARE min_users_geo_l7 NUMERIC DEFAULT 45;

SELECT * EXCEPT(views_dt_start, views_dt_end, pt, posts_not_removed_l28, users_l7)
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_score_pct_of_country_20220122` as geo
WHERE
    posts_not_removed_l28 >= MIN_POSTS_L28_NOT_REMOVED
    AND users_l7 >= min_users_geo_l7
    AND (
        country_name IN (
            'Germany', 'Austria', 'Switzerland', 'India', 'France', 'Spain', 'Brazil', 'Portugal', 'Italy',
            'Mexico', 'Argentina', 'Chile'
        )
        -- OR geo_region = 'LATAM' -- LATAM is noisy, focus on top countries instead
        -- eng-i18n =  Canada, UK, Australia
        OR geo_country_code IN ('CA', 'GB', 'AU')
    )
ORDER BY users_l7 DESC, subreddit_name
;
