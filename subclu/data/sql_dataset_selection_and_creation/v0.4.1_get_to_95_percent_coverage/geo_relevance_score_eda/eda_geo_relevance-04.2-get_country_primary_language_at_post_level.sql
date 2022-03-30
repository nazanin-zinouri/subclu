-- Use this query as a general check - we expect the primary language in
--  a COUNTRY to match primary language spoken in that country
DECLARE rating_date DATE DEFAULT '2022-01-22';

-- We can make the filters more strict later
DECLARE MIN_LANGUAGE_RANK NUMERIC DEFAULT 10;

CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subclu_posts_primary_language_by_country_20220122`
AS (
    WITH
    posts_lang AS (
        SELECT
            p.subreddit_id
            , p.subreddit_name
            , p.post_id
            , p.text_len
            , p.weighted_language
            , p.language_name
            , p.language_name_top_only
            , p.geolocation_country_name

        FROM `reddit-employee-datasets.david_bermejo.subclu_posts_primary_language_check_20220122` AS p
        WHERE geolocation_country_name IS NOT NULL
    ),
    subreddit_lang_posts AS (
        SELECT
            geolocation_country_name
            , weighted_language
            , 'post' AS thing_type
            , SUM(COUNT(post_id)) OVER (PARTITION BY geolocation_country_name) AS total_count
            , COUNT(post_id) AS language_count
            , ((0.0 + COUNT(post_id)) / (SUM(COUNT(post_id)) OVER (PARTITION BY geolocation_country_name))) as language_percent
        FROM posts_lang
        GROUP BY geolocation_country_name, weighted_language,  thing_type
    ),
    subreddit_lang_posts_rank AS (
        SELECT
            *
            , ROW_NUMBER() OVER (
                PARTITION BY geolocation_country_name
                ORDER BY language_percent DESC
            ) AS language_rank
        FROM subreddit_lang_posts
    )

SELECT
    a.*
    , ll.language_name
    , ll.language_name_top_only
    , ll.language_in_use_multilingual

FROM subreddit_lang_posts_rank AS a
    LEFT JOIN `reddit-employee-datasets.david_bermejo.language_detection_code_to_name_lookup_cld3` AS ll
        ON a.weighted_language = ll.language_code

WHERE 1=1
    AND language_rank <= MIN_LANGUAGE_RANK

ORDER BY geolocation_country_name ASC, language_rank ASC
);  -- Close CREATE TABLE statement


-- Select language for target i18n countries
--  Run query in EDA notebook
-- SELECT *
-- FROM `reddit-employee-datasets.david_bermejo.subclu_posts_primary_language_by_country_20220122` as geo
-- WHERE 1=1
--     AND (
--         geolocation_country_name IN (
--             'Germany', 'Austria', 'Switzerland', 'India', 'France', 'Spain', 'Brazil', 'Portugal', 'Italy',
--             'Mexico', 'Argentina', 'Chile',
--             'Canada', 'Australia', 'United Kingdom'
--         )
--     )
-- ORDER BY geolocation_country_name ASC, language_rank ASC
-- ;
