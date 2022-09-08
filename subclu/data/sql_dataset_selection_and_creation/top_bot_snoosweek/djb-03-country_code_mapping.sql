-- Create view with clean country names
--   i.e., shorten long official names

DECLARE regex_cleanup_country_name_str STRING DEFAULT r" of Great Britain and Northern Ireland| of America";

CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.countrycode_name_mapping`
AS (
WITH clean_name_lookup AS (
    SELECT
        'KP' AS country_code
        , 'North Korea' AS country_name
    UNION ALL SELECT 'KR', 'South Korea'
    UNION ALL SELECT 'LA', 'Lao'
    UNION ALL SELECT 'VA', 'Vatican'
    UNION ALL SELECT 'BQ', 'Caribbean Netherlands'
    UNION ALL SELECT 'RU', 'Russia'
    UNION ALL SELECT 'VN', 'Vietnam'
    -- Kosovo's extension appears to be temporary
    UNION ALL SELECT 'XK', 'Kosovo'
)

SELECT
    cm.region
    , COALESCE(cm.country_code, cl.country_code) AS country_code
    -- Split to remove long official names like:
    --   Tanzania, United Republic of; Bolivia, Plurinational State of
    -- Regex replace long names w/o a comma
    , COALESCE(
        cl.country_name,
        REGEXP_REPLACE(
            SPLIT(cm.country_name, ', ')[OFFSET(0)],
            regex_cleanup_country_name_str, ""
        )
    ) AS country_name
    , cm.country_name AS country_name_raw
    , CURRENT_DATE() AS pt
FROM `data-prod-165221.ds_utility_tables.countrycode_region_mapping` AS cm
    FULL OUTER JOIN clean_name_lookup AS cl
        ON cm.country_code = cl.country_code

WHERE 1=1
    AND (
        cm.country_code != 'Country code'
        OR cl.country_code IS NOT NULL
    )

ORDER BY 3
)
;
