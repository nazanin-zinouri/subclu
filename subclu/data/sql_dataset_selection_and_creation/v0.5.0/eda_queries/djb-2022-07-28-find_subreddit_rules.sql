-- We can check info schema to check column names
--  we expect that "rule" should be in a column that include subreddit or community rules
WITH target_tables AS (
    SELECT * FROM `data-prod-165221.cnc.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    UNION ALL SELECT * FROM `data-prod-165221.ds_v2_postgres_tables.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    UNION ALL SELECT * FROM `data-prod-165221.ds_subreddit_whitelist_tables.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    UNION ALL SELECT * FROM `data-prod-165221.ds_subreddit_whitelist_tables_staging.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    UNION ALL SELECT * FROM `data-prod-165221.swat_tables.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`

    UNION ALL SELECT * FROM `data-prod-165221.postgres_cdc_applied.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    UNION ALL SELECT * FROM `data-prod-165221.postgres_cdc_consolidated.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    UNION ALL SELECT * FROM `data-prod-165221.postgres_data_prod.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`

    UNION ALL SELECT * FROM `data-prod-165221.channels.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    UNION ALL SELECT * FROM `data-prod-165221.attributes.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    UNION ALL SELECT * FROM `data-prod-165221.community.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    UNION ALL SELECT * FROM `data-prod-165221.core_growth.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    UNION ALL SELECT * FROM `data-prod-165221.ds_gold_eng_tables.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    UNION ALL SELECT * FROM `data-prod-165221.ds_v2_gold_tables.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
)

SELECT
    *
FROM target_tables
WHERE 1=1
    AND column_name LIKE "%rule%"
    -- table_name="commits"
    -- OR column_name="difference"
ORDER BY column_name, table_schema
;


-- Once we find tables that include "rule", we can check whether they have any data:
--  As of 2022-07-28, all of these tables have null (empty) cells :sad-panda:
SELECT
    subreddit_id
    , subreddit_name
    , community_rules
FROM `data-prod-165221.swat_tables.subreddit_attributes`  -- rules empty
-- FROM `data-prod-165221.ds_subreddit_whitelist_tables.postgres_subreddit_attributes`  -- rules empty
-- FROM `data-prod-165221.ds_subreddit_whitelist_tables_staging.postgres_subreddit_attributes`  -- rules empty
WHERE 1=1
    AND community_rules IS NOT NULL
LIMIT 1000
;
