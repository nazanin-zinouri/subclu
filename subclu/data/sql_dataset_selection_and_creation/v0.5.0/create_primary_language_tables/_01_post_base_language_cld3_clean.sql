-- Create base table with language post counts
-- Use this as foundation to get subreddit primary language
--  Includes language per post date so that we can do language trends over time
DECLARE PT_END DATE DEFAULT "2022-08-07";
DECLARE POST_PT_START DATE DEFAULT PT_END - 120;


CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.post_language_detection_cld3_clean`
PARTITION BY dt AS (
WITH
    post_language AS (
        -- This table has duplicates for (at least) 2 reasons:
        --  * When OP comments on their post it triggers a 2nd "post" event
        --  * When someone makes a comment it can trigger a "post" event but with the user_id of the commenter (instead of OP)
        --  * Unclear if edits by OP create a new row

        -- In this CTE, we remove *some* duplicates by using row_number
        -- We get final UNIQUE post-ids by JOINING on: user_id (OP), post_id, and subreddit_id
        -- Example for 1 day:
        --  total rows  | row_num()=1 rows | unique post IDs
        --  9.4 million | 6.7 million      | 1.7 million

        SELECT
            sp.dt
            , sp.submit_date
            , sp.subreddit_id
            , sp.post_id
            , sp.user_id
            -- Only add subreddit name from latest partition to prevent errors when subreddit changes names
            , LOWER(slo.name) AS subreddit_name
            , sp.removed
            , sp.is_deleted
            , sp.post_type
            , CHAR_LENGTH(
                COALESCE(
                    TRIM(pl.text),
                    CONCAT(sp.post_title, COALESCE(CONCAT(' ', sp.post_body_text), ''))
                )
            ) AS post_title_and_body_text_length
            , CHAR_LENGTH(post_title) AS post_title_length
            , CHAR_LENGTH(post_body_text) AS post_body_length
            , (sp.post_body_text IS NULL) OR (CHAR_LENGTH(post_body_text) = 0) post_body_text_null
            , COALESCE(lc1.language_code = pl.weighted_language, FALSE) AS top1_equals_weighted_language_code

            , lc1.language_name AS top1_language_name
            , COALESCE(lc1.language_code, 'UNPROCESSED') AS top1_language_code
            , pl.cld3_top1_probability AS top1_language_probability

            , lc.language_name AS weighted_language_name
            , COALESCE(pl.weighted_language, 'UNPROCESSED') AS weighted_language_code
            , pl.weighted_probability AS weighted_language_probability
            , sp.geo_country_code

            , pl.text
            , sp.post_title
            , sp.post_body_text

            -- Rank by post-ID + user_id
            --  Sort by created DESC to get latest value
            , ROW_NUMBER() OVER(
                PARTITION BY pl.post_id, pl.user_id
                -- PARTITION BY pl.post_id
                ORDER BY pl.created_timestamp DESC
            ) AS post_thing_user_row_num

        FROM (
            SELECT
                subreddit_id
                , user_id
                , dt
                , submit_date
                , post_id
                , post_type
                , removed
                , is_deleted
                , post_title
                , post_body_text
                , geo_country_code
            FROM `data-prod-165221.cnc.successful_posts`
            WHERE dt BETWEEN POST_PT_START AND PT_END
        ) AS sp
            LEFT JOIN `data-prod-165221.language_detection.post_language_detection_cld3` AS pl
                ON sp.subreddit_id = pl.subreddit_id
                    AND sp.post_id = pl.post_id
                    -- Get pt date +1 in case the language job was lagging
                    AND sp.dt BETWEEN (DATE(pl._PARTITIONTIME)) AND (DATE(pl._PARTITIONTIME) + 1)

            LEFT JOIN `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup` AS slo
                ON sp.subreddit_id = slo.subreddit_id

            LEFT JOIN `reddit-employee-datasets.david_bermejo.language_detection_code_to_name_lookup_cld3` AS lc
                ON pl.weighted_language = lc.language_code

            LEFT JOIN `reddit-employee-datasets.david_bermejo.language_detection_code_to_name_lookup_cld3` AS lc1
                ON pl.cld3_top1_language = lc1.language_id

        WHERE 1=1
            AND DATE(pl._PARTITIONTIME) BETWEEN POST_PT_START AND PT_END
            AND slo.dt = PT_END

            -- Remove duplicates
            QUALIFY ROW_NUMBER() OVER(
                PARTITION BY pl.post_id, sp.user_id
                ORDER BY created_timestamp DESC
            ) = 1

            -- check language codes to fix
            -- AND (
            --     -- lcr.id <= 11
            --     lcr.id >=10
            --     AND lcr.id != 17
            -- )
            -- AND pl.weighted_language != 'en'

            -- Only posts from seed subreddits (optional/testing)
            -- AND LOWER(slo.name) IN (
            --     'de', 'mexico', 'india', 'meirl', 'ich_iel', 'france'
            --     , 'czech', 'prague', 'sweden'
            --     , 'japan', 'china_irl', 'newsokunomoral'
            --     -- , 'askreddit'
            -- )
    )


-- Select deduped posts and some precomputed stats
SELECT * EXCEPT(post_thing_user_row_num)
FROM post_language
); -- Close create table parens
# LIMIT 2000


-- Get Length aggregates per post type
--  Check how much longer are "text" posts than other posts?
-- SELECT
--     post_type
--     , AVG(COALESCE(post_title_length, 0)) AS post_title_len_avg
--     , AVG(post_body_length) AS post_body_len_avg_if_not_null
--     , AVG(post_title_and_body_text_length) AS post_title_and_body_len_avg
--     , COUNT(DISTINCT post_id) AS post_id_count
--     , ROUND(100.0 * COUNT(DISTINCT post_id) / (SELECT COUNT(*) FROM post_language), 3) AS posts_pct
-- FROM post_language
-- GROUP BY 1
-- ORDER BY 5 DESC, 2 DESC
-- ;

-- Check counts of post language table
--  All should be the same value
-- SELECT
--     COUNT(*) as row_count
--     , COUNT(DISTINCT post_id) as posts_unique_count
--     , SUM(IF(post_thing_user_row_num = 1, 1, 0)) as row_num1_posts
-- FROM post_language
-- ;


-- Check top languages (overall)
-- SELECT
--     weighted_language_name
--     -- , top1_language_name
--     -- , removed

--     , STRING_AGG(DISTINCT(weighted_language_code), ',') AS weighted_language_codes
--     , COUNT(DISTINCT post_id) as posts_unique_count
--     , ROUND(100.0 * COUNT(DISTINCT post_id) / (SELECT COUNT(*) FROM post_language), 3) AS posts_pct
--     -- , COUNT(*) AS row_count

-- FROM post_language AS p

-- WHERE 1=1
--     AND post_title_and_body_text_length >= 1
--     -- AND weighted_language_name IN (
--     --     'Chinese', 'Russian'
--     -- )
-- GROUP BY 1  -- , 2  -- 3=removed
-- ORDER BY 3 DESC, 1 ASC
-- ;


-- Check codes (overall)
--  if we filter by code.id flag languages that look wrong
-- SELECT
--     lc.language_name
--     , p.weighted_language_code
--     , p.top1_language_code

--     -- , COUNT(DISTINCT p.top1_language_code) as language_code_count
--     -- , STRING_AGG(DISTINCT(weighted_language_code), ',') AS weighted_language_codes
--     -- , STRING_AGG(DISTINCT(p.top1_language_code), ',') AS top1_language_codes
--     , COUNT(DISTINCT post_id) as posts_unique_count
--     , ROUND(100.0 * COUNT(DISTINCT post_id) / (SELECT COUNT(*) FROM post_language), 2) AS posts_pct

-- FROM post_language AS p
--     LEFT JOIN `reddit-employee-datasets.david_bermejo.language_detection_code_to_name_lookup_cld3` AS lc
--         ON p.weighted_language_code = lc.language_code

-- WHERE 1=1
--     -- AND post_title_and_body_text_length >= 9

-- GROUP BY 1, 2, 3

-- HAVING (
--     posts_unique_count >= 10
--     AND weighted_language_code != top1_language_code
-- )
-- ORDER BY 1 ASC, 4 DESC
-- ;
