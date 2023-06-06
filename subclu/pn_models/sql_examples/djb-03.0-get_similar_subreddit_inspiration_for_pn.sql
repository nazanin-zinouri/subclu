-- Find most similar subreddits for PN targeting based on input list
DECLARE PT_DT DATE DEFAULT CURRENT_DATE() - 2;
DECLARE MIN_FT2_SIMILARITY NUMERIC DEFAULT 0.6;

DECLARE MIN_USERS_L7 NUMERIC DEFAULT 100;
DECLARE MIN_POSTS_L7 NUMERIC DEFAULT 1;

DECLARE TARGET_SUBREDDIT_NAMES DEFAULT [
    "diablo"
    , "diablo4"
    , "diablo3"
    , 'diablo2'
    , 'diabloimmortal'
    , 'diablo_2_resurrected'
    , 'projectdiablo2'
    , 'diablo2resurrected'
    , 'pathofdiablo'
    , 'diabloswitch'
    , 'diablo4atclub'

    -- Not in model / no users from GB:
    , 'diablo4builds'
];
DECLARE TARGET_COUNTRY_CODES DEFAULT [
    "GB"
    -- , "FR"
    -- , "MX"
];

DECLARE PT_FT2 DATE DEFAULT (
    SELECT
        DATE(PARSE_TIMESTAMP("%Y%m%d", MAX(partition_id)))
    FROM `data-prod-165221.ml_content`.INFORMATION_SCHEMA.PARTITIONS
    WHERE table_name = "similar_subreddit_ft2"
        AND COALESCE(partition_id, '__NULL__') NOT IN (
            '__NULL__', '__UNPARTITIONED__'
        )
);


WITH
target_inputs AS (
    SELECT
        tn AS subreddit_name
        , 1 AS target_input
    FROM UNNEST(TARGET_SUBREDDIT_NAMES) tn
)
, ann_behavior_ranked AS (
    SELECT
        s.subreddit_id AS subreddit_id_seed
        , s.subreddit_name AS subreddit_name_seed
        , n.* EXCEPT(subreddit_id)
        , ROW_NUMBER() OVER(
            partition by s.subreddit_id ORDER BY score DESC
        ) distance_rank
        , asr.users_l7
        , asr.users_l28
        , asr.posts_l7
        , n.subreddit_id
    FROM `data-prod-165221.ml_content.similar_subreddit_ft2` AS s
        -- We need to UNNEST & join the field with nested JSON
        LEFT JOIN UNNEST(similar_subreddit) AS n
        LEFT JOIN (
            SELECT subreddit_name, users_l7, users_l28, posts_l7
            FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
            WHERE DATE(pt) = PT_DT
                AND NOT REGEXP_CONTAINS(LOWER(subreddit_name), r'^u_.*')
        ) AS asr
            ON LOWER(asr.subreddit_name) = n.subreddit_name

    WHERE s.pt = PT_FT2
        AND s.subreddit_name IN UNNEST(TARGET_SUBREDDIT_NAMES)
        AND users_l7 >= MIN_USERS_L7
        AND posts_l7 >= MIN_POSTS_L7
        -- After unnesting, we can apply filters based on nested fields
        AND n.score >= MIN_FT2_SIMILARITY
        AND s.subreddit_id IS NOT NULL
)
, ann_behavior_wide AS (
    -- We want to get one row for each candidate subreddit with aggregate scores/ranks
    SELECT
        subreddit_id
        , COALESCE(a.subreddit_name, t.subreddit_name) AS subreddit_name
        , COALESCE(t.target_input, 0) AS target_input
        , users_l28
        , users_l7
        , posts_l7

        , COUNT(DISTINCT subreddit_id_seed) AS relevant_input_count
        , AVG(score) AS score_avg
        , AVG(distance_rank) AS rank_avg

    FROM ann_behavior_ranked AS a
        FULL OUTER JOIN target_inputs AS t
            ON a.subreddit_name = t.subreddit_name
    GROUP BY 1,2,3,4,5,6
    -- ORDER BY target_input DESC, score_avg DESC
)
, ann_behavior_wide_ranks AS (
    SELECT
        aw.subreddit_id
        , subreddit_name
        , s.subscribers
        , s.public_description

        , ROW_NUMBER() OVER(ORDER BY users_l7 DESC) AS users_l7_rank
        , ROW_NUMBER() OVER(ORDER BY s.subscribers DESC) AS subscribers_rank

        -- DENSE is ok so we give the same rank to all subs with the same # of relevant inputs
        , DENSE_RANK() OVER( ORDER BY relevant_input_count DESC) AS relevant_input_rank

        , DENSE_RANK() OVER( ORDER BY score_avg DESC) AS score_avg_rank
        , DENSE_RANK() OVER( ORDER BY (score_avg * relevant_input_count) DESC) AS norm_score_avg_rank
        , DENSE_RANK() OVER( ORDER BY rank_avg ASC) AS rank_avg_rank
        , DENSE_RANK() OVER( ORDER BY SAFE_DIVIDE(rank_avg, relevant_input_count) ASC, relevant_input_count DESC) AS norm_rank_avg_rank
        , aw.* EXCEPT(subreddit_id, subreddit_name)
    FROM ann_behavior_wide AS aw
        LEFT JOIN (
            SELECT subreddit_id, subscribers, public_description
            FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
            WHERE dt = PT_DT
                AND NOT REGEXP_CONTAINS(name, r'^u_.*')
        ) AS s
            ON aw.subreddit_id = s.subreddit_id
)
, combined_rank AS (
    -- New output with combined rank
    SELECT
        PT_DT AS pt_activity
        , PT_FT2 AS pt_similarity
        , CASE
            WHEN subreddit_name IN UNNEST(TARGET_SUBREDDIT_NAMES) THEN 0
            ELSE DENSE_RANK() OVER(
                ORDER BY COALESCE(
                    IF(
                        subreddit_name IN UNNEST(TARGET_SUBREDDIT_NAMES),
                        NULL,
                        (
                            (15 * relevant_input_rank) +
                            score_avg_rank + norm_score_avg_rank +
                            (0.65 * users_l7_rank) + (0.45 * subscribers_rank) +
                            norm_rank_avg_rank
                        )
                    ),
                    1000
                ) ASC, users_l7 DESC, posts_l7 DESC
            )
        END AS combined_rank
        , target_input
        , subreddit_id
        , subreddit_name
        , users_l28
        , subscribers
        , users_l7
        , posts_l7
        , public_description
        , score_avg AS distance_avg

        , * EXCEPT(
            target_input
            , subreddit_id
            , subreddit_name
            , users_l28
            , subscribers
            , users_l7
            , posts_l7
            , public_description
            , score_avg
        )
    FROM ann_behavior_wide_ranks
)
, candidate_counts AS (
    SELECT
        m.* EXCEPT(target_subreddit_id, target_subreddit)
        , r.*
    FROM combined_rank AS r
        -- Add user counts from PN model
        LEFT JOIN (
            SELECT
                m.target_subreddit
                , m.target_subreddit_id
                , m.user_geo_country_code
                , COUNT(DISTINCT t.user_id) AS candidate_users
            FROM `reddit-growth-prod.pn_targeting.pn_model_subreddit_user_click_v1` AS m
                LEFT JOIN UNNEST(top_users) AS t

            WHERE
                pt = (
                    SELECT DATE(PARSE_TIMESTAMP("%Y%m%d", MAX(partition_id)))
                    FROM `reddit-growth-prod.pn_targeting`.INFORMATION_SCHEMA.PARTITIONS
                    WHERE table_name = "pn_model_subreddit_user_click_v1"
                        AND COALESCE(partition_id, '__NULL__') NOT IN (
                        '__NULL__', '__UNPARTITIONED__'
                    )
                )
                AND user_geo_country_code IN UNNEST(TARGET_COUNTRY_CODES)
            GROUP BY 1,2,3
        ) AS m
            ON r.subreddit_id = m.target_subreddit_id

    WHERE 1=1
        -- Limit to only the top N subs by rank to narrow results & improve signal
        AND combined_rank <= 25
)

SELECT
    user_geo_country_code AS user_geo
    , c.* EXCEPT(user_geo_country_code)
FROM candidate_counts AS c
    -- Get ratings
    LEFT JOIN `data-prod-165221.taxonomy.daily_export` AS tx
        ON c.subreddit_id = tx.subreddit_id
WHERE 1=1
    AND (
        combined_rank = 0
        OR (
            combined_rank > 0
            AND user_geo_country_code IS NOT NULL
        )
    )
ORDER BY target_input DESC, user_geo_country_code, candidate_users DESC, combined_rank ASC, users_l28 DESC
-- );  -- Close CREATE parens
