-- Get City<>Subreddit Scores to help Local Onboarding Curation
DECLARE PT_END DATE DEFAULT "2023-02-15";
DECLARE PT_START DATE DEFAULT PT_END - 1;

DECLARE CITY_MIN_DAU NUMERIC DEFAULT 1000;

DECLARE MIN_USERS_L7 NUMERIC DEFAULT 700;
DECLARE MIN_USERS_L7_WITH_POSTS NUMERIC DEFAULT 100; -- lower threshold if subreddit has some posts in L7
DECLARE MIN_POSTS_L7 NUMERIC DEFAULT 1;


WITH
total_users AS (
    -- We need total users for the standardized scores
    SELECT
        geo_country_code
        , geo_region
        -- , geo_metro_code
        , geo_city

        , count(distinct user_id) as tot_users
    FROM `data-prod-165221.fact_tables.screenview_events` AS se
    WHERE DATE(se.pt) BETWEEN PT_START AND PT_END
        -- and geo_country_code in UNNEST(TARGET_COUNTRIES)
        AND not REGEXP_CONTAINS(subreddit_name, r"^u_.*")
        AND geo_city IS NOT NULL
    GROUP BY 1, 2, 3
    HAVING tot_users >= CITY_MIN_DAU
)
, subreddit_base AS (
    -- Caveat from all_reddit_subreddits: It counts attempted posts, but doesn't discount removed posts (spam or moderation)
    SELECT
        s.subreddit_id
        , asr.subreddit_name
        , asr.users_l7
        , asr.posts_l7
        , s.allow_discovery
        , s.whitelist_status
    FROM (
        SELECT
            subreddit_name
            , users_l7
            , posts_l7
        FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
        WHERE DATE(pt) = PT_END
            AND NOT REGEXP_CONTAINS(subreddit_name, r'^u_.*')
    ) AS asr
        -- Join with SLO b/c that might have more recent delete & banned info
        --  Also, it's a better source for subreddit_id
        INNER JOIN (
            SELECT *
            FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
            WHERE dt = PT_END
                AND NOT REGEXP_CONTAINS(name, r'^u_.*')
        ) AS s
            ON asr.subreddit_name = LOWER(s.name)
    WHERE 1=1
        -- Filter out subs below activity thresholds
        AND (
            users_l7 >= MIN_USERS_L7
            OR (
                -- Bots & spam subs can have posts w/o visits, so add a baseline
                users_l7 >= MIN_USERS_L7_WITH_POSTS
                AND posts_l7 >= MIN_POSTS_L7
            )
        )

        AND s.subreddit_id != 't5_4vo55w'  -- "r/profile" subreddit generates weird results

        -- Filter out subs based on spam/banned info
        AND COALESCE(s.verdict, "") <> "admin-removed"
        AND COALESCE(s.is_spam, FALSE) = FALSE
        AND COALESCE(s.is_deleted, FALSE) = FALSE
        AND s.deleted IS NULL
        AND type IN ('public', 'restricted')
)

-- SELECT * FROM total_users
SELECT * FROM subreddit_base
ORDER BY users_l7 DESC, posts_l7 DESC
;
