-- Get subreddits that match some primary or secondar languages
DECLARE TARGET_PRIMARY_LANGUAGES DEFAULT [
    'German'
];
DECLARE TARGET_SECONDARY_LANGUAGES DEFAULT [
    ''
];

-- What do you want to check?
--  'posts', 'comments', 'posts_and_comments'
DECLARE THING_TYPE_TO_COUNT STRING DEFAULT 'posts_and_comments';



WITH
sub_primary_language AS (
    SELECT
        lr.subreddit_id
        , lr.subreddit_name
        , lr.language_name AS primary_language
        , lr.language_percent AS primary_language_pct
        , lr.language_count AS primary_language_count
        , lr.thing_type

    FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_language_rank` lr
    WHERE 1=1
        AND thing_type = THING_TYPE_TO_COUNT
        AND language_rank = 1
)
, sub_2nd_language AS (
    SELECT
        lr.subreddit_id
        , lr.language_name AS secondary_language
        , lr.language_percent AS secondary_language_pct
        , lr.language_count AS secondary_language_count

    FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_language_rank` lr
    WHERE 1=1
        AND thing_type = THING_TYPE_TO_COUNT
        AND language_rank = 2
)


-- Use a self-join to get primary & secondary languages in the same column
SELECT
    l1.* EXCEPT(thing_type)
    , l2.* EXCEPT(subreddit_id)
    , l1.thing_type

FROM sub_primary_language AS l1
    LEFT JOIN sub_2nd_language AS l2
        ON l1.subreddit_id = l2.subreddit_id

WHERE 1=1
    AND (
        primary_language IN UNNEST(TARGET_PRIMARY_LANGUAGES)
        OR secondary_language IN UNNEST(TARGET_SECONDARY_LANGUAGES)
    )

ORDER BY subreddit_name
;
