-- Select only subreddits that have target language as primary language
WITH
    subreddits_with_target_languages AS (
        SELECT
            DISTINCT(subreddit_id) as subreddit_id
        FROM `reddit-employee-datasets.david_bermejo.subclu_posts_primary_language_by_subreddit_20220122` as geo
        WHERE
            (
                language_rank = 1
                -- OR language_rank = 2
            )
            AND language_name IN (
                -- exclude English for now
                -- 'English',
                'German',
                'French', 'Italian',
                'Spanish', 'Portuguese',

                -- Languages in India
                'Hindi',
                'Marathi',
                'Tamil',
                'Telugu',
                'Malayalam'
            )
    )


SELECT geo.*
FROM subreddits_with_target_languages sel
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_posts_primary_language_by_subreddit_20220122` AS geo
        ON sel.subreddit_id = geo.subreddit_id

WHERE
    language_rank <= 3
ORDER BY subreddit_name ASC, language_rank ASC
;
