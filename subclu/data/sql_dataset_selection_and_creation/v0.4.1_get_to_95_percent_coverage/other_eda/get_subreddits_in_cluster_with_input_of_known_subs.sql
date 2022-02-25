-- Input a list of known subs and get related subreddits (same cluster ID)
WITH
    known_soccer_subs AS (
        SELECT *
        FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a`
        WHERE 1=1
            AND subreddit_name IN (
                'astonvilla',
                'fifa',
                'premierleague',
                'psg',
                'soccer',
                'fussball',
                'worldcup',
                'womenssoccer',

                -- national teams
                'dfb', 'ussoccer',
                'soceroos', 'uswnt'

                -- unclear why indianfootball fails
                -- 'indianfootball',
                -- Not in the language in model, so they throw things off
                -- 'norskfotball',
            )
    )


SELECT
    subreddit_id
    , * EXCEPT (subreddit_id)
FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a`
WHERE 1=1
    AND k_0657_label IN (
        SELECT DISTINCT k_0657_label FROM known_soccer_subs
    )
    -- AND k_0320_label IN (
    --     SELECT DISTINCT k_0320_label FROM known_soccer_subs
    -- )

ORDER BY model_sort_order
;
