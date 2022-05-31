-- Test to compare raw mature themes v. some pre-processing

DECLARE PARTITION_DATE DATE DEFAULT (CURRENT_DATE() - 2);

-- Regex for mature themes
-- profanity & terrorism are too broad and too easy for people to troll us on them
--  For terrorism, it's also hard to distinguish when it's describing/reporting or in fantasy (movies, games) or in reality
DECLARE REGEX_MATURE_REMOVE_STR STRING DEFAULT r"_ref|_full_liberal|_moderate|_sr_name|profanity[\_a-z]*|or_|_major|_partial|community_focus|_?terrorism|_full|guns_";

-- Function to remove duplicates when we extract unique mature themes
CREATE TEMP FUNCTION ARRAY_DISTINCT_STRING(STRING_ARRAY ANY TYPE) AS ((
    SELECT
        -- ARRAY_AGG(text_unique.item) -- This returns an array
        --   Note: for array_to_string, you need to select the individual item (column) array to convert into string!
        ARRAY_TO_STRING(ARRAY_AGG(text_unique.item), " ")
    FROM (
        SELECT DISTINCT TRIM(item) as item
        FROM UNNEST(STRING_ARRAY) AS item
    ) AS text_unique
));

SELECT
    nt.rating_name
    , nt.rating_short
    , nt.primary_topic
    , sel.users_l7
    , sel.subreddit_name
    , mature_themes_count_raw

    -- if primary topic is gaming or movies, exclude `violence` b/c it's not used consistently
    , IF(
        (nt.primary_topic IN ('Gaming', 'Movies', 'Anime', 'Television')) AND (COALESCE(nt.rating_short, '') != 'V')
        , REGEXP_REPLACE(mature_themes_deduped_text, r"violence ?", '')
        , mature_themes_deduped_text
    ) AS mature_themes_clean
    , mature_themes_deduped_text
    -- TODO(djb): add 'adult content' if sub is over_18

    , CASE
        WHEN CHAR_LENGTH(mature_themes_deduped_text) <= 0 THEN 0
        ELSE ARRAY_LENGTH(SPLIT(mature_themes_deduped_text, ' '))
    END AS mature_themes_word_count
    , ARRAY_TO_STRING(nt.mature_themes, ', ') AS mature_themes_raw

    -- , nt.* EXCEPT(subreddit_id)
FROM `reddit-relevance.tmp.subclu_subreddits_for_modeling_20220519` AS sel
    LEFT JOIN (
        SELECT
            subreddit_id
            , primary_topic
            , rating_short
            , rating_name
            , mature_themes
            , rating_weight
            , ARRAY_LENGTH(mature_themes)  AS mature_themes_count_raw
            , ARRAY_DISTINCT_STRING(
                SPLIT(
                    TRIM(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                ARRAY_TO_STRING(mature_themes, ' ')
                                , REGEX_MATURE_REMOVE_STR, ''
                            )  -- ref(erences) doesn't help the model.
                            , '_', ' '  -- replace underscores
                        )
                    )
                    , ' '
                )
            ) AS mature_themes_deduped_text
        FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
        WHERE pt = PARTITION_DATE
    ) AS nt
        ON sel.subreddit_id = nt.subreddit_id

    WHERE 1=1
        AND nt.rating_weight >= 9
        -- AND NOT (
        --     -- Profanity SR name doesn't help anything
        --     --   profanity in general doesn't matter, b/cthe model will pick it up
        --     'profanity_sr_name' IN UNNEST(mature_themes)
        --     OR 'profanity_regular' IN UNNEST(mature_themes)
        --     OR 'profanity_occasional' IN UNNEST(mature_themes)
        --     OR 'profanity_mild_occasional' IN UNNEST(mature_themes)
        -- )
        AND (
            subreddit_name LIKE "%interesting%"
            OR subreddit_name LIKE "gamer%"
            OR subreddit_name LIKE "%gaming%"
            OR nt.primary_topic IN (
                'Movies'
                ,'Television'
                -- , 'Anime'
                -- , 'Podcasts and Streamers'
            )
            OR nt.rating_short IN ('V')
        )
        AND mature_themes_count_raw >= 1

ORDER BY rating_short, primary_topic, users_l7 DESC
LIMIT 1000
;
