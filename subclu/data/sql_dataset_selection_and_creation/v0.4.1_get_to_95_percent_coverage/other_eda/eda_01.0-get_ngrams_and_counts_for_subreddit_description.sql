-- EDA to get ngrams on subreddit descriptions
-- Use the lessons here to get BM25 at post, subreddit, and i18n cluster level
--  Why? It should help with labeling clusters

-- replace common web tags
-- TODO(djb): maybe do some of the clean up in python (e.g., to clean up markdown markup)
DECLARE REGEX_REMOVE_STR STRING DEFAULT r"https://|http://|www\.|\.html|\.com|\.org";
DECLARE REGEX_REPLACE_WITH_SPACE_STR STRING DEFAULT r"wiki/|/|-|_|\?|&nbsp;";


WITH
    clean_text AS (
        SELECT
            *
            , TRIM(REGEXP_REPLACE(REGEXP_REPLACE(title, REGEX_REMOVE_STR, ""), "n't", "not")) AS title_clean
            , TRIM(REGEXP_REPLACE(REGEXP_REPLACE(description, REGEX_REMOVE_STR, ""), regex_replace_with_space_str, " ")) AS description_clean
            , TRIM(REGEXP_REPLACE(REGEXP_REPLACE(public_description, REGEX_REMOVE_STR, ""), regex_replace_with_space_str, " ")) AS public_description_clean
        FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
        # Look back 2 days because looking back 1-day could be an empty partition
        WHERE dt = (CURRENT_DATE() - 2)
            AND recent_activity >= 2
            AND subscribers >= 1000
            AND COALESCE(over_18, 'f') != 't'
    ),
    merged_text AS (
        SELECT
            *
            # do word count for full concat column on final query
            , CASE
                WHEN (description = public_description) THEN CONCAT(
                    name, "\n",
                    COALESCE(title_clean, ""), "\n",
                    COALESCE(public_description_clean, "")
                )
                ELSE CONCAT(
                    name, "\n",
                    COALESCE(title_clean, ""), "\n",
                    COALESCE(public_description_clean, ""), "\n",
                    COALESCE(description_clean, "")
                )
                END AS subreddit_name_title_and_clean_descriptions
        FROM clean_text
    ),
    text_as_array AS (
        SELECT
            *
            , regexp_extract_all(
                title_clean,
                r"\br\/[\p{L}]+|[\p{L}]+|\b\d{2,4}\b"
            ) AS title_clean_split

            -- , ARRAY_CONCAT_AGG(SPLIT(description, ' ')) AS split_description
            -- , ML.NGRAMS(ARRAY(description), [1, 2], ' ')
        FROM merged_text
    )

SELECT
    name
    -- , title
    , title_clean

    , title_clean_split

FROM text_as_array
WHERE 1=1
    AND LOWER(name) IN (
        -- numbers
        'anno1800',
        'y1883',

        -- emoji
        'superstonk',
        'satoshistreetbets',

        -- 'fifa',
        -- 'dankruto',
        'gamingleaksandrumours',
        -- 'me_irl',
        -- 'longdistance',  # contractions clean up (can't -> canot)

        -- other languages
        'china',
        'ich_iel',
        'russia',
        'de',
        'thaithai',
        'hanguk'  # Korea
    )
LIMIT 100
;
