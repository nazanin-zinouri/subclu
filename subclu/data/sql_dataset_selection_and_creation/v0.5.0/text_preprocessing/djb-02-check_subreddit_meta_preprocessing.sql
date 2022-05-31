-- Create table with selected subs for subs for topic modeling
--    Clustering includes NSFW -- we are only excluding subs that were banned or marked as spam
--  v0.3.2 pull we had 3,700 subs (mostly Germany-relevant)
--  v0.4.0 ~19k subs
--  v0.4.1 ~50k subs
--  v0.5.0 ~80k subs

DECLARE PARTITION_DATE DATE DEFAULT (CURRENT_DATE() - 2);

-- Regex for subreddit description
DECLARE REGEX_REMOVE_STR STRING DEFAULT r"(?i)https://|http://|\w{1,3}\.reddit|redd.it|\.gg|goo.gl|bit.ly|search\?\w+=|www\.|\br/|/r/|\.html|\.com|\.org|#|/index|\*|\n-{2,}|\< ";
DECLARE REGEX_REPLACE_WITH_SPACE_STR STRING DEFAULT r"(?i)/?wiki/|#wiki_|\b/\w+\?\w+=|&\w{1,}=|\?\w+=|\)\|?\[|\]\|?\(| *\| *|&nbsp;| {2,}|flair%3A|%3A|%2B|%\w{2}|/|-|_";
DECLARE REGEX_REMOVE_2ND_PASS_STR STRING DEFAULT r"\|?:?-+:?\|{1,}|\(|\)|\[|\]|\>|\^| +\| +|: +:\||\|{2,}|\n\|+ {0,9}\|{0,9}|\n ?: +:|\x60|~|={2,}|:{2,}";

-- Regex for mature themes
-- profanity & terrorism are too broad and too easy for people to troll us on them
--  For terrorism, it's also hard to distinguish when it's describing/reporting or in fantasy (movies, games) or in reality
DECLARE REGEX_MATURE_REMOVE_STR STRING DEFAULT r"_ref|_full_liberal|_moderate|_sr_name|profanity[\_a-z]*|or_|_major|_partial|community_focus|\_?terrorism|_full|guns_";

-- Besides making sure that English/high-activity communities have the `active` flag
--  we also make sure that there are a miniumn number of users in L7
DECLARE MIN_ACTIVE_SUB_USERS_l7 NUMERIC DEFAULT 45;
DECLARE MIN_ACTIVE_SUB_POSTS_L28 NUMERIC DEFAULT 3;

-- Increase threshold for non-active to increase quality of counterparts
--  & reduce noise from small subreddits
DECLARE MIN_NON_ACTIVE_SUB_USERS_l7 NUMERIC DEFAULT 150;
DECLARE MIN_NON_ACTIVE_SUB_POSTS_L28 NUMERIC DEFAULT 8;

-- Filters for i18n countries
--  These filters have moved to this table: subclu_subreddit_geo_selected_XXXX

-- Function to remove duplicates when we extract subreddit names from a sub description
CREATE TEMP FUNCTION ARRAY_DISTINCT_STRING(
    STRING_ARRAY ANY TYPE
    , array_sep STRING
) AS ((
    SELECT
        -- ARRAY_AGG(text_unique.item) -- This returns an array
        -- Note: for array_to_string, you need to select the individual item (column) array to convert into string!
        ARRAY_TO_STRING(ARRAY_AGG(text_unique.item), array_sep)
    FROM (
        SELECT
            DISTINCT
            TRIM(REGEXP_REPLACE(item, "^r/", '')) as item -- remove r/ to reduce character overhead
        FROM UNNEST(STRING_ARRAY) AS item
    ) AS text_unique
));

-- Function to clean up description & public_description
--  make it a function b/c we'll need to apply it to multiple columns
CREATE TEMP FUNCTION CLEAN_DESCRIPTION_TEXT(
    description_col ANY TYPE
    , REGEX_REMOVE_STR STRING
    , REGEX_REPLACE_WITH_SPACE_STR STRING
    , REGEX_REMOVE_2ND_PASS_STR STRING
) AS ((
    SELECT
        CASE
            WHEN description_col IS NULL THEN ''
            ELSE TRIM(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(COALESCE(description_col, ''), REGEX_REMOVE_STR, ""),
                                    REGEX_REPLACE_WITH_SPACE_STR, " "
                                ),
                                REGEX_REMOVE_2ND_PASS_STR, ""
                            ), " {2,}", " " -- Remove multiple spaces next to each other
                        ),
                        r"\n *\n *\n *\n*|\n{3,}", "\n\n" -- remove multiple new lines
                    )
                )
        END
));


-- Start CREATE TABLE statement
-- CREATE OR REPLACE TABLE `reddit-relevance.tmp.subclu_subreddits_for_modeling_20220519`
-- AS (
    WITH
    -- Most of the logic has moved to a candidates table to make querying easier/faster
    -- For highly active subs (usually English/US) we mostly rely on CnC's "active" definition flag

    -- Here's where we apply filters and update flags for: top (no geo), geo-top, & ambassador subs
    selected_subs AS (
        SELECT
            gc.i18n_type
            , sel.subreddit_id
            , sel.subreddit_name
            , gc.geo_relevant_country_count
            , gc.geo_relevant_countries
            , gc.geo_relevant_country_codes
            , sel.* EXCEPT(subreddit_name, subreddit_id, i18n_type)

        FROM `reddit-relevance.tmp.subclu_subreddit_candidates_20220526` AS sel
        LEFT JOIN `reddit-relevance.tmp.subclu_subreddit_geo_selected_20220526` gc
            ON sel.subreddit_id = gc.subreddit_id

        WHERE
            -- select geo subs
            gc.subreddit_id IS NOT NULL

            -- select subs that are highly active
            OR (
                (
                    sel.active = TRUE
                    AND sel.users_l7 >= MIN_ACTIVE_SUB_USERS_l7
                    AND sel.posts_not_removed_l28 >= MIN_ACTIVE_SUB_POSTS_L28
                )
                OR (
                    sel.activity_7_day >= 9
                    AND sel.users_l7 >= MIN_NON_ACTIVE_SUB_USERS_l7
                    AND sel.posts_not_removed_l28 >= MIN_NON_ACTIVE_SUB_POSTS_L28
                    -- Reduce spammy accounts (with only a single user posting)
                    --  might filter these on the back-end
                    AND sel.unique_posters_l7_submitted >= 1
                )
            )
    )
    , subreddit_text_and_mature_themes AS (
        -- Mege text & apply initial regexes for clean text data
        SELECT
            slo.name
            , slo.subreddit_id

            -- Calculate the word count after the final check, if a tall
            , CASE
                WHEN CHAR_LENGTH(mature_themes_deduped_text) <= 0 THEN 0
                ELSE ARRAY_LENGTH(SPLIT(mature_themes_deduped_text, ' '))
            END AS mature_themes_word_count

            , mature_themes_deduped_text
            , slo.title     AS subreddit_title
            , slo.public_description AS subreddit_public_description
            , slo.description AS subreddit_description
            , slo.subreddits_in_descriptions

            -- Do word count for full concat column on final query
            -- Use coalesce because some fields can be null and one null can make the whole concat null
            -- Final check (below) will check whether description = public description and append only if they're different
            , (description = public_description) AS description_equals_public_desc
            , TRIM(
                CONCAT(
                    name
                    , ". r/", LOWER(name), ". ",
                    -- Only add title if diff from name
                    COALESCE(
                        IF(
                            LOWER(name) = LOWER(REGEXP_REPLACE(title, r"\b\/?r/", "")),
                            NULL,
                            CONCAT(
                                REGEXP_REPLACE(title, r"(?i)\bfap{1,2}\b|\bfap{1,2}ers\b", "fap-porn")
                                , "\n"
                            )
                        )
                        , "\n"
                    ),
                    COALESCE(
                        IF(
                            COALESCE(over_18, '') = 't',
                            "Adult content or porn. ",
                            NULL
                        )
                        , ''
                    ),
                    -- only add mature themes if there's at least one
                    COALESCE(
                        IF(
                            CHAR_LENGTH(mature_themes_deduped_text)=0,
                            NULL,
                            CONCAT(mature_themes_deduped_text, ".\n")
                        )
                        , ''
                    ),
                    COALESCE(
                        IF(
                            LOWER(name) = LOWER(subreddits_in_descriptions),
                            NULL,
                            CONCAT(subreddits_in_descriptions, "\n\n")
                        )
                        , "\n"
                    ),

                    CLEAN_DESCRIPTION_TEXT(public_description, REGEX_REMOVE_STR, REGEX_REPLACE_WITH_SPACE_STR, REGEX_REMOVE_2ND_PASS_STR)
                )
            ) AS subreddit_meta_pub_desc_only

        FROM (
            -- use subquery to extract subreddits in descriptions & re-use them
            SELECT
                *
                , ARRAY_DISTINCT_STRING(
                    ARRAY_CONCAT(
                        REGEXP_EXTRACT_ALL(COALESCE(description, ''), r"\br/\w+"),
                        REGEXP_EXTRACT_ALL(COALESCE(public_description, ''), r"\br/\w+")
                    )
                    , ', '
                 ) AS subreddits_in_descriptions

            FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`

            -- Look back 2+ days because looking back 1-day could be an empty partition
            WHERE dt = PARTITION_DATE
        ) AS slo
            -- Use distinct in case a sub qualifies for more than 1 reason
            INNER JOIN (SELECT DISTINCT subreddit_id FROM selected_subs) AS sel
                ON slo.subreddit_id = sel.subreddit_id
            LEFT JOIN (
                SELECT
                    * EXCEPT(mature_themes_deduped_raw)
                    -- For some entertainment categories, 'violence' and 'sex' can be unevenly applied, exclude it for now
                    --  Example it's always sunny in philadelphia is a TV show that might mention sex, but is tagged as "sex explicit"
                    -- For now keep "sex", but remove violence
                    , IF(
                        (
                            (COALESCE(primary_topic, '') IN ('Gaming', 'Movies', 'Anime', 'Television'))
                            AND (COALESCE(rating_short, '') != 'V')
                        )
                        , REGEXP_REPLACE(COALESCE(mature_themes_deduped_raw, ''), r"violence ?", '')
                        , COALESCE(mature_themes_deduped_raw, '')
                    ) AS mature_themes_deduped_text

                FROM(
                    SELECT
                        subreddit_id
                        , primary_topic
                        , rating_short
                        , rating_name
                        , mature_themes
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
                                , ' '  -- split all mature themes to get unique vals
                            )
                            , ' '  -- separate mature themes with space
                        ) AS mature_themes_deduped_raw
                    FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
                    WHERE pt = PARTITION_DATE
                )
            ) AS nt
                ON sel.subreddit_id = nt.subreddit_id
    )

    , subreddit_lookup_clean_text_meta AS (
        -- Apply FINAL checks for clean textdata
        SELECT
            name
            , subreddit_id
            , mature_themes_deduped_text
            , mature_themes_word_count
            , subreddit_title
            , subreddit_public_description
            , subreddits_in_descriptions
            , description_equals_public_desc
            , subreddit_description

            -- Do word count for full concat column on final query
            -- Use coalesce because some fields can be null and one null can make the whole concat null
            -- Only append description (with markdown) if different from public_description
            , CASE
                WHEN description_equals_public_desc THEN subreddit_meta_pub_desc_only
                ELSE TRIM(
                    CONCAT(
                        subreddit_meta_pub_desc_only
                        , "\n\n",
                        CLEAN_DESCRIPTION_TEXT(subreddit_description, REGEX_REMOVE_STR, REGEX_REPLACE_WITH_SPACE_STR, REGEX_REMOVE_2ND_PASS_STR)
                    )
                )
            END AS subreddit_meta_for_embeddings

        FROM subreddit_text_and_mature_themes AS slo1
    )
    , final_table AS (
        SELECT
            sel.*

            -- Text from lookup
            , slo.subreddit_title
            , slo.subreddit_public_description
            , slo.subreddits_in_descriptions
            , LENGTH(slo.subreddit_meta_for_embeddings) AS subreddit_meta_for_embeddings_len
            , array_length(
                regexp_extract_all(subreddit_meta_for_embeddings, r"\b\w+\b")
            ) AS subreddit_meta_for_embeddings_word_count
            , description_equals_public_desc
            , slo.mature_themes_word_count
            , mature_themes_deduped_text
            , slo.subreddit_description
            , slo.subreddit_meta_for_embeddings

        -- Use distinct in case a sub qualifies for more than 1 reason
        FROM (SELECT DISTINCT * FROM selected_subs) AS sel

            LEFT JOIN subreddit_lookup_clean_text_meta AS slo
                ON sel.subreddit_id = slo.subreddit_id
    )

    -- SELECT to test text creation
    SELECT
        -- *

        slo.subreddit_title
        , slo.subreddits_in_descriptions
        , slo.mature_themes_word_count
        , rating_short
        , primary_topic
        , over_18
        , subreddit_name
        , slo.subreddit_meta_for_embeddings
        , slo.subreddit_description
        , slo.subreddit_public_description

    FROM final_table AS slo

    WHERE 1=1
        -- AND (
        --     subreddit_name LIKE "%bolly%"
        --     OR subreddit_name LIKE "%desi%"
        -- )
        AND (
                subreddit_name IN (
                "redditsessions", "blursedimages", "hmmm", "hmm", "bollywood", "bollyblindsngossip", "bollyarm", "bollywoodmemes", "twitter", "eyebleach", "makenewfriendshere", "meetpeople", "berlinsocialclub", "nycmeetups", "news", "antiwork", "damnthatsinteresting", "publicfreakout", "lifeprotips", "jedi"
                , "jamesbond", "clonewars", "archerfx", "loveislandtv", "residentevil", "mortalkombat", "ukrainewarvideoreport", "sfx", "formula1", "gonewild", "minecraft", "china_irl", "lebanon", "hottiesoftvandyt", "mdma", "de", "mexico", "france", "rance_iel", "relationship_advice", "gonewildstories", "sex", "nsfwiama", "lgbt", "askgaybros", "asexuality", "me_irlgbt"
                , "blackladies", "asianamerican", "mixedrace", "askreddit", "nostupidquestions", "yahooqr", "tinder", "ama", "wallstreetbets", "cryptocurrency", "soccer", "nba", "nfl", "fifa", "sports", "baseball", "fuckcars", "texas", "canada", "australia", "worldnews", "ukraine", "europe"
                , "todayilearned", "india", "conspiracy", "space", "nasa", "explainlikeimfive", "eldenring", "apexlegends", "hiphopheads", "music", "listentothis", "halo", "marvelstudios", "starwars", "movies", "deuxmoi", "aww", "interestingasfuck"
                , "anime", "genshin_impact", "sweden", "denmark", "romania", "ufc", "mma", "kpop", "kpopde", "freekarma4u", "fitness", "trees", "cocktails", "vegan", "cooking", "food", "amitheasshole", "showerthoughts", "memexico", "mujico"
            )
            OR rating_short IN ('V')
            OR primary_topic IN (
                'Movies'
                ,'Television'
                -- , 'Anime'
                -- , 'Podcasts and Streamers'
            )
        )
        AND mature_themes_word_count >= 1

    -- Order by rating + topic + over_18 to understand which subs we'd exclude if we filter things out
    ORDER BY over_18, rating_short, primary_topic, users_l7 DESC
    -- ORDER BY subreddit_name
    -- ORDER BY users_l7 DESC, posts_not_removed_l28 DESC

    LIMIT 2000

    -- SELECT for TABLE CREATION (or table preview)
    -- SELECT *
    -- FROM final_table
    -- ORDER BY users_l7 DESC, posts_not_removed_l28 DESC
-- );  -- Close CREATE TABLE parens
