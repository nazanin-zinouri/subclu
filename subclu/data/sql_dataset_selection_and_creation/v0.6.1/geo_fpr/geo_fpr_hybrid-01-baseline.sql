-- Create FRP topic model output for a target countries
--   This is best thought of as a hybrid model because it combines
--   both text similarity & behavior similarity

DECLARE PT_DATE DATE DEFAULT (CURRENT_DATE() - 2);

-- Values for CAU embeddings/similarity model
DECLARE CA_ANN_PT_DATE DATE DEFAULT "2022-11-22";
DECLARE CA_MODEL_VERSION STRING DEFAULT "v0.6.1";

-- TODO(djb): Values for ML/behavior similarity (use these to limit recommendations)

-- Values to select local subreddits
DECLARE LOCALNESS_DEFAULT DEFAULT [
    "strict"
    , "loose"
];
-- TODO(djb): can we use one query (w/o loops) to get all 2 queries:
--   1) non-English countries (with lower threshold for localness)
--   2) English speaking countries (with higher thresholds for localness)
DECLARE TARGET_COUNTRY_GEO_CODES DEFAULT [
    "MX"
    , "DE"
    -- , "US"
    , "CA"
    , "GB"
    , "IE"
    , "IN"
    , "GR", "TR"
];

-- TODO(djb): calculate combined score for localness merging standardized + absolute scores
--  e.g., subs like r/cricket might not be captured by "loose" definition for UK & Australia

-- WITH cau_recs_raw AS (

-- )
SELECT
    ls.geo_country_code
    , ls.localness
    , ann.subreddit_id
    , ann.subreddit_name
    -- , ann.similar_subreddit

    -- Transform rec_subreddits back to struct
    , ARRAY_AGG(
        STRUCT(
            COALESCE(rec.subreddit_id, b_ann.subreddit_id) AS subreddit_id
            , COALESCE(rec.subreddit_name, b_ann.subreddit_name) AS subreddit_name
            , rec.cosine_similarity
            , rec.distance_rank
            , b_ann.behavior_rank
            , b_ann.score
            , lr.geo_country_code
            , lr.localness
        )
        ORDER BY rec.distance_rank, b_ann.behavior_rank
    ) AS similar_subreddit
FROM `reddit-employee-datasets.david_bermejo.cau_similar_subreddits_by_text` AS ann
    -- Expand ANN so we can create filters for recommended subs
    LEFT JOIN UNNEST(similar_subreddit) AS rec

    -- Keep only SEED subs local to target country
    INNER JOIN (
        SELECT *
        FROM `data-prod-165221.i18n.community_local_scores`
        WHERE DATE(pt) = PT_DATE
            AND geo_country_code IN UNNEST(TARGET_COUNTRY_GEO_CODES)
            AND localness IN UNNEST(LOCALNESS_DEFAULT)
            -- TODO(djb): add custom localness metric for borderline subs(?)
            -- How many more subs do we gain with this borderline metric? are they any good/worth it?
    ) AS ls
        ON ann.subreddit_id = ls.subreddit_id

    -- Keep only RECOMMENDED subs local to target country
    INNER JOIN (
        SELECT
            *
        FROM `data-prod-165221.i18n.community_local_scores`
        WHERE DATE(pt) = PT_DATE
            AND geo_country_code IN UNNEST(TARGET_COUNTRY_GEO_CODES)
            AND localness IN UNNEST(LOCALNESS_DEFAULT)
            -- TODO(djb): add custom localness metric for borderline subs(?)
            -- How many more subs do we gain with this borderline metric? are they any good/worth it?
    ) AS lr
        ON rec.subreddit_id = lr.subreddit_id

    -- Add behavior models distance to limit recommending OPPOSITE subreddits (e.g., r/antivegan to r/vegan)
    LEFT JOIN (
        SELECT
            s.subreddit_id AS subreddit_id_seed
            , s.subreddit_name AS subreddit_name_seed
            , ROW_NUMBER() OVER(
                PARTITION BY s.subreddit_id
                ORDER BY score DESC
            ) AS behavior_rank
            , n.*
        FROM `data-prod-165221.ml_content.similar_subreddit_ft2` AS s
            -- We need to UNNEST & join the field with nested JSON
            LEFT JOIN UNNEST(similar_subreddit) AS n
        WHERE pt = "2022-12-06"
    ) AS b_ann
        ON ann.subreddit_id = b_ann.subreddit_id_seed AND rec.subreddit_id = b_ann.subreddit_id
    -- TODO(djb): From RECOMMENDED subs - remove subs with "do_not_recommend" flag
    -- TODO(djb): keep only subs in clustering model

WHERE
    ann.pt = CA_ANN_PT_DATE
    AND ann.model_version = CA_MODEL_VERSION

    -- TODO(djb): keep only where SEED & REC country is the same
    AND ls.geo_country_code = lr.geo_country_code

    -- TODO(djb): Make sure country for SEED & REC are the same
    -- TODO(djb): From RECOMMENDED subs - remove subs with "do_not_recommend" flag
    -- TODO(djb): keep only subs in clustering model

    -- TODO(djb): Instead of relying so much on scores, might be better to invest now
    --  on a list of DO NOT RECOMMEND pairs. Otherwise we limit the potential subs we can recommend a lot
    --   for example: r/greece <> r/turkey
    AND (
        -- Case 1: Sub-Pair ONLY scored by CAU
        --  Keep only if similarity is EXTREMELY high
        (
            -- EXAMPLE: carnivore <> vegetarian text similarity ~0.827
            rec.cosine_similarity >= 0.80
            AND b_ann.score IS NULL
        )

        -- Case 2a: Sub-Pair scored by both CAU & ML
        --  High Text, Low Behavior
        OR (
            -- Limit by Text Similarity
            --   There's no guarantee that the N most similar subs are related, so combine rank & distance
            (
                rec.distance_rank <= 1
                OR rec.cosine_similarity >= 0.73 -- ~ 0.75 seems to be ok. Maybe lower it when we AlSO apply the behavior filters
            )
            -- Limit by Behavior SIMILARITY ~0.775, 0.75
            -- Examples:
            --  - antivegan <> vegan = 0.7689
            --  - carnivore <> plantbasedDiet = 0.771
            AND b_ann.score >= 0.62
        )
        -- Case 2b: Sub-Pair scored by both CAU & ML. This gets into hybrid territory
        --  Low Text, High Behavior
        OR (
            -- Limit by Text Similarity
            (
                rec.distance_rank <= 1
                OR rec.cosine_similarity >= 0.7
            )
            -- Limit by Behavior SIMILARITY
            AND b_ann.score >= 0.84
        )
    )

    -- Testing to limit scope of seed subs
    AND ann.subreddit_name IN (
        -- Subs to test opposites & 'ireland' (failure from prev output)
        'vegande', 'carnivore', 'antivegan', 'vegetarianketo', 'ireland'
        -- , 'de', 'ich_iel', 'bundesliga', 'fussball', 'mauerstrassenwetten', 'fragreddit'
        -- , 'bayer04', 'herthabsc'

        -- , 'mexico', 'memexico', 'askmexico'
        , 'ligamx', 'rayados'
        -- , 'mezcal', 'tequila', 'mexicanfood', 'memesenespanol'
        , 'latinopeopletwitter', 'narco'
        -- , 'cancun', 'cdmx', 'mexicocity', 'monterrey'

        -- , 'formula1', 'cricket'
        -- , 'greece', 'turkey'
    )

GROUP BY 1, 2, 3, 4
ORDER BY geo_country_code, subreddit_name
;
