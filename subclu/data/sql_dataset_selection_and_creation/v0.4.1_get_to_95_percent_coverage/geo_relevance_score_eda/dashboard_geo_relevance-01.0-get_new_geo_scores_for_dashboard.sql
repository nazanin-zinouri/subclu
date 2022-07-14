-- Get BOTH: old geo-relevance AND new geo-relevance (cultural relevance)
--  And add latest rating & over_18 flags to get best estimate of SFW subs for clustering

-- Set minimum thresholds for scores: b & e
--  These thresholds are lower than the final definition, but use them to check what it would take
--  to make some subs relevant to some countries
DECLARE B_MIN_USERS_PCT_BY_SUB DEFAULT 0.10;
DECLARE E_MIN_USERS_PCT_BY_COUNTRY_STANDARDIZED DEFAULT 0.7;
DECLARE TOP_N_RANK_SUBS_PER_COUNTRY DEFAULT 10;  -- include top subs even if they don't qualify under a relevance metric

WITH
  relevance_default_tier_2 AS (
    -- we need an additional query to get tier 2 countries b/c they weren't included in the original
    -- default geo-relevance query
    SELECT
        subreddit_id
        , subreddit_name
        , geo_country_code
        , country_name
        , geo_relevance_default

    FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_score_default_daily_20220222`
    WHERE 1=1
        AND geo_country_code IN (
            'JP','KR','PH','NL','TR','RO','DK','SE','FI','PL','ID','RU'
        )
  ),
  relevance_merge_t1_and_t2 AS (
    -- Add geo-relevant subs for tier 2 countries in a single table to make additional joins easier
    SELECT
        COALESCE(s.subreddit_id, t2.subreddit_id) AS subreddit_id
        , COALESCE(s.geo_country_code, t2.geo_country_code) AS geo_country_code
        , COALESCE(s.subreddit_name, t2.subreddit_name) AS subreddit_name
        , COALESCE(s.country_name, t2.country_name) AS country_name
        , CASE
            -- if it's a country in the t2 list, coalesce with t2 list first
            WHEN
                t2.geo_country_code IN (
                    'JP','KR','PH','NL','TR','RO','DK','SE','FI','PL','ID','RU'
                )
                THEN COALESCE(t2.geo_relevance_default, s.geo_relevance_default)
            -- for everything else, pick t1 value first
            ELSE COALESCE(s.geo_relevance_default, t2.geo_relevance_default)
            END
            AS geo_relevance_default

        , s.* EXCEPT(subreddit_id,  subreddit_name, country_name, geo_relevance_default, geo_country_code)

    FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_score_standardized_20220212` AS s
        -- Add geo-relevant subs for tier 2 countries
        FULL OUTER JOIN relevance_default_tier_2 AS t2
          ON s.subreddit_id = t2.subreddit_id AND s.geo_country_code = t2.geo_country_code
  ),
  relevance_base AS (
    SELECT
        nt.rating_name
        , nt.primary_topic
        , nt.rating_short
        , slo.over_18
        , CASE
            WHEN(COALESCE(slo.over_18, 'f') = 't') THEN 'X_or_over_18'
            WHEN(COALESCE(nt.rating_short, '') IN ('X')) THEN 'X_or_over_18'
            ELSE 'unrated_or_E_M_D_V'
        END AS grouped_rating
        , CASE
            WHEN(COALESCE(tm.subreddit_id, '') != '') THEN 'subreddit_in_model'
            ELSE 'subreddit_missing'
        END AS subreddit_in_v041_model
        , s.* EXCEPT(over_18, pt, verdict, users_percent_by_country_stdev, type)

    FROM relevance_merge_t1_and_t2 AS s
        -- Add rating so we can get an estimate for how many we can actually use for recommendation
        LEFT JOIN (
            SELECT *
            FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
            WHERE dt = DATE(CURRENT_DATE() - 2)
        ) AS slo
        ON s.subreddit_id = slo.subreddit_id
        LEFT JOIN (
            SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
            WHERE pt = (CURRENT_DATE() - 2)
        ) AS nt
            ON s.subreddit_id = nt.subreddit_id

        -- Exclude popular US subreddits
        -- Can't query this table from local notebook because of errors getting google drive permissions. smh, excludefor now
        -- LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_us_to_exclude_from_relevance` tus
        --     ON s.subreddit_name = LOWER(tus.subreddit_name)

        -- Add latest table for v0.4.1 model so we can count which ones are missing
        LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` tm
            ON s.subreddit_id = tm.subreddit_id

    WHERE 1=1
        AND s.subreddit_name != 'profile'
        AND COALESCE(slo.type, '') = 'public'
        AND COALESCE(slo.verdict, 'f') <> 'admin-removed'

    ),
    subs_and_countries_above_thresh AS (
        -- Select only country+subs that make the cut by threshold AND target countries
        SELECT *
        FROM relevance_base
        WHERE 1=1
          AND (
              geo_relevance_default = TRUE
              OR b_users_percent_by_subreddit >= B_MIN_USERS_PCT_BY_SUB
              OR e_users_percent_by_country_standardized >= E_MIN_USERS_PCT_BY_COUNTRY_STANDARDIZED
          )
          AND (
              -- tier 0
              geo_country_code IN ('GB','AU','CA')

              -- tier 1
              OR geo_country_code IN ('DE','FR','BR','MX','IN')

              -- tier 2
              OR geo_country_code IN ('IT','ES','JP','KR','PH','NL','TR','RO','DK','SE','FI','PL','ID','RU')

              -- Additional countries, PT=Portugal, AR=Argentina
              OR country_name IN (
                    'Portugal', 'Argentina'
              )

          )
    ),
    subs_broad_relevance AS (
        -- Select subreddits that we expect to be widely relevant, even if not in a target country
        --   These can help us adjust thresholds or understand why a country doesn't qualify
            SELECT *
            FROM relevance_base
            WHERE 1=1
              AND (
                  geo_relevance_default = TRUE
                  OR b_users_percent_by_subreddit >= B_MIN_USERS_PCT_BY_SUB
                  OR e_users_percent_by_country_standardized >= 0.5  -- Pull any country slightly above average
              )
              AND subreddit_name IN (
                  'cricket',
                  'soccer', 'futbol',
                  'formula1',
                  'rugbyunion',
                  'rugbyaustralia',
                  'bundesliga',
                  'premierleague',
                  'reddevils',
                  'liverpoolfc',
                  'ligue1',
                  'laliga',
                  'barca',
                  'ligamx',
                  'seriea'
              )
    ),
    top_subs_in_target_countries AS (
        -- Select country+subs that are the top ranked in target countries, even if not relevant
        SELECT *
        FROM relevance_base
        WHERE 1=1
            AND (d_users_percent_by_country_rank <= TOP_N_RANK_SUBS_PER_COUNTRY)
            AND country_name IN (
                    'Germany', 'Austria', 'India', 'France', 'Brazil','Italy',
                    'Spain', 'Mexico',
                    -- 'Argentina', 'Portugal',
                    'United Kingdom', 'Canada', 'Australia'
                  )

            -- AND (
            --     -- Exclude subs that are top in US but we want to exclude as culturally relevant
            --     --  For simplicity, let's go with the English exclusion (more relaxed) than the non-English one
            --     COALESCE(tus.english_exclude_from_relevance, '') <> 'exclude'
            -- )
    )


SELECT
    DISTINCT *
FROM (
    SELECT * FROM subs_and_countries_above_thresh
    UNION ALL
    SELECT * FROM subs_broad_relevance
    UNION ALL
    SELECT * FROM top_subs_in_target_countries
)

ORDER BY users_l7 DESC, subreddit_name, c_users_percent_by_country DESC
