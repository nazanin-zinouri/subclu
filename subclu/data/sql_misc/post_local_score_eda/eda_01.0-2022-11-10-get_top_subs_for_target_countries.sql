-- Get subreddits to use as baselines for post-level localness
-- Two types of subs:
--   - HIGHLY local subs (even higher thresholds than strict) -> expect 90%+ of posts to be local
--   - Subs with lots of visits from country, but NOT local -> e.g., r/askreddit will have visits from most countries, but 90%+ of posts should NOT be local

WITH subs_highly_local AS (
    SELECT
      subreddit_id
      , geo_country_code
      , subreddit_name
      , CASE
          WHEN (sub_dau_perc_l28 >= 0.6) THEN 'highly_local'
          WHEN (sub_dau_perc_l28 < 0.05) THEN 'not_local'
          ELSE NULL
      END AS localness_higher_threshold
      , ROW_NUMBER() OVER(PARTITION BY geo_country_code ORDER BY sub_dau_l28 DESC) AS country_rank
      , * EXCEPT(
        pt, subreddit_id, geo_country_code, subreddit_name
        , sub_dau_l1, sub_dau_perc_l1
        , is_removed, is_spam
      )

    FROM `data-prod-165221.i18n.community_local_scores`
    WHERE DATE(pt) = "2022-11-14"
      AND geo_country_code IN (
        'DE', "FR", 'MX', "BR"
        , "CA", 'GB', 'IN', 'AU'
      )
      AND COALESCE(nsfw, FALSE) = FALSE

    QUALIFY country_rank <= 25
        AND activity_7_day >= 9
        -- Only keep subs that should be clearly local or not local
        --  add some exceptions for subs with diaspora issues (e.g., r/India only has 49% of visits inside of India)
        AND (
            localness_higher_threshold IS NOT NULL
            OR subreddit_name IN (
                'australia'
                , 'canada'
                , 'askuk', 'casualuk', 'london', 'unitedkingdom'
                , 'india', 'indiaspeaks', 'cricket', 'bangalore'
                , 'mexicocity', 'preguntaleareddit'
            )
        )
    ORDER BY geo_country_code, country_rank
)

SELECT *
FROM subs_highly_local
;
