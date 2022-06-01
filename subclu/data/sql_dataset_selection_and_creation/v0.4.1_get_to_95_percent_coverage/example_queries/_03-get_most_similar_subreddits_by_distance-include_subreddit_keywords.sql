-- Input a subreddit and get the most similar subreddits + their cluster topics & keywords
DECLARE PARTITION_DATE DATE DEFAULT (CURRENT_DATE() - 2);
DECLARE INPUT_SUBREDDIT_NAME STRING DEFAULT 'depression';

DECLARE N_CLOSEST_SUBREDDITS NUMERIC DEFAULT 25;

-- Top words to keep per subreddit. The 2 algos sometimes differ slightly
--  so it's good to sample from both
DECLARE MAX_NGRAM_RANK_TFIDF NUMERIC DEFAULT 8;
DECLARE MAX_NGRAM_RANK_BM25 NUMERIC DEFAULT 8;


WITH
closest_subreddits AS (
    -- Only get the distances for subs relative to input subreddit
    SELECT
        subreddit_id_a
        , subreddit_id_b
        , cosine_similarity
    FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_distances_c_top_100`
    WHERE
        subreddit_name_a = INPUT_SUBREDDIT_NAME
        AND distance_rank <= N_CLOSEST_SUBREDDITS
)
, top_keywords_per_sub AS (
    SELECT
      subreddit_id
      , STRING_AGG(ngram, ', ') AS subreddit_post_and_flair_top_keywords
    FROM `reddit-employee-datasets.david_bermejo.subreddit_top_tfidf_bm25_20211215` AS bm
    -- Keep only top keywords per subreddit
    WHERE 1=1
        AND (
          ngram_rank_bm25 <= MAX_NGRAM_RANK_BM25
          OR ngram_rank_tfidf <= MAX_NGRAM_RANK_TFIDF
      )
    GROUP BY 1
)


SELECT
    sc.subreddit_id
    , nt.rating_name
    , CASE WHEN
        sc.subreddit_name = INPUT_SUBREDDIT_NAME THEN 1.0
        ELSE dis.cosine_similarity
    END AS cosine_similarity_to_input
    , nt.primary_topic
    , sc.subreddit_name
    , asr.users_l7
    , asr.seo_users_l7_pct_of_total
    , subreddit_post_and_flair_top_keywords
    , tm.k_0100_label_name AS cluster_name
    , tm.k_0400_label_name AS cluster_subtopic

    -- check labels at different tiers
    -- , k_1000_label
    -- , k_4000_label

FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full` AS sc
    -- Get subreddit distances
    LEFT JOIN closest_subreddits AS dis
        ON sc.subreddit_id = dis.subreddit_id_b

    LEFT JOIN (
        SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
        WHERE pt = PARTITION_DATE
    ) AS nt
        ON sc.subreddit_id = nt.subreddit_id
    -- get subreddit activity
    LEFT JOIN (
        SELECT
            subreddit_id
            , users_l7
            , seo_users_l7_pct_of_total
        FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_activity`
    ) AS asr
        ON sc.subreddit_id = asr.subreddit_id

    -- Join manual topic model labels
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_manual_names` AS tm
        ON sc.k_0400_label = tm.k_0400_label

    -- Join to get top keywords per subreddit
    LEFT JOIN top_keywords_per_sub AS bm
        ON sc.subreddit_id = bm.subreddit_id

WHERE 1=1
    -- Keep only nearest subreddits and input subreddit
    AND (
        sc.subreddit_name = INPUT_SUBREDDIT_NAME
        OR dis.subreddit_id_b IS NOT NULL
    )

ORDER BY cosine_similarity_to_input DESC, subreddit_name
;
