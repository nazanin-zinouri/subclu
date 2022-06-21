-- Use Eugene's table to get NSFW suspects + ngrams

-- Top words to return
DECLARE MAX_NGRAM_RANK_TFIDF NUMERIC DEFAULT 9;
DECLARE MAX_NGRAM_RANK_BM25 NUMERIC DEFAULT 9;

WITH sub_top_keywords AS (
    SELECT
        subreddit_id
        , STRING_AGG(ngram, ', ') AS top_keywords_in_sub_posts
    FROM `reddit-employee-datasets.david_bermejo.subreddit_top_tfidf_bm25_20211215`
    WHERE 1=1
        AND (
            ngram_rank_bm25 <= MAX_NGRAM_RANK_BM25
            OR ngram_rank_tfidf <= MAX_NGRAM_RANK_TFIDF
        )
    GROUP BY 1
)

SELECT
    c.users_l7
    , c.rating_short
    , c.rating_name
    , c.over_18
    , c.subreddit_name
    , c.primary_topic
    , i18n_cluster_name
    , i18n_cluster_subtopic
    , top_keywords_in_sub_posts
    , top_subreddits_in_subtopic
    , top_keywords_in_subtopic

FROM `reddit-employee-datasets.eugene_hwang.potential_unrated_nsfw` AS c
    LEFT JOIN sub_top_keywords AS tf
        ON c.subreddit_id = tf.subreddit_id

ORDER BY users_l7 DESC

;
