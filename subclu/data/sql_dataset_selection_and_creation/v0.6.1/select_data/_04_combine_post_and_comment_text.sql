-- Create new table with combined text from post + comments
--  Hypothesis: combining text might improve post-2-post embeddings/recommendations

DECLARE MAX_COMMENT_LEN NUMERIC DEFAULT 200;
-- Only save post+comment text longer than this variable for embeddings:
DECLARE MIN_POST_AND_COMMENT_TEXT_LEN_FOR_EMBEDDING DEFAULT 3;

CREATE OR REPLACE TABLE `reddit-relevance.${dataset}.subclu_post_and_comment_text_combined_${run_id}`
AS (
WITH
comments_agg AS (
    SELECT
        post_id
        , COUNT(DISTINCT comment_id) AS comment_for_embedding_count
        , STRING_AGG(LEFT(comment_text_clean, SAFE_CAST(MAX_COMMENT_LEN AS INT64)), "\n" ORDER BY comment_rank_by_post_id ASC) AS comments_text_clean_agg
    FROM `reddit-relevance.${dataset}.subclu_comments_for_modeling_${run_id}`
    GROUP BY 1
)
, combined_text AS (
    SELECT
        * EXCEPT(post_and_comment_text_clean)
        , CHAR_LENGTH(post_and_comment_text_clean) AS post_and_comment_text_clean_len
        , post_and_comment_text_clean
    FROM (
        SELECT
            subreddit_id
            , p.post_id
            , net_upvotes_lookup
            , upvotes_lookup
            , subreddit_name
            , comment_for_embedding_count
            , CASE
                WHEN COALESCE(CHAR_LENGTH(comments_text_clean_agg), 0) >= 1
                    THEN CONCAT(p.post_text_for_embeddings, "\n\n", COALESCE(comments_text_clean_agg, ''))
                ELSE p.post_text_for_embeddings
            END AS post_and_comment_text_clean
        FROM `reddit-relevance.${dataset}.subclu_posts_for_modeling_${run_id}` AS p
            LEFT JOIN comments_agg AS c
                ON p.post_id = c.post_id
    )
)


SELECT
    subreddit_id
    , subreddit_name
    , post_id
    , net_upvotes_lookup
    , comment_for_embedding_count
    , post_and_comment_text_clean_len
    , post_and_comment_text_clean
FROM combined_text ct

WHERE 1=1
    AND COALESCE(post_and_comment_text_clean_len, 0) >= MIN_POST_AND_COMMENT_TEXT_LEN_FOR_EMBEDDING

ORDER BY subreddit_name, net_upvotes_lookup DESC
);  -- close create table parens
