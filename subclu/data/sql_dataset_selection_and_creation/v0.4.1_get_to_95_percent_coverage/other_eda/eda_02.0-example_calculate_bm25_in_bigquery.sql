-- source/reference:
--  https://github.snooguts.net/reddit/relevance-airflow-dags/pull/327/files
--  - dags/sql/key_phrase_relevancy.sql

WITH
  key_phrases AS (
    SELECT
      key_phrase,
      post_id,
      subreddit_name
    FROM `reddit-relevance.nlp.comment_key_phrases`
    WHERE
      pt = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 1 DAY), DAY)
    UNION ALL SELECT
      gram AS key_phrase,
      post_id,
      subreddit_name
    FROM `reddit-relevance.nlp.comment_n_grams`
    WHERE
      pt = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 1 DAY), DAY)
    UNION ALL SELECT
      key_phrase,
      post_id,
      subreddit_name
    FROM `reddit-relevance.nlp.post_key_phrases`
    WHERE
      pt = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 1 DAY), DAY)
    UNION ALL SELECT
      gram AS key_phrase,
      post_id,
      subreddit_name
    FROM `reddit-relevance.nlp.post_n_grams`
    WHERE
      pt = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 1 DAY), DAY)

  ),

  key_phrase_counts AS (
    SELECT
      key_phrase,
      COUNT(*) AS ct
    FROM
      key_phrases
    GROUP BY key_phrase
  ),

  key_phrases_per_sub AS (
    SELECT
      subreddit_name,
      COUNT(DISTINCT key_phrase) AS ct
    FROM
      key_phrases
    GROUP BY
      subreddit_name
  ),

  avg_key_phrases_in_sub AS (
    SELECT
      AVG(ct) AS avg
    FROM
      key_phrases_per_sub
  ),

  num_subs AS (
    SELECT
      COUNT(DISTINCT subreddit_name) AS ct
    FROM
      key_phrases
  ),

  subs_with_key_phrase AS (
    SELECT
      key_phrase,
      COUNT(DISTINCT subreddit_name) as ct
    FROM
      key_phrases
    GROUP BY
      key_phrase
  ),

  key_phrase_in_sub AS (
    SELECT
      key_phrase,
      subreddit_name,
      COUNT(*) AS ct
    FROM
      key_phrases
    GROUP BY
      key_phrase,
      subreddit_name
  ),

  bm25_subreddit_key_phrases AS (
    SELECT
      key_phrase_in_sub.key_phrase AS key_phrase,
      key_phrase_counts.ct AS key_phrase_counts_ct,
      key_phrase_in_sub.subreddit_name AS subreddit_name,
      key_phrase_in_sub.ct AS key_phrase_in_sub_ct,
      subs_with_key_phrase.ct AS subs_with_key_phrase_ct,
      key_phrases_per_sub.ct AS key_phrases_per_sub_ct,
      LN(1.0 + ((SELECT ct FROM num_subs) - subs_with_key_phrase.ct + 0.5) / (subs_with_key_phrase.ct + 0.5) ) * (key_phrase_in_sub.ct * 1.2) / (key_phrase_in_sub.ct + 1.2 + (1.0 - 0.75 + 0.75 * (key_phrases_per_sub.ct/(SELECT avg FROM avg_key_phrases_in_sub)) ) ) AS bm25
    FROM
      key_phrase_in_sub
    LEFT JOIN
      subs_with_key_phrase
    ON
      key_phrase_in_sub.key_phrase = subs_with_key_phrase.key_phrase
    LEFT JOIN
      key_phrases_per_sub
    ON
      key_phrase_in_sub.subreddit_name = key_phrases_per_sub.subreddit_name
    LEFT JOIN
      key_phrase_counts
    ON
      key_phrase_in_sub.key_phrase = key_phrase_counts.key_phrase
  )

SELECT
  key_phrase,
  subreddit_name,
  bm25,
  ROW_NUMBER() OVER(PARTITION BY subreddit_name ORDER BY bm25 DESC) AS rank,
  key_phrase_counts_ct,
  key_phrase_in_sub_ct,
  subs_with_key_phrase_ct,
  key_phrases_per_sub_ct,
  CURRENT_TIMESTAMP AS job_timestamp,
  TIMESTAMP_TRUNC(CURRENT_TIMESTAMP, DAY) AS pt
FROM
  bm25_subreddit_key_phrases
