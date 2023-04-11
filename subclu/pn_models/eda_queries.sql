-- Get test users for end-to-end embeddings
CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.pn_test_users_for_embedding` AS (
SELECT
    DISTINCT user_id

FROM `data-prod-165221.fact_tables.post_consume_post_detail_view_events`
WHERE DATE(pt) BETWEEN (CURRENT_DATE() - 8) AND (CURRENT_DATE() - 2)
    AND subreddit_name IN (
        'themandaloriantv', 'de', 'france'
    )
    AND action IN ('consume', 'view')
);
