-- get total upvotes, downvotes & net_upvotes for a post
-- For some reason, the `post_lookup` table doesn't do a running count, it only does a snapshot of
--  upvotes in a partition
DECLARE start_date DATE DEFAULT '2021-08-01';
DECLARE end_date DATE DEFAULT '2021-09-21';


-- this one results in double counting net-upvotes
-- SELECT
--     *
--     , (upvotes - downvotes) AS net_upvotes
-- FROM (
--     SELECT
--         subreddit_id
--         , post_id
--         , SUM(upvotes)      AS upvotes
--         , SUM(downvotes)    AS downvotes

--     FROM `data-prod-165221.ds_v2_postgres_tables.post_lookup` AS plo
--     WHERE DATE(_PARTITIONTIME) BETWEEN start_date AND end_date
--         AND post_id IN(
--             't3_p6eqom', 't3_pfs2fl', 't3_p8gpwl',
--             't3_payw1w'
--         )

--     GROUP BY 1, 2
-- )
-- ;
-- Results
-- Row	subreddit_id	post_id	 upvotes 	 downvotes 	 net_upvotes
-- 1	t5_2qh03	t3_p8gpwl	 432,125 	 82,734 	 349,391  (should be ~116K net upvotes)
-- 2	t5_2qh03	t3_p6eqom	 488,222 	 81,228 	 406,994
-- 3	t5_2qh03	t3_payw1w	 323,665 	 29,355 	 294,310  (should be ~66K net upvotes)
-- 4	t5_2qh03	t3_pfs2fl	 279,921 	 57,841 	 222,080


-- count only latest values
-- this one is missing  A LOT of votes... so we'll go back to `successful_post`
--  table for upvotes
SELECT
    subreddit_id
    , post_id
    , upvotes
    , downvotes
    , (upvotes - downvotes) AS net_upvotes
    , plo.*

FROM `data-prod-165221.ds_v2_postgres_tables.post_lookup` AS plo
WHERE DATE(_PARTITIONTIME) BETWEEN (CURRENT_DATE() - 3) AND (CURRENT_DATE() - 2)
    AND post_id IN(
        't3_p6eqom', 't3_pfs2fl', 't3_p8gpwl',
        't3_payw1w'
    )
ORDER BY plo.post_id ASC
;
