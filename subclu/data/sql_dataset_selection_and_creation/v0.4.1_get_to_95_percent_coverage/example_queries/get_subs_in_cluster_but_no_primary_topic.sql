-- Find subs that have a label in my clusters, but no primary topic
DECLARE PARTITION_DATE DATE DEFAULT (CURRENT_DATE() - 2);

SELECT
    s.subreddit_id
    , LOWER(name) AS subreddit_name
    , users_l7
    , nt.primary_topic
    , nt.rating_name
    , tm.k_0100_label_name AS cluster_name
    , tm.k_0400_label_name AS cluster_subtopic

    -- , users_l14
    -- , users_l28

FROM (
    SELECT *
    FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
    WHERE dt = PARTITION_DATE
) AS s
    -- JOIN topic model IDs
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full` AS ti
        ON s.subreddit_id = ti.subreddit_id
    -- Join manual topic model labels
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_manual_names` AS tm
        ON ti.k_0400_label = tm.k_0400_label
    LEFT JOIN (
        SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
        WHERE DATE(pt) = PARTITION_DATE
    ) AS asr
        ON LOWER(s.name) = asr.subreddit_name

    LEFT JOIN (
        SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
        WHERE pt = PARTITION_DATE
    ) AS nt
        ON s.subreddit_id = nt.subreddit_id

WHERE 1=1
    -- Exclude user profiles. These are not rated
    AND NOT REGEXP_CONTAINS(LOWER(s.name), r'^u_.*')

    AND users_l7 >= 2000
    AND (
        nt.primary_topic IS NULL
        AND ti.subreddit_id IS NOT NULL
    )

ORDER BY cluster_name, cluster_subtopic, users_l7 DESC
;


-- Sample specific subreddits for primary topic v. cluster name
DECLARE PARTITION_DATE DATE DEFAULT (CURRENT_DATE() - 2);

SELECT
    s.subreddit_id
    , nt.rating_name
    , users_l7
    , COALESCE(tm.k_0100_label_name, nt.primary_topic) AS cluster_name_or_primary_topic
    , LOWER(name) AS subreddit_name
    , nt.primary_topic
    , tm.k_0100_label_name AS cluster_name
    , tm.k_0400_label_name AS cluster_subtopic

    -- , users_l14
    -- , users_l28

FROM (
    SELECT *
    FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
    WHERE dt = PARTITION_DATE
) AS s
    -- JOIN topic model IDs
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full` AS ti
        ON s.subreddit_id = ti.subreddit_id
    -- Join manual topic model labels
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_manual_names` AS tm
        ON ti.k_0400_label = tm.k_0400_label
    LEFT JOIN (
        SELECT * FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
        WHERE DATE(pt) = PARTITION_DATE
    ) AS asr
        ON LOWER(s.name) = asr.subreddit_name

    LEFT JOIN (
        SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
        WHERE pt = PARTITION_DATE
    ) AS nt
        ON s.subreddit_id = nt.subreddit_id

WHERE 1=1
    -- Exclude user profiles. These are not rated
    AND NOT REGEXP_CONTAINS(LOWER(s.name), r'^u_.*')

    -- Sample of subreddits for demo
    AND ti.subreddit_name IN (
        'de', 'mexico', 'turkey', 'india'
        , 'japan', 'china_irl', 'korea', 'bangladesh'
        , 'finanzen'
        , 'wallstreetbets'

        , 'nba', 'baseball', 'nfl', 'cricket', 'rugby'
        , 'fussball'
        , 'bundesliga'

        , 'askreddit', 'fragreddit'
        , 'relationship_advice', 'lgbt', 'asktransgender'
        , 'fitness'
        , 'formula1'
        -- , 'beyondthebump'
        , 'wallstreetbets', 'finanzen'
        , 'cryptocurrency', 'nft'
        , 'personalfinance', 'fire', 'fireuk', 'ausfinance'

        , 'pics', 'showerthoughts'
        , 'meirl', 'ich_iel', 'me_irl'

        -- Unlabeled with high # of users
        -- , 'bathandbodyworks'
        -- , 'essentialoils'
        , 'teslamotors', 'cartalkuk'
        , 'cocktails', 'tequila'
        -- , 'cbd', 'electronic_cigarette'
        , 'comedy'
        -- , 'projectrunway'
        -- , '90s'
        -- , 'mrrobot'
        -- , 'lacasadepapel'
        , 'asksciencefiction'
        -- , 'fashionreps', 'malefashion'
        , 'binance'
        , 'robinhood', 'gmecanada'
        , 'keto'
        , 'castiron'
        , 'gamedev', 'apexcirclejerk', 'dungeonsanddragons'
        , 'guns'
        , 'homebrewing'
        , 'chihuahua'
        , 'aww'
        -- , 'service_dogs'
        , 'aquariums'
        , 'interestingasfuck', 'damnthatsinteresting', 'nextfuckinglevel'
        , 'humansbeingbros'
        , 'oddlyterrifying', 'oddlysatisfying'
        , 'holup', 'worldpolitics'
        -- , 'help', 'modsupport'
        , 'copypasta_es'
        , 'tifu'
        -- , 'casualconversation', 'happy', 'casualuk'
        -- , 'unpopularopinion', 'randomthoughts'
        -- , 'ghosts', 'paranormal'
        -- , 'mbti'
        -- , 'unsolvedmysteries'
        , 'publicfreakout', 'iamatotalpieceofshit', 'rage'
    )

    -- OPTIONAL: Exclude spam, removed, & sketchy subs
    -- AND COALESCE(s.verdict, 'f') <> 'admin_removed'
    -- AND COALESCE(s.is_spam, FALSE) = FALSE
    -- AND COALESCE(s.is_deleted, FALSE) = FALSE
    -- AND s.deleted IS NULL
    -- AND type IN ('public', 'private', 'restricted')

ORDER BY cluster_name, cluster_subtopic, users_l7 DESC
;
