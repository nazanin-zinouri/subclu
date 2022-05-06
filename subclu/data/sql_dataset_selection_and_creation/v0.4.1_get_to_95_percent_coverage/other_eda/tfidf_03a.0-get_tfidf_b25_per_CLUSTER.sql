-- Get TF-IDF & BM25 at CLUSTER level
--  The best strategy might be to get the top N from TF-IDF and top M from BM25

-- EXCLUDE rare words
DECLARE MIN_NGRAM_COUNT DEFAULT 4;
DECLARE MIN_DOCS_WITH_NGRAM NUMERIC DEFAULT 2;  -- 1= no filter
DECLARE MIN_DF NUMERIC DEFAULT 0.0;  -- 0= no filter. higher num -> exclude rare words

-- EXCLUDE common words
DECLARE MAX_DF NUMERIC DEFAULT 0.98;  -- 1.0= no filter. lower num -> exclude common words

-- k1 = 1.2 term frequency saturation paramete.
--  [0,3] Could be higher than 3 | [0.5,2.0] "Optimal" starting range
--  High -> staturation is slower (books)
--  Low  -> downweight counts quickly (news articles)
DECLARE K1 NUMERIC DEFAULT 24.0;

-- b  = 0.75 doc length penalty.  0 -> no penalty
--  [0,1] MUST be between 0 & 1 | [0.3, 0.9] "optimal" starting range
--  High -> broad articles, penalize a lot
--  Low  -> detailed/focused technical articles (low penalty)
DECLARE B NUMERIC DEFAULT 0.50;

-- CREATE OR REPLACE TABLE `reddit-employee-datasets.david_bermejo.subreddit_top_tfidf_bm25_20211215`
-- AS (
WITH ngram_counts_per_subreddit AS (
    -- By default start with subreddit level, need to change this to get cluster-level
    SELECT
        -- Set the cluster grain here:
        a.k_0070_label AS cluster_id
        , ngram
        , SUM(ngram_count) AS ngram_count
    FROM `reddit-employee-datasets.david_bermejo.subreddit_ngram_test_20211215` AS n
        LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full` AS a
        USING (subreddit_id)
    WHERE 1=1
        AND ngram_count >= MIN_NGRAM_COUNT
        -- For testing, filter subreddit names here, otherwise the IDF will be wrong

        -- Exclude stop words
        AND COALESCE(TRIM(ngram), '') NOT IN (
            -- German
            'eine', 'einen', 'einem', 'für', 'nicht', 'der', 'wenn', 'dass', 'dann'
            , 'ich', 'und', 'zu', 'sich', 'von', 'als', 'meine', 'meines', 'meinen', 'wird', 'sind'
            , 'jetzt', 'aber', 'in der', 'mehr', 'zum', 'keine', 'keinen', 'wie', 'wir', 'haben'
            , 'ich dann', 'irgendwann', 'ist', 'auf', 'auch', 'oder', 'vor', 'sie'
            , 'werden', 'mich', 'habe', 'nur', 'ihr', 'das', 'ein', 'une', 'noch', 'du'

            -- Stop words from other languages
            , 'să', 'det', 'det är', 'je', 'în', 'är', 'jeg', 'sh', 'ppy', 'sh scores', 'continuare'
            , 'resultat', 'me r', 'mogu', 'svt se', 'som', 'noen', 'ett', 'har'
            , 'že', 'da je', 'det er', 'få', 'ikke', 'för', 'på', 'och', 'jag'
            , 'att det', 'inte', 'sapmi', 'hvad', 'mycket', 'kako', 'hogy', 'egy'
            , 'form i'
            , 'artstation assets images', 'assets images images', 'youpoll me r', 'youpoll'
            , 'anime planet anime', 'planet anime', 'thanks hate', 'control out'
            , 'esteve aqui', 'judge judy judge'
            , 'routine help'
            , '7i16384', 'enjoy video think', 'video think doing', 'think doing good'
            , 'job consider subscribing', 'good job consider'
            , '1e1 3m5'
            , '3m7 1e1 3m5'
            , 'over round'
            , 'w paypal', 'new ★'
            , 'pleurotus ostreatus', 'oyster pleurotus'
            , 'what maximum', 'need included', 'f imgur'
            , 'image avatar'
            , 'link comments redgifs', 'comments redgifs'
            , 'source comment'
            , 'link source comment'
            , 'comment profile', 'before link comments', 'before link', 'seen before link', 'link comments'
            , 'telegram nos comentários', 'telegram nos', 'server discord', 'discord server discord'
            , 'free side', 'movies of all', 'movies of', 'of all time', 'of all', 'of', 'all', 'most demanded'
            , 'premium lifetime'
            , 'table provided rolfsweather', 'provided rolfsweather'
            , '7i16384 8i8192', '8i8192', '3m5', '3a'
            , 'ko na', 'kasi', 'only north', 'redejobs'
            , 'state f', '1cp', 'year results', 'date quality', 'dradio'
            , 'blev', 'bilo', 'skal', 'ljudi', 'jsem', 'eller', 'author u'

            , '`', '` `'
            , 'l l l', 'j j j', 'l l', 'j j', 'j', 'l'
            , '3a 3y', '3a 4y', 'data 3m7', '3m7', '3m7 1e1'
            , 'aon aon aon', 'aon aon', 'us7', 'ako', 'you pressed', 'gmail discord'
            , 'click above', 'pes pes pes', 'pes pes', 'him told', 'her told'
            , 'cum cum cum', 'cum cum'

            -- Flares
            , 'always yummy youtube', 'recipe always'
            , 'posted on', 'redejobs job'
            , 'binance en', 'use learn', 'date fri'
            , 'peak pro', 'important clarify', 'know more details'
            , 'stillness', 'objs', 'version clip'
            , 'la1ere', 'yards yards'
            , 'krak grenades', 'frag krak'
            , 'guilty guilty guilty', 'out out out', 'hi hi hi', 'hi hi', 'out out'
            , '❁', 'sh scores osu', 'ppy sh', 'osu ppy sh'
            , 'points comments', 'earn keep'

            , 'battlefield battlefield battlefield'
            , 'battlefield battlefield', 'aramco aramco aramco', 'aramco aramco'
            , 'cyberpunk cyberpunk cyberpunk', 'cyberpunk cyberpunk'
            , 'pancakeswap finance swap'
            , 'extremely easy use', 'en register'
            , 'x2003'
            , 'res cloudinary'
            , 'image upload'
            , 'la1ere francetvinfo fr'
            , 'judy judge judy'
            , 'forsen forsen forsen'
            , 'e9dj69yainqhvepk81jhsytacl0uxkwk5zfmnfe49tq3vun9av'
            , 'ontario ca en'
            , 'last day week'
            , 'nu nl', 'saintrampaljim'
            , 'very based', 'rampal'
            , 'set increases', 'retreat cost weakness'
            , 'length weight', 'off lowest', 'games detail'
            , 'sp open'

            -- URLs
            , 'open spotify album', 'open spotify track'
            , 'de file dradio', 'file dradio', 'dradio de file', 'mp3 dradio'
            , 'img image'

            -- English: most are now part of regex that removes most stopwords at the start
            , 'she', 'be', 'youtu', 'the', 'not', 'my', 'by', 'you', 'your'
            , 'its', 'was', 'yep'

            -- French, Spanish, Others
            , 'hay', 'ser', 'fue', 'por el', 'se', 'al'
            , 'de la', 'à', 'en la', 'en el', 'a', 'me', 'el', 'una', 'del'
            , 'je', 'por', 'a la', 'de la', 'de los', 'lo que'
            , 'que', 'que se', 'que les', 'qué', 'más', 'que no', 'nous'
            , 'pero', 'algo', 'muy', 'nada', 'hace', 'hacer', 'tengo', 'tiene'
            , 'hasta', 'de las', 'desde', 'no se', 'no me', 'no te', 'no le'
            , 'estaba', 'cuando', 'como', 'esta'
            , 'pas', 'les', 'des'
            , 'porque', 'y', 'e', 'o', 'na', 'com', 'con', 'los', 'de', 'isso'
            , 'ele', 'meu', 'es'
        )


    -- WE NEED TO GROUP BY because otherwise we'll get duplicate ngrams per cluster
    GROUP BY 1, 2

)

, ngram_total_words AS (
    -- Total words in each CLUSTER or SUBBREDDIT
    --  If we want to do it by cluster we'd need to join with a table that has cluster IDs
    SELECT
        cluster_id  -- change this param to get a cluster grouping

        , COUNT(*) OVER() AS n_docs  -- how many subreddits/clusters (for idf)
        , SUM(ngram_count) AS total_count

    FROM ngram_counts_per_subreddit
    GROUP BY 1
)
, avg_ngrams_per_subreddit AS (
    -- We need this average for BM25
    SELECT
        AVG(total_count) AS avg
    FROM ngram_total_words
)
, ngram_tf AS (
    -- Term-Frequency for ngram in cluster
    SELECT
        n.cluster_id
        , n.ngram
        , ngram_count
        , n.ngram_count / t.total_count AS tf
        , ngram_count / (
            ngram_count +
            K1 * (
                1.0 - B +
                B * total_count / (SELECT avg FROM avg_ngrams_per_subreddit)
            )
        ) AS tf_bm25

    FROM ngram_counts_per_subreddit AS n
        INNER JOIN ngram_total_words AS t
            USING(cluster_id)
)
, ngram_in_docs AS (
    -- How many "documents" have a word
    --  docs could be subreddits or clusters
    SELECT
        ngram
        , COUNT(DISTINCT cluster_id) n_docs_with_ngram
    FROM ngram_counts_per_subreddit
    GROUP BY 1
)
, ngram_idf AS (
    -- df & idf for an ngram
    SELECT
        n.ngram
        , n.n_docs_with_ngram
        , n_docs
        , n_docs_with_ngram / t.n_docs         AS df
        , LOG(t.n_docs / n_docs_with_ngram)    AS idf
        , LN(1 + (n_docs - n_docs_with_ngram + 0.5) / (n_docs_with_ngram + 0.5)) as idf_prob
    FROM ngram_in_docs AS n
        CROSS JOIN (
           SELECT DISTINCT
              n_docs
           FROM ngram_total_words
        ) AS t
)
, tf_idf_and_bm25_raw AS (
    -- We can save this "raw" table
    --   and apply filters on demand like: min count, min & max df
    SELECT
        t.*
        , i.* EXCEPT(ngram)
        , tw.* EXCEPT(cluster_id, n_docs)
        , tf * idf AS tfidf
        , tf_bm25 * idf_prob AS bm25

    FROM ngram_tf AS t
        LEFT JOIN ngram_idf AS i
            USING(ngram)
        LEFT JOIN ngram_total_words AS tw
            USING(cluster_id)
)
, tf_idf_with_rank AS (
    SELECT
        t.cluster_id
        , t.ngram
        , ((ngram_rank_bm25 + ngram_rank_tfidf) / 2) AS ngram_rank_avg
        , ngram_type
        , ngram_char_len
        , t.* EXCEPT(cluster_id, ngram)
    FROM (
        SELECT
            cluster_id
            , ngram
            , ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY bm25 DESC, ngram_count DESC) as ngram_rank_bm25
            , ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY tfidf DESC, ngram_count DESC) as ngram_rank_tfidf
            , bm25
            , tfidf
            , ngram_count
            , * EXCEPT(cluster_id, ngram, tfidf, bm25, ngram_count)
        FROM tf_idf_and_bm25_raw
        WHERE 1=1
            -- We need to filter out before we calculate ranks
            AND df >= MIN_DF
            AND df <= MAX_DF
            AND n_docs_with_ngram >= MIN_DOCS_WITH_NGRAM
    ) AS t
    -- Join to get the len & type of ngram (unigram, bigram, trigram)
    LEFT JOIN (
        SELECT DISTINCT ngram, ngram_type, ngram_char_len
        FROM `reddit-employee-datasets.david_bermejo.subreddit_ngram_test_20211215`
    ) AS n
        ON t.ngram = n.ngram
)


-- Check TF-IDF & BM25
SELECT
    t.cluster_id
    -- , a.cluster_name
    , t.* EXCEPT(cluster_id)
FROM tf_idf_with_rank AS t
    -- TODO(djb) Need to join to new table to get subreddit cluster name
    -- LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` AS a


WHERE 1=1
    AND (
        ngram_rank_bm25 <= 7
        OR ngram_rank_tfidf <= 7
    )

ORDER BY cluster_id, ngram_rank_avg
-- LIMIT 5000
-- );  -- close create TABLE
