-- noinspection SqlNoDataSourceInspectionForFile

-- List of some specific subreddits comes from ambassador program
CREATE TABLE `reddit-employee-datasets.david_bermejo.subclu_selected_subs_20210506`
AS
(
    SELECT
        geo.subreddit_name
        , geo.geo_country_code
        , geo.pct_sv_country
        , geo.rank_no

    FROM `reddit-employee-datasets.wacy_su.geo_relevant_subreddits_2021` AS geo

    WHERE geo.geo_country_code = "DE"
        and geo.rank_no <= 15
)

UNION ALL

(
    SELECT
        filt.subreddit_name
        , geo.geo_country_code
        , geo.pct_sv_country
        , geo.rank_no

    FROM (
        SELECT
            asr.subreddit_name

        FROM `data-prod-165221.all_reddit.all_reddit_subreddits` asr

        WHERE DATE(asr.pt) = "2021-05-05"
            AND asr.subreddit_name IN(
                "studentenkueche",
                "augenschmaus",
                "backen",
                "vegetarischkochen",
                "einheitsgebot",
                "gaumengraus",
                "veganerezepte",
                "vegetarischde",
                "nudeln",
                "asiatischkochen",
                "spube",
                "mediende",
                "mediathek",
                "musizierende",
                "kulturdigital",
                "buehne",
                "streamen",
                "deutschefilme",
                "kurzgefragt",
                "lustiges",
                "duschgedanken",
                "heutelernteich",
                "binichdasarschloch",
                "naturfreunde",
                "bestofde",
                "lagerfeuer",
                "ratschlag",
                "dingore",
                "augenbleiche",
                "daheim",
                "fussball",
                "formel1",
                "motorsport_de",
                "handball_de",
                "gfl",
                "reitsport",
                "radsport",
                "wandern",
                "kreisliga",
                "flascheleer",
                "kampfsport",
                "schach",
                "tischtennis",
                "sommerspiele",
                "platzreife",
                "sport",
                "frauenstudio",
                "fifa_de",
                "rainbowsixde",
                "raketenliga",
                "diesiedler",
                "annode",
                "playsi",
                "aoede",
                "clashofclansde",
                "counterstrikede",
                "pcbaumeister",
                "battlefieldde",
                "footballmanagerde",
                "eurotruckde",
                "minecraftde",
                "switchde",
                "zeldade"
            )
    ) AS filt
    LEFT JOIN `reddit-employee-datasets.wacy_su.geo_relevant_subreddits_2021` AS geo
        ON filt.subreddit_name = geo.subreddit_name
)

ORDER BY subreddit_name ASC, rank_no ASC
;
