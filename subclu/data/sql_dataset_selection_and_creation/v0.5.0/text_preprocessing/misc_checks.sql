-- Get old subreddit descriptions to compare with new text preprocessing
SELECT
    subreddit_name
    , subreddit_name_title_and_clean_descriptions
FROM `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_no_geo_20211214`
WHERE 1=1
    AND (
        subreddit_name IN (
            "redditsessions", "blursedimages", "hmmm", "hmm", "bollywood", "bollyblindsngossip", "bollyarm", "bollywoodmemes", "twitter", "eyebleach", "makenewfriendshere", "meetpeople", "berlinsocialclub", "nycmeetups", "news"
            , "antiwork", "damnthatsinteresting", "publicfreakout", "lifeprotips", "jedi", "jamesbond", "clonewars", "archerfx", "loveislandtv", "residentevil", "mortalkombat", "ukrainewarvideoreport", "sfx", "formula1", "gonewild", "minecraft", "china_irl", "lebanon", "hottiesoftvandyt", "mdma"
            , "de", "mexico", "france", "rance_iel", "relationship_advice", "gonewildstories", "sex", "nsfwiama", "lgbt", "askgaybros", "asexuality", "me_irlgbt", "blackladies", "asianamerican", "mixedrace", "askreddit", "nostupidquestions", "yahooqr", "tinder", "ama", "wallstreetbets"
            , "cryptocurrency", "soccer", "nba", "nfl", "fifa", "sports", "baseball", "fuckcars", "texas", "canada", "australia", "worldnews", "ukraine", "europe", "todayilearned", "india", "conspiracy", "space", "nasa", "explainlikeimfive", "eldenring", "apexlegends", "hiphopheads", "music", "listentothis", "halo"
            , "marvelstudios", "starwars", "movies", "deuxmoi", "aww", "interestingasfuck", "anime", "genshin_impact", "sweden", "denmark", "romania", "ufc", "mma", "kpop", "kpopde", "freekarma4u", "fitness", "trees", "cocktails", "vegan"
            , "cooking", "food", "amitheasshole", "showerthoughts", "memexico", "mujico", "gardening", "humansaremetal", "anormaldayinrussia"
        )
  )
ORDER BY subreddit_name
LIMIT 1000
