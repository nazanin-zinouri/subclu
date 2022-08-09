-- Query to get subreddits relevant to a list of specific countries
-- that are within a specific cluster
-- Francis Foo requested it to send push notifications
--  to users that subscribe to these subs so they can check out
--  r/commonwealthgames

-- Pick clusters to select
--   See cluster names here:
--   https://docs.google.com/spreadsheets/d/1Mug388O6DDxF3I9sNYl03_tShsX6vqGIwpxe0Rrp3mM/edit#gid=2065532248
DECLARE TARGET_CLUSTER_NAMES DEFAULT [
    'Sports', 'Outdoors and Nature, Sports and Gaming'
];

-- Pick primary topics to select. These come from the crowdsourced topics
DECLARE TARGET_PRIMARY_TOPICS DEFAULT [
    'Sports'
];

-- Pick countries & relevance threshold
DECLARE GEO_TARGET_COUNTRY_CODES DEFAULT [
    'GB', 'AU'
];
DECLARE GEO_TARGET_COUNTRY_NAMES DEFAULT [
    'India', 'Anguilla', 'Antigua and Barbuda', 'Australia', 'Bangladesh'
    , 'Barbados', 'Belize', 'Bermuda', 'Botswana', 'British Virgin Islands'
    , 'Brunei', 'Cameroon', 'Canada', 'Cayman Islands', 'Cook Islands', 'Cyprus'
    , 'Dominica', 'England', 'Eswatini', 'Falkland Islands'
    , 'Fiji', 'Ghana', 'Gilbraltar', 'Grenada', 'Guernsey'
    , 'Guyana', 'Isle of Man ', 'Jamaica', 'Jersey', 'Kenya', 'Kiribati'
    , 'Lesotho', 'Malawi', 'Malaysia', 'Maldives', 'Malta ', 'Mauritius'
    , 'Montserra', 'Mozambique', 'Namibia', 'Nauru', 'New Zealand', 'Nigeria'
    , 'Niue', 'Nortolk Island', 'Northern Ireland', 'Pakistan', 'Papua New Guinea'
    , 'Rwanda', 'Samoa', 'Scotland', 'Seychelles', 'Sierra Leone', 'Singapore'
    , 'Solomon Islands', 'South Africa', 'Sri Lanka', 'St Helena', 'Saint Kitts and Nevis'
    , 'Saint Lucia', 'St Vincent & Grenadines', 'Tanzania', 'Bahamas', 'Gambia', 'Tonga'
    , 'Trinidad and Tobago', 'Turks and Caicos Islands', 'Tuvalu', 'Uganda', 'Vanuatu', 'Wales', 'Zambia'
];

DECLARE MIN_USERS_PERCENT_BY_SUBREDDIT_L28 NUMERIC DEFAULT 0.20; -- default is 0.14 (14%)
DECLARE MIN_USERS_PERCENT_BY_COUNTRY_STANDARDIZED NUMERIC DEFAULT 2.9; -- default is 3.0


WITH
-- Define Target-country subreddits
subreddits_relevant_geo AS (
    SELECT
        rel.subreddit_id
        , nt.rating_short
        , nt.primary_topic
        , rel.subreddit_name
        , m.k_0100_label_name
        , rel.country_name
        , rel.geo_country_code
        -- sa.* EXCEPT(
        --     users_l7_rank_100, users_l7_rank_400
        --     , posts_l7_rank_100, posts_l7_rank_400
        --     , app_users_l7, app_users_l28
        -- )

    FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_relevance_beta_20220725` AS rel
        LEFT JOIN (
            SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
            WHERE pt = (CURRENT_DATE() - 2)
        ) AS nt
            ON rel.subreddit_id = nt.subreddit_id
        LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_activity` AS sa
            ON sa.subreddit_id = rel.subreddit_id
        -- Merge with the new manual labels table so that we can get the rank by new cluster name
        LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_manual_names` AS m
            ON sa.k_0400_label = m.k_0400_label
    WHERE 1=1
        AND (
            -- For the target country include more subreddits that are relevant with other scores
            geo_relevance_default = TRUE
            OR users_percent_by_subreddit_l28 >= MIN_USERS_PERCENT_BY_SUBREDDIT_L28
            OR users_percent_by_country_standardized >= MIN_USERS_PERCENT_BY_COUNTRY_STANDARDIZED
        )
        AND (
            geo_country_code IN UNNEST(GEO_TARGET_COUNTRY_CODES)
            OR country_name IN UNNEST(GEO_TARGET_COUNTRY_NAMES)
        )
        AND (
            m.k_0100_label_name IN UNNEST(TARGET_CLUSTER_NAMES)
            OR nt.primary_topic IN UNNEST(TARGET_PRIMARY_TOPICS)
        )


        -- Exclude sub names that are noisy/incorrect
        AND rel.subreddit_name NOT LIKE "aviation%"
        AND rel.subreddit_name NOT LIKE "airline%"
        AND rel.subreddit_name NOT LIKE "%bussimulator%"

        -- Exclude some clusters and topics that are noisy
        AND m.k_0100_label_name NOT IN (
            'Guns, Weapons, & Ammunition', 'Marketplace and Deals'
        )
        AND nt.primary_topic NOT IN (
            'Hobbies', 'Travel', 'Outdoors and Nature', 'Technology'
            , 'Marketplace and Deals', 'Learning and Education', 'Place'
            , 'Podcasts and Streamers', 'Fitness and Nutrition'
            , 'Careers', 'Internet Culture and Memes', 'Crypto'
            , 'Cars and Motor Vehicles'
            , 'Tabletop Games'
        )
)

, subs_relevant_country_agg AS (
    -- Get 1 row per subreddit by agg of country into a column
    SELECT
        * EXCEPT(country_name, geo_country_code)
        -- Order so that we get (Mexico, US) only, and not both: (US, Mexico) AND (Mexico, US)
        , COUNT(geo_country_code) AS geo_relevant_country_count
        , STRING_AGG(country_name, ', ' ORDER BY country_name) AS geo_relevant_countries
        , STRING_AGG(geo_country_code, ', ' ORDER BY country_name) AS geo_relevant_country_codes
    FROM subreddits_relevant_geo
    GROUP BY 1, 2, 3, 4, 5
)


SELECT *
FROM subs_relevant_country_agg
ORDER BY subreddit_name
;
