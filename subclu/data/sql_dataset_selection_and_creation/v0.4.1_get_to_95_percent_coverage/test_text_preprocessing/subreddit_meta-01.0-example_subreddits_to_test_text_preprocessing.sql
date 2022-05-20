-- Check text preprocessing on key subreddits
--  Cover multiple languages and levels of markdown use

SELECT
  geo_relevant_countries
  , subreddit_public_description
  , subreddit_description
  , subreddit_name
  , subreddit_name_title_related_subs_and_clean_descriptions

FROM `reddit-relevance.tmp.subclu_subreddits_for_modeling_20220517`

WHERE subreddit_name IN (
  'eyebleach', 'hmmm', 'blursedimages'
  , 'bollywood', 'twitter'
  , 'makenewfriendshere', 'berlinsocialclub'
  , 'formula1'
  , 'de', 'mexico'
  , 'meirl', 'ich_iel'
  , 'nederlands', 'vdm'
  , 'relationship_advice', 'askreddit', 'fragreddit'
  , 'fitness'
  , 'beyondthebump'
  , 'wallstreetbets', 'finanzen'
  , 'cryptocurrency', 'nft'
  , 'personalfinance', 'fire', 'fireuk', 'ausfinance'
  , 'pics', 'showerthoughts'
  , 'japan', 'china_irl', 'korea'
  , 'listentothis'
)
