-- Queries to export data from bigQuery to create GCS parquet files
-- THen I can clean these parquet files to create local data we can use in a
--  dash app


-- Get latest geo-relevance scores
-- Pick only countries in tiers: 0, 1, 2
EXPORT DATA OPTIONS(
  uri='gs://i18n-subreddit-clustering/ReddEx/2021-06-13/subclu_subreddit_geo_relevance_standardized_20220611/*.parquet',
  format='PARQUET',
  overwrite=true
) AS
SELECT *
FROM `reddit-relevance.tmp.subclu_subreddit_geo_relevance_standardized_20220611`
WHERE 1=1
  AND (
      -- tier 0
      geo_country_code IN ('GB','AU','CA')

      -- tier 1
      OR geo_country_code IN ('DE','FR','BR','MX','IN')

      -- tier 2
      OR geo_country_code IN ('IT','ES','JP','KR','PH','NL','TR','RO','DK','SE','FI','PL','ID','RU')

      -- other countries in top 50
      OR geo_country_code = 'US'

      OR geo_country_code IN (
          'SG', 'NZ', 'MY', 'NO', 'BE', 'IE', 'AR', 'AT', 'CH', 'PT'
          -- , 'CZ', 'HU', 'ZA', 'CL', 'VN', 'HK', 'TH', 'CO', 'GR', 'UA'
          -- , 'IL', 'AE', 'TW', 'SA', 'PE', 'RS', 'HR'
      )
  )

-- EXPORT DATA OPTIONS(
--   uri='gs://i18n-subreddit-clustering/ReddEx/2021-06-13/subclu_v0041_subreddit_activity/*.parquet',
--   format='PARQUET',
--   overwrite=true
-- ) AS

-- SELECT *
-- FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_activity`
-- ;

-- EXPORT DATA OPTIONS(
--   uri='gs://i18n-subreddit-clustering/ReddEx/2021-06-13/subclu_v0041_subreddit_clusters_c_a_full/*.parquet',
--   format='PARQUET',
--   overwrite=true
-- ) AS

-- SELECT model_sort_order
--     ,subreddit_name, subreddit_id, posts_for_modeling_count
--     , k_0010_label, k_0013_label, k_0020_label, k_0030_label, k_0040_label, k_0041_label, k_0050_label, k_0060_label, k_0070_label, k_0080_label, k_0090_label
--     , k_0100_label, k_0125_label, k_0150_label, k_0175_label, k_0300_label, k_0320_label, k_0400_label, k_0500_label
--     , k_0600_label, k_0700_label, k_0800_label, k_0900_label
--     , k_1000_label, k_1250_label, k_1500_label, k_1750_label, k_2000_label, k_2250_label, k_2500_label, k_2750_label
--     , k_3000_label, k_3200_label, k_3400_label, k_3600_label, k_3800_label, k_4000_label
-- FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a_full`
-- ;

-- EXPORT DATA OPTIONS(
--   uri='gs://i18n-subreddit-clustering/ReddEx/2021-06-13/subclu_v0041_subreddit_clusters_c_manual_names/*.parquet',
--   format='PARQUET',
--   overwrite=true
-- ) AS

-- SELECT *
-- FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_manual_names`
-- ;


-- EXPORT DATA OPTIONS(
--   uri='gs://i18n-subreddit-clustering/ReddEx/2021-06-13/subclu_v0041_subreddit_distances_c_top_100/*.parquet',
--   format='PARQUET',
--   overwrite=true
-- ) AS

-- SELECT *
-- FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_distances_c_top_100`
-- WHERE distance_rank <= 50
-- ;


-- EXPORT DATA OPTIONS(
--   uri='gs://i18n-subreddit-clustering/ReddEx/2021-06-13/subclu_v0041_subreddit_language_rank/*.parquet',
--   format='PARQUET',
--   overwrite=true
-- ) AS

-- SELECT *
-- FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_language_rank`
-- ;



-- EXPORT DATA OPTIONS(
--   uri='gs://i18n-subreddit-clustering/ReddEx/2021-06-13/subclu_v0041_subreddit_tsne1/*.parquet',
--   format='PARQUET',
--   overwrite=true
-- ) AS

-- SELECT
--   subreddit_id
--   , subreddit_name
--   , tsne_0, tsne_1
--   , geo_relevant_subreddit_all, geo_relevant_countries_all, i18n_type, i18n_country_code, i18n_type_2
--   , geo_relevant_subreddit, geo_relevant_countries, geo_relevant_country_codes, geo_relevant_country_count
--   , subreddit_clean_description_word_count, subreddit_name_title_and_clean_descriptions_word_count, subreddit_title, subreddit_public_description, subreddit_description, subreddit_name_title_and_clean_descriptions
--   , primary_post_language, primary_post_language_percent, primary_post_language_in_use_multilingual, secondary_post_language, secondary_post_language_percent, post_median_word_count, post_median_text_len
-- FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_tsne1`
-- ;
