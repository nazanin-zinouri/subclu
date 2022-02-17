"""
Utils to reshape cluster outputs when creating a sheet to QA clusters in a specific country

Note that these functions are expected to be done in a Colab notebook where we can run
queries from BigQuery & read/write to google sheets.

The SQL queries below need to run in a colab cell (bigquery magic) because that's
the fastest way to get the queries from BQ into a pandas dataframe
"""
import gc
import pandas as pd

from ..utils.eda import (
    reorder_array,
)


def keep_only_target_labels(
        df_labels: pd.DataFrame,
        df_geo: pd.DataFrame,
        col_sort_order: str = 'model_sort_order',
        l_ix_subs: list = None,
        l_cols_to_front: list = None,
        geo_cols_to_drop: list = None,
) -> pd.DataFrame:
    """Keep only subs that are in BOTH:
    - df-geo-relevance for target country
    - df-lables (subreddits that have been clustered)
    """
    if l_ix_subs is None:
        l_ix_subs = ['subreddit_name', 'subreddit_id']

    if geo_cols_to_drop is None:
        geo_cols_to_drop = ['primary_topic']
    # make sure that cols to drop exist:
    geo_cols_to_drop = [c for c in geo_cols_to_drop if c in df_geo.columns]

    # move cols to front
    if l_cols_to_front is None:
        l_cols_to_front = [
            col_sort_order,
            'subreddit_id',
            'subreddit_name',
            'primary_topic',
            'rating_short',
            'over_18',
            'rating_name',
        ]

    df_labels_target = (
        df_labels.merge(
            df_geo
            .drop(geo_cols_to_drop, axis=1)
            ,
            how='right',
            on=l_ix_subs,
        )
        .copy()
        .sort_values(by=[col_sort_order], ascending=True)
    )

    # move some columns to the end of the df
    l_cols_to_end = ['table_creation_date', 'mlflow_run_uuid']
    l_cols_to_end = [c for c in l_cols_to_end if c in df_labels_target.columns]

    df_labels_target = df_labels_target[
        df_labels_target.drop(l_cols_to_end, axis=1).columns.to_list() +
        l_cols_to_end
    ]

    # make sure cols to front exist in output
    l_cols_to_front = [c for c in l_cols_to_front if c in df_labels_target.columns]
    df_labels_target = df_labels_target[
        reorder_array(l_cols_to_front, df_labels_target.columns)
    ]

    # Drop subs if they're not in cluster
    mask_subs_not_in_model = df_labels_target[col_sort_order].isnull()
    print(f"{mask_subs_not_in_model.sum():,.0f} <- subs to drop b/c they're not in model")
    df_labels_target = df_labels_target[~mask_subs_not_in_model].copy()

    # Change key columns to integer
    df_labels_target[col_sort_order] = df_labels_target[col_sort_order].astype(int)

    l_cols_label_int = [c for c in df_labels_target.columns if c.endswith('_label')]
    df_labels_target[l_cols_label_int] = df_labels_target[l_cols_label_int].astype(int)

    print(f"{df_labels_target.shape} <- df_labels_target.shape")

    gc.collect()
    return df_labels_target


# ==================
# SQL queries
# ===
_SQL_GET_RELEVANT_SUBS_FOR_COUNTRY = """
%%time
%%bigquery df_geo --project data-science-prod-218515 

-- Select geo+cultural subreddits for a target country
--  And add latest rating & over_18 flags to exclude X-rated & over_18
DECLARE TARGET_COUNTRY STRING DEFAULT 'Australia';


SELECT
    s.* EXCEPT(over_18, pt, verdict) 
    , nt.rating_name
    , nt.primary_topic
    , nt.rating_short
    , slo.over_18
    , CASE 
        WHEN(COALESCE(slo.over_18, 'f') = 't') THEN 'over_18_or_X_M_D_V'
        WHEN(COALESCE(nt.rating_short, '') IN ('X', 'M', 'D', 'V')) THEN 'over_18_or_X_M_D_V'
        ELSE 'unrated_or_E'
    END AS grouped_rating

FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` AS t
    -- Inner join b/c we only want to keep subs that are geo-relevant AND in topic model
    INNER JOIN (
        SELECT *
        FROM `reddit-employee-datasets.david_bermejo.subclu_subreddit_geo_score_standardized_20220212`
        WHERE country_name = TARGET_COUNTRY
    ) AS s
        ON t.subreddit_id = s.subreddit_id

    -- Add rating so we can get an estimate for how many we can actually use for recommendation
    LEFT JOIN (
        SELECT *
        FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
        -- Get latest partition
        WHERE dt = DATE(CURRENT_DATE() - 2)
    ) AS slo
    ON s.subreddit_id = slo.subreddit_id
    LEFT JOIN (
        SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
        WHERE pt = DATE(CURRENT_DATE() - 2)
    ) AS nt
        ON s.subreddit_id = nt.subreddit_id

    -- Exclude popular US subreddits
    -- Can't query this table from local notebook because of errors getting google drive permissions. excludefor now
    LEFT JOIN `reddit-employee-datasets.david_bermejo.subclu_subreddits_top_us_to_exclude_from_relevance` tus
        ON s.subreddit_name = LOWER(tus.subreddit_name)

WHERE 1=1
    AND s.subreddit_name != 'profile'
    AND COALESCE(s.type, '') = 'public'
    AND COALESCE(s.verdict, 'f') <> 'admin_removed'
    AND COALESCE(slo.over_18, 'f') = 'f'
    AND COALESCE(nt.rating_short, '') NOT IN ('X', 'D')

    AND(
        s.geo_relevance_default = TRUE
        OR s.relevance_percent_by_subreddit = TRUE
        OR s.relevance_percent_by_country_standardized = TRUE
    )
    AND country_name IN (
            TARGET_COUNTRY
        )

    AND (
         -- Exclude subs that are top in US but we want to exclude as culturally relevant
         --  For simplicity, let's go with the English exclusion (more relaxed) than the non-English one
         COALESCE(tus.english_exclude_from_relevance, '') <> 'exclude'
    )

ORDER BY e_users_percent_by_country_standardized DESC, users_l7 DESC, subreddit_name
;
"""


_SQL_LOAD_MODEL_LABELS_ = """
%%time
%%bigquery df_labels --project data-science-prod-218515 

-- select subreddit clusters from bigQuery

SELECT
    sc.subreddit_id
    , sc.subreddit_name
    , nt.primary_topic

    , sc.* EXCEPT(subreddit_id, subreddit_name, primary_topic_1214)
FROM `reddit-employee-datasets.david_bermejo.subclu_v0041_subreddit_clusters_c_a` sc
    LEFT JOIN (
        -- New view should be visible to all, but still comes from cnc_taxonomy_cassandra_sync
        SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
        WHERE DATE(pt) = (CURRENT_DATE() - 2)
    ) AS nt
        ON sc.subreddit_id = nt.subreddit_id
;
"""

#
# ~ fin
#
