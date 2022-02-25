"""
Utils to reshape cluster outputs when creating a sheet to QA clusters in a specific country

Note that these functions are expected to be done in a Colab notebook where we can run
queries from BigQuery & read/write to google sheets.

The SQL queries below need to run in a colab cell (bigquery magic) because that's
the fastest way to get the queries from BQ into a pandas dataframe
"""
import gc
from typing import Union, Tuple

from tqdm import tqdm
import pandas as pd

from .clustering_utils import (
    create_dynamic_clusters
)
from ..utils.eda import (
    reorder_array,
)

_L_MATURE_CLUSTERS_TO_EXCLUDE_FROM_QA_ = [
    '0001',
    '0001-0001',
    '0001-0001-0001',
    '0001-0001-0001-0001-0001-0001-0001-0001',
    '0001-0001-0001-0001-0001-0001-0001-0001-0001-0001-0001-0001-0001-0001-0001-0001-0001-0001-0001-0001-0001-0001',
    '0001-0001-0001-0001-0001-0001-0001-0001-0001-0002-0002-0002-0002-0002-0002-0002-0003-0003-0003-0003-0003-0003',
    '0001-0001-0001-0001-0001-0001-0001-0001-0001-0002-0002-0002-0002-0002-0002-0002-0003-0003-0004',
    '0001-0001-0001-0001-0001-0001-0001-0001-0001-0002-0002-0002-0002-0002-0003-0003',
    '0001-0001-0001-0001-0001-0001-0001-0001-0001-0002-0002-0003-0003-0003-0004-0004-0006-0007-0009-0010-0010-0010',
    '0001-0001-0001-0001-0001-0001-0001-0001-0003-0005-0006-0007-0009-0009-0012-0012-0018-0020-0023-0026-0026-0026',
    '0001-0001-0002-0002-0002-0003-0003-0003-0006-0011-0014-0016',
    '0001-0001-0002-0002-0002-0003-0003-0003-0006-0012-0015-0017-0028-0034-0040-0041-0057-0065-0071-0081-0086-0087',
    '0001-0001-0002-0002-0002-0003-0003-0003-0007-0013-0017-0019',
    '0001-0001-0002-0002-0002-0003-0003-0003-0006-0012',
    '0001-0001-0002-0002-0002-0003-0003-0003-0009',
    '0001-0001-0003-0003-0003-0004-0004-0004-0010-0018-0025-0028',
    '0001-0001-0003-0003-0003-0004-0004-0004-0010-0018-0025-0028-0045-0053-0060-0066-0088-0099-0110-0125-0133-0134',
    '0001-0001-0003-0003-0003-0004-0004-0004-0010-0018-0025-0028-0046-0054-0061-0067-0089',
    '0001-0001-0003-0003-0003-0004-0004-0004-0010-0019',
    '0001-0001-0003-0003-0003-0004-0005-0005-0012-0022-0032',
    '0001-0001-0003-0003-0003-0004-0005-0005-0012-0022-0032-0036-0055-0067-0078-0086-0113-0129-0141-0158-0168-0169',
    '0001-0001-0003-0003-0003-0004-0005-0005-0012-0022-0032-0036-0055-0067-0079-0087-0114-0130-0142-0159-0169-0170',
    '0001-0001-0003-0003-0003-0004-0005-0005-0012-0023-0033',
    '0001-0001-0003-0003-0003-0004-0005-0005-0012-0023-0033-0038-0058-0070-0082-0090-0119-0137-0150-0169-0179-0180',
    '0001-0001-0003-0003-0003-0004-0005-0005-0013',
    '0001-0001-0003-0003-0003-0004-0005-0005-0014',
    '0001-0001-0001-0001-0001-0002-0002-0002',
    '0001-0001-0002-0002-0002-0003-0003-0003',
    '0001-0001-0002-0002-0002-0003-0003-0003-0008-0015',
    '0001-0001-0002-0002-0002-0003-0003-0003-0008-0015-0020-0023-0037-0044-0051-0056-0076-0085-0093-0108-0115-0116',
    '0001-0001-0003-0003-0003-0004-0005-0005',
    '0001-0001-0002-0002-0002-0003-0003-0003-0008-0015-0020-0023-0038-0045-0052-0057',

    '0002',
    '0002-0002',
    '0002-0002-0005-0005-0005',
    '0002-0002-0005-0005-0005-0007-0008-0009',
    '0002-0002-0004-0004-0004-0005-0006-0006-0015',
    '0002-0002-0004-0004-0004-0005-0006-0006-0015-0033-0045-0053-0083-0103-0127-0142-0182-0214-0238-0265-0281-0285',
    '0002-0002-0004-0004-0004-0005-0006-0006-0015-0034-0047-0055',
    '0002-0002-0004-0004-0004-0005-0006-0006-0015-0034-0047-0055-0087-0107-0131-0146-0186-0218-0242-0269-0286-0290',
    '0002-0002-0005-0005-0005-0006-0007-0007-0017',
    '0002-0002-0005-0005-0005-0006-0007-0007-0017-0038-0053-0061-0096-0117-0144-0160-0205-0240-0264-0291-0308-0312',
    '0002-0002-0005-0005-0005-0006-0007-0007-0017-0038-0054-0062-0097-0118',
    '0002-0002-0005-0005-0005-0006-0007-0007-0017-0038-0054-0063-0098-0119-0147-0163',
    '0002-0002-0005-0005-0005-0007-0008-0009-0019-0041',
    '0002-0003-0007-0008-0008-0010-0011-0012',
    '0002-0004-0009-0010-0010-0012-0013',

    '0003',

    '0004',

    '0005',
    '0005-0007-0012',
    '0005-0007-0012-0014-0014-0017-0018',

    '0008-0013',  # thegirlsurvivalguide could be good to show, but prob not for ppl looking at r/onlinedating...?

    # 0008-0013...0046-0064 is a can of worms... but it includes askreaddit, feminism
    #  and other LGBTQ subs that could be good for some people...
    # '0008-0013-0023-0032-0034-0044-0046-0064',

    '0008-0014-0025-0034-0036-0046-0048-0066',
    '0008-0014-0025-0034-0036-0046-0048-0066-0190-0378',
    '0008-0014-0025-0034-0036-0047-0049-0067',
    '0008-0014-0025-0034-0036-0047-0049-0067-0193',

    '0010-0017-0030-0040-0042-0053-0056-0078-0219-0437-0630-0705',
    '0010-0017-0030-0040-0042-0053-0056-0078-0219-0437-0630-0705-1017-1195-1439-1532-1828-2032-2196-2380-2482-2516',
]


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


def get_table_for_optimal_dynamic_cluster_params(
        df_labels_target: pd.DataFrame,
        col_new_cluster_val: str = 'cluster_label',
        col_new_cluster_name: str = 'cluster_label_k',
        col_new_cluster_prim_topic: str = 'cluster_majority_primary_topic',
        col_new_cluster_topic_mix: str = 'cluster_topic_mix',
        min_subs_in_cluster_list: list = None,
        col_num_orph_subs: str = 'num_orphan_subreddits',
        col_num_subs_mean: str = 'num_subreddits_per_cluster_mean',
        col_num_subs_median: str = 'num_subreddits_per_cluster_median',
        return_optimal_min_subs_in_cluster: bool = False,
        verbose: bool = False,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, int]]:
    """We want to balance two things:
    - prevent orphan subreddits
    - prevent clusters that are too large to be meaningful

    In order to do this at a country level, we'll be better off starting with smallest clusters
    and rolling up until we have at least N subreddits in one cluster.
    """
    if min_subs_in_cluster_list is None:
        min_subs_in_cluster_list = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    # even if cluster at k < 20 is generic, keep it to avoid orphan subs
    #  For a while I used a slice to exclude the broadest clusters
    #  but that left a lot of orphans
    l_cols_labels = (
        [c for c in df_labels_target.columns
         if all([c != col_new_cluster_val, c.endswith('_label')])
         ]
        # [1:]  # use all the columns! helps prevent a orphan subs
    )

    l_iteration_results = list()
    n_subs_in_target = df_labels_target['subreddit_id'].nunique()

    for n_ in tqdm(min_subs_in_cluster_list):
        d_run_clean = dict()
        d_run_clean['subs_to_cluster_count'] = n_subs_in_target
        d_run_clean['min_subreddits_in_cluster'] = n_

        df_clusters_dynamic_ = create_dynamic_clusters(
            df_labels_target,
            agg_strategy='aggregate_small_clusters',
            min_subreddits_in_cluster=n_,
            l_cols_labels_input=l_cols_labels,
            col_new_cluster_val=col_new_cluster_val,
            col_new_cluster_name=col_new_cluster_name,
            col_new_cluster_prim_topic=col_new_cluster_prim_topic,
            verbose=verbose,
        )
        d_run_clean = {
            **d_run_clean,
            **get_dynamic_cluster_summary(
                    df_dynamic_labels=df_clusters_dynamic_,
                    col_new_cluster_val=col_new_cluster_val,
                    col_new_cluster_name=col_new_cluster_name,
                    col_new_cluster_prim_topic=col_new_cluster_prim_topic,
                    col_new_cluster_topic_mix=col_new_cluster_topic_mix,
                    col_num_orph_subs=col_num_orph_subs,
                    col_num_subs_mean=col_num_subs_mean,
                    col_num_subs_median=col_num_subs_median,
                    return_dict=True,
            )
        }

        l_iteration_results.append(d_run_clean)

    del df_clusters_dynamic_, d_run_clean
    gc.collect()

    if return_optimal_min_subs_in_cluster:
        df_out = pd.DataFrame(l_iteration_results)
        optimal_min = df_out.loc[
            df_out['num_orphan_subreddits'] == df_out['num_orphan_subreddits'].min(),
            'min_subreddits_in_cluster'
        ].values[0]
        return df_out, optimal_min
    else:
        return pd.DataFrame(l_iteration_results)


def get_dynamic_cluster_summary(
        df_dynamic_labels: pd.DataFrame,
        col_new_cluster_val: str = 'cluster_label',
        col_new_cluster_name: str = 'cluster_label_k',
        col_new_cluster_prim_topic: str = 'cluster_majority_primary_topic',
        col_new_cluster_topic_mix: str = 'cluster_topic_mix',
        col_num_orph_subs: str = 'num_orphan_subreddits',
        col_num_subs_mean: str = 'num_subreddits_per_cluster_mean',
        col_num_subs_median: str = 'num_subreddits_per_cluster_median',
        return_dict: bool = True,
) -> Union[dict, pd.DataFrame]:
    """Input a dynamic cluster and get a summary for the cluster"""
    d_run = dict()

    d_run['cluster_count'] = df_dynamic_labels[col_new_cluster_val].nunique()
    df_vc_clean = df_dynamic_labels[col_new_cluster_val].value_counts()
    dv_vc_below_threshold = df_vc_clean[df_vc_clean <= 1]
    d_run[col_num_orph_subs] = len(dv_vc_below_threshold)
    d_run[col_num_subs_mean.replace('_mean', '_min')] = df_vc_clean.min()
    d_run[col_num_subs_mean] = df_vc_clean.mean()
    d_run[col_num_subs_median] = df_vc_clean.median()
    d_run[col_num_subs_mean.replace('_mean', '_max')] = df_vc_clean.max()

    # get count of mature clusters
    df_unique_clusters = df_dynamic_labels.drop_duplicates(
        subset=[col_new_cluster_val, col_new_cluster_name]
    )
    d_run['num_clusters_with_mature_primary_topic'] = (
        df_unique_clusters[col_new_cluster_prim_topic].str.lower()
        .str.contains('mature')
        .sum()
    )

    # convert list to string so we don't run into problems with pandas & styling
    d_run['cluster_ids_with_orphans'] = ', '.join(sorted(list(dv_vc_below_threshold.index)))

    if return_dict:
        return d_run
    else:
        return pd.DataFrame([d_run])


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
