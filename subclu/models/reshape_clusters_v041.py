"""
Utils to reshape cluster outputs when creating a sheet to QA clusters in a specific country

Note that these functions are expected to be done in a Colab notebook where we can run
queries from BigQuery & read/write to google sheets.

The SQL queries below need to run in a colab cell (bigquery magic) because that's
the fastest way to get the queries from BQ into a pandas dataframe
"""
import gc
from typing import Union, Tuple, List

from tqdm import tqdm
import numpy as np
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
    '0001-0001-0001-0001-0001-0001-0001-0001-0001-0001-0001',
    '0001-0001-0001-0001-0001-0001-0001-0001-0001-0002-0002',
    '0001-0001-0001-0001-0001-0001-0001-0001-0003-0005-0006',
    '0001-0001-0001-0001-0001-0002-0002-0002',
    '0001-0001-0001-0001-0001-0002-0002-0002-0004',
    '0001-0001-0001-0001-0001-0002-0002-0002-0004-0008-0011',
    '0001-0001-0001-0001-0001-0002-0002-0002-0005',
    '0001-0001-0001-0001-0001-0002-0002-0002-0005-0010-0013-0015',
    '0001-0001-0002-0002-0002-0003-0003-0003',
    '0001-0001-0002-0002-0002-0003-0003-0003-0006',
    '0001-0001-0002-0002-0002-0003-0003-0003-0006-0011-0014-0016',
    '0001-0001-0002-0002-0002-0003-0003-0003-0006-0012',
    '0001-0001-0002-0002-0002-0003-0003-0003-0006-0012-0015-0017',
    '0001-0001-0002-0002-0002-0003-0003-0003-0007-0013-0017-0019',
    '0001-0001-0002-0002-0002-0003-0003-0003-0007-0014-0018',
    '0001-0001-0002-0002-0002-0003-0003-0003-0008-0015',
    '0001-0001-0002-0002-0002-0003-0003-0003-0008-0015-0019',
    '0001-0001-0002-0002-0002-0003-0003-0003-0008-0015-0020',
    '0001-0001-0002-0002-0002-0003-0003-0003-0009',
    '0001-0001-0003-0003-0003-0004',
    '0001-0001-0003-0003-0003-0004-0004-0004-0010-0018-0025-0028',
    '0001-0001-0003-0003-0003-0004-0004-0004-0010-0019',
    '0001-0001-0003-0003-0003-0004-0005-0005',
    '0001-0001-0001-0001-0001-0002-0002-0002-0005-0009-0012-0014',
    '0001-0001-0002-0002-0002-0003-0003-0003-0009-0017',
    '0001-0001-0003-0003-0003-0004-0005-0005-0011-0020-0029',
    '0001-0001-0003-0003-0003-0004-0005-0005-0011-0020-0030',
    '0001-0001-0003-0003-0003-0004-0005-0005-0012',
    '0001-0001-0003-0003-0003-0004-0005-0005-0012-0022-0032',
    '0001-0001-0003-0003-0003-0004-0005-0005-0012-0023-0033',
    '0001-0001-0003-0003-0003-0004-0005-0005-0013',
    '0001-0001-0003-0003-0003-0004-0005-0005-0013-0026-0036-0043',
    '0001-0001-0003-0003-0003-0004-0005-0005-0014',

    '0002',
    '0002-0002',
    '0002-0002-0004-0004-0004-0005-0006-0006-0015',
    '0002-0002-0004-0004-0004-0005-0006-0006-0015-0033-0045-0053',
    '0002-0002-0004-0004-0004-0005-0006-0006-0015-0034-0047-0055',
    '0002-0002-0005-0005-0005',
    '0002-0002-0005-0005-0005-0006-0007-0007-0017-0036-0051-0059',
    '0002-0002-0005-0005-0005-0007-0008-0009-0022',
    '0002-0002-0005-0005-0005-0006-0007-0007-0017',
    '0002-0002-0005-0005-0005-0006-0007-0007-0017-0038-0053-0061',
    '0002-0002-0005-0005-0005-0006-0007-0007-0017-0038-0054-0062',
    '0002-0002-0005-0005-0005-0006-0007-0007-0017-0038-0054-0063',
    '0002-0002-0005-0005-0005-0007-0008-0008-0018-0039',
    '0002-0002-0005-0005-0005-0007-0008-0009',
    '0002-0002-0005-0005-0005-0007-0008-0009-0019',
    '0002-0002-0005-0005-0005-0007-0008-0009-0019-0041',
    '0002-0002-0005-0005-0005-0007-0008-0009-0022-0047',
    '0002-0002-0005-0005-0005-0007-0008-0009-0023',
    '0002-0003-0007-0008-0008-0010-0011-0012',
    '0002-0004',
    '0002-0004-0009-0010-0010-0012-0013',
    '0002-0004-0009-0010-0010-0012-0013-0015-0041-0081-0121',

    '0003',

    '0004',

    '0005',
    '0005-0007-0012',
    '0005-0007-0012-0014-0014-0017-0018',

    # '0008-0013',  # thegirlsurvivalguide could be good to show, but prob not for ppl looking at r/onlinedating...?
    # 0008-0013-0023-0032-0034-0044-0046-0064 is a can of worms... but it includes askreaddit, feminism
    #  and other LGBTQ subs that could be good for some people. Rely on QA process sort it out.
    '0008-0014-0025-0034-0036-0046-0048-0066',
    '0008-0014-0025-0034-0036-0046-0048-0066-0190-0378',
    '0008-0014-0025-0034-0036-0047-0049-0067',
    '0008-0014-0025-0034-0036-0047-0049-0067-0193',

    '0010-0017-0030-0040-0042-0053-0056-0078-0219-0437-0630-0705',
    '0010-0017-0030-0040-0042-0053-0056-0078-0219-0437-0630-0705-1017-1195-1439-1532-1828-2032-2196-2380-2482-2516',

    # Also exclude covid-related clusters b/c it's not worth the risk of mis-information
    '0010-0017-0031-0042-0045-0056-0060-0085-0238',
    '0010-0017-0031-0042-0045-0056-0060-0085-0238-0476-0689-0769-1113-1309-1571-1674-2004-2221-2400-2607-2717-2756',
]


# ==================
# keywords & subreddits to exclude
# ===
# for now, exclude city/state/region clusters because they provide a bad experience (no hierarchy)
_L_PLACE_RELATED_CLUSTERS_TO_EXCLUDE_FROM_FPRS_ = [
    '0007-0011-0019-0026-0027-0036-0037-0047-0135-0273-0402-0452',
    '0007-0011-0019-0026-0027-0036-0037-0047-0136-0274-0404-0454-0659-0777-0934-0994',

    # German cities
    '0007-0011-0019-0026-0026-0035-0036-0046-0132-0264-0391-0441-0640-0753-0907-0965-1167-1296-1396-1523-1590-1615',

]
# Exclude these subs either as seeds or recommendations
_L_COVID_TITLE_KEYWORDS_TO_EXCLUDE_FROM_FPRS_ = [
    'covid',
    'coronavirus',
]

_L_COVID_CLUSTERS_TO_EXCLUDE_FROM_FPRS_ = [
    '0010-0017-0031-0042-0045-0056-0060-0085-0238',
    '0010-0017-0031-0042-0045-0056-0060-0085-0238-0476',

]

_L_OTHER_CLUSTERS_TO_EXCLUDE_FROM_FPRS_ = [
    # NSFW?
    # '0001-0001-0001-0001-0001-0002-0002-0002-0004-0008-0011',
    # '0001-0001-0001-0001-0001-0002-0002-0002-0005-0009-0012-0014',
    # '0001-0001-0002-0002-0002-0003-0003-0003-0006-0011-0014-0016-0025-0030',
    # '0001-0001-0002-0002-0002-0003-0003-0003-0006',
    # '0001-0001-0003-0003-0003-0004-0004-0004-0010-0018-0025-0028-0046-0054-0061-0067-0089-0102-0113-0128',
    # '0001-0001-0003-0003-0003-0004-0005-0005-0012-0021-0031-0034-0052-0063',
    # '0001-0001-0003-0003-0003-0004-0005-0005-0013',
    # '0001-0001-0003-0003-0003-0004-0005-0005-0014-0031-0043-0051',
    # '0002-0002-0005-0005-0005-0006-0007-0007-0017',
    '0002-0004-0009-0010-0010-0012-0013-0015',

    '0004-0006-0011-0013-0013',  # teen-related cluster
    # '0005-0007-0012-0014-0014-0017-0018-0021-0061-0116-0173-0197',  # lgbtq maybe nsfw?

    # smoking & vaping
    '0008-0014-0025-0034-0036-0047-0049-0067-0193',
    '0008-0014-0025-0034-0036-0047-0049-0067-0192',

    # medical conditions (depression & drugs)
    '0008-0014-0025-0034-0036-0046-0048-0066-0189-0374-0541',

    # conspiracy & misinformation
    '0010-0017-0030-0040-0043-0054-0057-0082-0230-0456-0657-0735-1065-1254',

]

_L_SENSITIVE_SUBREDDITS_TO_EXCLUDE_FROM_FPRS_ = [
    # Conspiracy & covid
    # only list subs that don't fit a regex like:
    # .str.contains('covid')
    'conspiracy',
    'debatevaccines',
    'banned4life',
    'novavax_vaccine_talk',
    'coronadownunder',
    'lockdownskepticismau',
    'modernavaccine',
    'covidvaccinated',
    'vaxxhappened',
    'takethejab',
    'bidenisnotmypresident',
    'fightingfakenews',
    'wuhanvirus',
    'china_flu',
    'cvnews',
    'trumpvirus',
    'vaccinemandates',

    'lockdownsceptics',
    'ukantilockdown',
    'lockdownskepticismcan',
    'vaccinepassport',
    'churchofcovid',
    'quitefrankly',
    'daverubin',
    'breakingpoints',
    'breakingpointsnews',
    'banned4life',
    'timpool',
    'qult_headquarters',
    'parlerwatch',
    'askthe_donald',
    'benshapiro',
    'tucker_carlson',
    'trueanon',
    'beholdthemasterrace',

    'globallockdown',
    'nurembergtwo',
    'covidiots',
    'covidbc',
    'coronakritiker',
    'berlinvaccination',
    'covidmx',

    # diet-related subs
    '1500isplenty',
    '1200isplenty',
    '1200australia',
    'vegan1200isplenty',
    'edanonymemes',
    'diettea',
    '1200isjerky',
    '1200isfineiguessugh',
    'fatpeoplestories',
    'edanonymous',
    'cico',
    'loseit',
    'supermorbidlyobese',
    'safe_food',  # people who have anxiety about food/diets
    'bingeeatingdisorder',
    '1200isfineiguessugh',

    # drug-related
    'abv',
    'avb',
    'modareviewsnotbought',

    # medical
    'narcoticsanonymous',
    'depression_de',
    'autism',
    'autisminwomen',
    'aspergirls',
    'aspergers',
    'twoxadhd',
    'adhd_anxiety',
    'adhd',
    'adhdwomen',
    'psychmelee',
    'schematherapy',
    'cptsd',

    'breastcancer',
    'cancer',

    'babyloss',
    'tfmr_support',
    'endo',
    'endometriosis',
    'hysterectomy',
    'secondaryinfertility',
    'stilltrying',
    'ttc30',

    # other
    'fuck',
    'shincheonji',
    'cults',
    'unethicallifeprotips',
    'ausguns',
    'backdoorgore2',
    'exlldm',
    'extj',
    'rorochan_1999',

    'eroticrelatos',
    'vyralnews',

    # hunting is next to animal/nature subs...
    #  probably not a great experience for animal-lovers to see hunting stuff
    'huntingaustralia',

]


def flag_mature_clusters_to_exclude_from_qa(
        df_cluster_list: pd.DataFrame,
        col_new_cluster_val: str = 'cluster_label',
        col_exclude_from_qa: str = 'exclude_from_qa',
        val_exclude_from_qa: str = 'exclude from QA',
        list_known_nsfw_labels: list = None,
        depth_to_exclude: int = 10,
        sep: str = '-',
        verbose: bool = False,
) -> pd.Series:
    """besides a direct cluster match also apply new logic to exclude clusters that belong to
    a known clusters of known depth.
    e.g., if cluster at level-8 (or deeper) is knowns NSFW, exclude clusters that contain this stem
    """
    if list_known_nsfw_labels is None:
        list_known_nsfw_labels = _L_MATURE_CLUSTERS_TO_EXCLUDE_FROM_QA_
    val_keep_ = 'keep'

    # simple match can just do an exact match
    df_new = df_cluster_list[[col_new_cluster_val]].copy()

    df_new[col_exclude_from_qa] = np.where(
        df_cluster_list[col_new_cluster_val].isin(list_known_nsfw_labels),
        val_exclude_from_qa,
        val_keep_,
    )

    # find labels that are depth of 10 or more
    l_nsfw_deeper_than_n = [lab for lab in list_known_nsfw_labels if len(lab.split(sep)) >= depth_to_exclude]

    # then we can just iterate over the subs that were marked as "keep"
    # check each row independently
    ix_to_check = df_new[df_new[col_exclude_from_qa] != val_exclude_from_qa].index

    for ix_ in ix_to_check:
        old_label = df_new.loc[ix_, col_new_cluster_val]

        if len(old_label.split(sep)) > depth_to_exclude:
            for nsfw_root_ in l_nsfw_deeper_than_n:
                if old_label.startswith(nsfw_root_):
                    df_new.loc[ix_, col_exclude_from_qa] = val_exclude_from_qa

                    if verbose:
                        print(
                            f"Cluster excluded:"
                            f"\n  old:  {old_label}"
                            f"\n  root: {nsfw_root_}"
                        )
    return df_new[col_exclude_from_qa]


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
# Functions to clean up subs after QA (and get FPR outputs)
# ===
def get_subs_to_filter_as_df(
        sh_filter,
        cols_to_keep: Union[str, iter] = 'core',
) -> pd.DataFrame:
    """Get all the subreddits from central google sheet with all subs that
    are missing rating or were flagged to be re-rated.
    Assume input is an open sheet to prevent having to deal with oauth in this fxn.

    gsheet_key: str = '1JiDpiLa8RKRTC0ZxjLI0ISgtngAFWTEbsoYEoeeaVO8',
    """
    ws_filter_list = sh_filter.worksheets()

    # loop through each ws to get the subreddits so we can filter them out:
    l_all_sh_filters = list()

    for ws_ in tqdm(ws_filter_list):
        df_ = pd.DataFrame(
            sh_filter
            .get_worksheet(ws_._properties['index'])
            .get_all_records()
        )
        if len(df_) > 1:
            l_all_sh_filters.append(
                df_
                .rename(columns={k: k.strip().lower().replace(' ', '_') for k in df_.columns})
                .dropna(subset=['subreddit_name'])
            )
    df_subs_to_filter = (
        pd.concat(l_all_sh_filters)
        # make sure subs are lower case
        .assign(subreddit_name=lambda x: x['subreddit_name'].str.lower())
        .drop_duplicates(subset=['subreddit_name'])
    )

    if cols_to_keep == 'core':
        cols_to_keep = ['subreddit_name', 'category', 'request_type']
    elif cols_to_keep is None:
        cols_to_keep = df_subs_to_filter.columns

    df_subs_to_filter = df_subs_to_filter[cols_to_keep]
    print(f"\n{df_subs_to_filter.shape} <- df_subs to filter shape")

    return df_subs_to_filter


def remove_sensitive_clusters_and_subs(
        df_qa: pd.DataFrame,
        col_new_cluster_val: str = 'cluster_label',
        additional_subs_to_filter: iter = None,
        print_qa_check: bool = True,
        additional_qa_keywords: List[str] = None,
) -> pd.DataFrame:
    """Remove subreddits and clusters that have been flagged as sensitive
    Main use case: apply to df-qa to clean up subreddits to prepare for FPR output.

    It applies all the lists in this file and allows additional inputs as
    a list/array/series of subreddit_names to remove.
    """
    print(f"{df_qa.shape} <- Initial shape")
    df_qa_clean = df_qa.copy()

    # cluster-level
    for cluster_ in _L_PLACE_RELATED_CLUSTERS_TO_EXCLUDE_FROM_FPRS_:
        df_qa_clean = (
            df_qa_clean[~df_qa_clean[col_new_cluster_val].str.startswith(cluster_, na=False)]
        )
    print(f"{df_qa_clean.shape} <- Shape AFTER dropping place-clusters")

    # covid-related clusters
    for cluster_ in _L_COVID_CLUSTERS_TO_EXCLUDE_FROM_FPRS_:
        df_qa_clean = (
            df_qa_clean[~df_qa_clean[col_new_cluster_val].str.startswith(cluster_, na=False)]
        )
    print(f"{df_qa_clean.shape} <- Shape AFTER dropping covid-clusters")

    # medical & other clusters
    for cluster_ in _L_OTHER_CLUSTERS_TO_EXCLUDE_FROM_FPRS_:
        df_qa_clean = (
            df_qa_clean[~df_qa_clean[col_new_cluster_val].str.startswith(cluster_, na=False)]
        )
    print(f"{df_qa_clean.shape} <- Shape AFTER dropping sensitive clusters")

    # subreddit-level
    if additional_subs_to_filter is not None:
        df_qa_clean = (
            df_qa_clean[~df_qa_clean['subreddit_name'].isin(additional_subs_to_filter)]
        )
        print(f"{df_qa_clean.shape} <- Shape AFTER dropping flagged subs A")

    df_qa_clean = (
        df_qa_clean[~df_qa_clean['subreddit_name'].isin(_L_SENSITIVE_SUBREDDITS_TO_EXCLUDE_FROM_FPRS_)]
    )
    print(f"{df_qa_clean.shape} <- Shape AFTER dropping flagged subs B")

    # subreddit-name matches
    for word_ in _L_COVID_TITLE_KEYWORDS_TO_EXCLUDE_FROM_FPRS_:
        df_qa_clean = (
            df_qa_clean[~df_qa_clean['subreddit_name'].str.contains(word_, na=False)]
        )
    print(f"{df_qa_clean.shape} <- Shape AFTER dropping covid-related subs")

    print(f"{len(df_qa) - len(df_qa_clean):,.0f} <- Total subreddits removed")
    if print_qa_check:
        print(f"\nQA keyword subreddit checks:")
        print_subreddit_name_qa_checks(
            df_qa=df_qa_clean,
            additional_qa_keywords=additional_qa_keywords,
        )
    return df_qa_clean


def print_subreddit_name_qa_checks(
        df_qa: pd.DataFrame,
        additional_qa_keywords: List[str] = None,
) -> None:
    """Print subreddit_names that may contain sensitive keywords"""
    l_keywords_for_qa_ = [
        'coro', 'cov', 'vacc', 'vax',
        'lockdown', 'skeptic', 'fakenews', 'anon',
        '1200', '1500', 'diet', 'binge',
        'gore',
        'nsfw', 'xxx', 'onlyfans', 'fap', 'teen', 'thots',
        'anxi', 'depress', 'adhd', 'pill',
        'adh',
    ]
    if additional_qa_keywords is not None:
        l_keywords_for_qa_ = l_keywords_for_qa_ + additional_qa_keywords

    for k_ in l_keywords_for_qa_:
        list_ = df_qa[df_qa['subreddit_name'].str.contains(k_, na=False)]['subreddit_name'].to_list()
        if len(list_) > 0:
            print(f"  {list_}")
    print('')


def apply_qa_filters_for_fpr(
        df: pd.DataFrame,
        col_rated_e_latest: str = 'rated_e_latest',
        col_over_18_latest: str = 'over_18_latest',
        col_country_relevant: str = 'not_country_relevant',
        col_releveant_to_cluster: str = 'relevant_to_cluster/_other_subreddits_in_cluster',
        col_safe_to_show_in_cluster: str = 'safe_to_show_in_relation_to_cluster',
        col_allow_discovery_latest: str = 'allow_discovery_latest',
        print_qa_check: bool = True,
        additional_qa_keywords: List[str] = None,
) -> pd.DataFrame:
    """Apply expected filters to df"""
    mask_rated_e = df[col_rated_e_latest] == True
    mask_not_over_18 = df[col_over_18_latest] != 't'
    mask_relevant_to_country = df[col_country_relevant] != 'TRUE'  # do this in case there are nulls
    mask_relevant_to_cluster = df[col_releveant_to_cluster] == 'TRUE'
    mask_safe_in_cluster = df[col_safe_to_show_in_cluster] == 'TRUE'
    mask_allows_discovery = df[col_allow_discovery_latest] != 'f'

    mask_clean_for_fpr = (
            mask_rated_e &
            mask_not_over_18 &
            mask_relevant_to_country &
            mask_relevant_to_cluster &
            mask_safe_in_cluster &
            mask_allows_discovery
    )

    df_clean = df[mask_clean_for_fpr].copy()

    if print_qa_check:
        print(f"\nQA keyword subreddit checks:")
        print_subreddit_name_qa_checks(
            df_qa=df_clean,
            additional_qa_keywords=additional_qa_keywords,
        )

    print(f"{len(df):,.0f} <- Initial subreddit count")
    print(f"{mask_clean_for_fpr.sum():,.0f} <- Clean subreddits to use")
    print(f"{df_clean.shape} <- df subreddits to use for FPR")

    return df_clean


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
