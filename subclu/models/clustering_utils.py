"""
Utils for clustering.

sklearn doesn't have tools out of the box to introspect hierarchical clusters
and scipy's tools need a little tweaking (like these fxns).

reference:
- Describe different ways to use scipy's tools
    - https://joernhees.de/blog/2015/08/26/scipy-hierarchical-clustering-and-dendrogram-tutorial/
"""
import gc
import logging
from pathlib import Path
from typing import Union, Tuple, List

from matplotlib import pyplot as plt
from matplotlib import cm
import numpy as np
import pandas as pd
from tqdm import tqdm

from scipy.cluster.hierarchy import dendrogram

from ..utils.eda import reorder_array


def create_dynamic_clusters(
        df_labels: pd.DataFrame,
        agg_strategy: str = 'aggregate_small_clusters',
        min_subreddits_in_cluster: int = 5,
        l_cols_labels_input: list = None,
        col_new_cluster_val: str = 'cluster_label',
        col_new_cluster_name: str = 'cluster_label_k',
        col_new_cluster_val_int: str = 'cluster_label_int',
        col_new_cluster_prim_topic: str = 'cluster_majority_primary_topic',
        col_new_cluster_topic_mix: str = 'cluster_topic_mix',
        col_subreddit_topic_mix: str = 'subreddit_full_topic_mix',
        append_columns: bool = True,
        verbose: bool = False,
        l_ix: list = None,
        n_mix_start: int = 4,
        suffix_primary_topic_col: str = '_majority_primary_topic',
        suffix_new_topic_mix: str = '_topic_mix_nested',
        col_full_depth_mix_count: str = 'subreddit_full_topic_mix_count',
        l_cols_primary_topics: list = None,
        log_n_clusters_below_threshold: bool = False,
        tqdm_log_col_iterations: bool = True,
        # redo_orphans: bool = True,
        # orphan_increase: int = 2,
) -> pd.DataFrame:
    """For country to country clusters (DE to DE, FR to FR)
    we need to resize the clusters because some might be too big and others too small
    some might even be orphan.

    Use this function to create a new set of columns to use for FPRs
    and similar use cases.

    Args:
        df_labels:
        agg_strategy:
        min_subreddits_in_cluster:
        l_cols_labels_input:
        col_new_cluster_val:
        col_new_cluster_name:
        col_new_cluster_val_int:
        col_new_cluster_prim_topic:
        col_new_cluster_topic_mix:
            topic mix (concat primary topics) for a cluster
        col_subreddit_topic_mix:
            The deepest topic mix for a specific subreddit. Keep it so we can see it
            even if a sub ends up in a cluster that is shallow.
        append_columns:
        verbose:
        l_ix:
        n_mix_start:
        suffix_primary_topic_col:
        suffix_new_topic_mix:
        col_full_depth_mix_count:
        l_cols_primary_topics:
        log_n_clusters_below_threshold:
        tqdm_log_col_iterations:
            Whether to use tqdm to show column checks. Set to true to match previous behavior
            but set to false for v0.5.0 to prevent a wall of tqdm progress bars

    Returns:
        dataframe with new dynamic columns & nested label & topic columns
    """
    if l_cols_labels_input is None:
        l_cols_labels_input = [c for c in df_labels.columns if c.endswith('_label')]

    # some cols for the fxn to get the nested topic mix:
    if l_ix is None:
        l_ix = ['subreddit_id', 'subreddit_name']

    if l_cols_primary_topics is None:
        l_cols_primary_topics = sorted([
            c for c in df_labels.columns if all([c.startswith('k'), c.endswith(suffix_primary_topic_col)])
        ])

    # Create new cols that have zero-padding so we can concat and sort them
    l_cols_labels_new = [f"{c}_nested" for c in l_cols_labels_input]
    df_new_labels = df_labels[l_ix].copy()

    if verbose:
        logging.info(f"Concat'ing nested cluster labels...")
    # First convert the label vals [1, 5, 328] to string & apply zero padding to normalize them and make it
    #  easy to sort them as text
    df_new_labels[l_cols_labels_new] = df_labels[l_cols_labels_input].apply(lambda x: x.map("{:04.0f}".format))
    # Concat the values of the new columns so it's easier to tell depth of each cluster
    for i in range(len(l_cols_labels_new)):
        if i == 0:
            df_new_labels[l_cols_labels_new[-1]] = (
                df_new_labels[l_cols_labels_new[0]]
                .str.cat(
                    df_new_labels[l_cols_labels_new[1:]],
                    sep='-'
                )
            )
        else:
            df_new_labels[l_cols_labels_new[-i - 1]] = (
                df_new_labels[l_cols_labels_new[0]]
                .str.cat(
                    df_new_labels[l_cols_labels_new[1:-i]],
                    sep='-'
                )
            )

    if verbose:
        logging.info(f"Getting topic mix at different depths...")
    # Initialize nested topic mix columns. We can join on l_ix for any dynamic strategy
    df_prim_topic_mix_cols = get_primary_topic_mix_cols(
        df_labels=df_labels,
        l_cols_primary_topics=l_cols_primary_topics,
        n_mix_start=n_mix_start,
        suffix_primary_topic_col=suffix_primary_topic_col,
        suffix_new_topic_mix=suffix_new_topic_mix,
        col_new_cluster_name=col_new_cluster_name,
        col_new_cluster_prim_topic=col_new_cluster_prim_topic,
        col_full_depth_mix_count=col_full_depth_mix_count,
        l_ix=l_ix,
        verbose=verbose,
        tqdm_log_col_iterations=tqdm_log_col_iterations,
    )
    # use reset_index() "trick" so that we can keep the same index when using masks
    # to copy data between df_new_labels & df_labels
    df_new_labels = (
        df_new_labels
        .reset_index()
        .merge(
            df_prim_topic_mix_cols,
            how='left',
            on=l_ix,
        )
        .set_index('index')
    )
    l_cols_new_topic_mix = sorted([c for c in df_prim_topic_mix_cols.columns if c.endswith(suffix_new_topic_mix)])
    df_new_labels[col_subreddit_topic_mix] = df_new_labels[l_cols_new_topic_mix[-1]]
    del df_prim_topic_mix_cols

    # Default algo works from smallest cluster to highest cluster (bottom up)
    if agg_strategy == 'aggregate_small_clusters':
        if verbose:
            logging.info(f"Initializing values for strategy: {agg_strategy}")
        # initialize values for new columns (smallest cluster name & values)
        df_new_labels[col_new_cluster_val] = df_new_labels[l_cols_labels_new[-1]]
        df_new_labels[col_new_cluster_name] = l_cols_labels_new[-1].replace('_nested', '')
        df_new_labels[col_new_cluster_prim_topic] = df_labels[
            l_cols_labels_new[-1].replace('_label_nested', '_majority_primary_topic')
        ]
        df_new_labels[col_new_cluster_topic_mix] = df_new_labels[l_cols_new_topic_mix[-1]]

        iter_cols = sorted(l_cols_labels_new[:-1], reverse=True)
        if tqdm_log_col_iterations:
            # only use TQDM if verbose=True, otherwise we can get a long list of tqdm progress bars
            iter_cols = tqdm(iter_cols)
        if verbose:
            logging.info(f"  Looping to roll-up clusters from smallest to largest...")
        for c_ in iter_cols:
            if log_n_clusters_below_threshold:
                print(c_)
            c_name_new = c_.replace('_nested', '')
            col_update_prim_topic = c_.replace('_label_nested', '_majority_primary_topic')
            c_update_topic_mix_ = c_.replace('_label_nested', suffix_new_topic_mix)

            # find which current clusters are below threshold
            df_vc = df_new_labels[col_new_cluster_val].value_counts()
            dv_vc_below_threshold = df_vc[df_vc <= min_subreddits_in_cluster]
            if log_n_clusters_below_threshold:
                print(f"  {dv_vc_below_threshold.shape} <- Shape of clusters below threshold")

            # Replace cluster labels & names for current clusters that have too few subs in a cluster
            mask_subs_to_reassign = df_new_labels[col_new_cluster_val].isin(dv_vc_below_threshold.index)
            df_new_labels.loc[
                mask_subs_to_reassign,
                col_new_cluster_val
            ] = df_new_labels[mask_subs_to_reassign][c_]

            df_new_labels.loc[
                mask_subs_to_reassign,
                col_new_cluster_name
            ] = c_name_new

            df_new_labels.loc[
                mask_subs_to_reassign,
                col_new_cluster_topic_mix
            ] = df_new_labels[mask_subs_to_reassign][c_update_topic_mix_]

            # NOTE that this assignment will only work for sure if index of both dfs is the same
            df_new_labels.loc[
                mask_subs_to_reassign,
                col_new_cluster_prim_topic
            ] = df_labels[mask_subs_to_reassign][col_update_prim_topic]

        # if redo_orphans:
        #     # TODO(djb) this might be better done manually, though...
        #     # Try to reassign ONLY the clusters where we have orphans
        #     df_vc_orphans = df_vc[df_vc <= 1]
        #     cluster_id_orphans = df_vc_orphans.index

    elif agg_strategy == 'split_large_clusters':
        df_new_labels[col_new_cluster_val] = df_new_labels[l_cols_labels_new[1]]
        df_new_labels[col_new_cluster_name] = l_cols_labels_new[1].replace('_nested', '')
        df_new_labels[col_new_cluster_prim_topic] = df_labels[
            l_cols_labels_new[1].replace('_label_nested', '_majority_primary_topic')
        ]

        for c_ in sorted(l_cols_labels_new[1:]):
            if verbose:
                print(c_)
            c_name_new = c_.replace('_nested', '')
            col_update_prim_topic = c_.replace('_label_nested', '_majority_primary_topic')

            # find which current clusters are ABOVE threshold
            df_vc = df_new_labels[col_new_cluster_val].value_counts()
            # multiply min by 2 so that we only split up a cluster if we have a high chance
            #  of getting at least 2 clusters from it
            dv_vc_above_threshold = df_vc[df_vc > (2 * min_subreddits_in_cluster)]
            if log_n_clusters_below_threshold:
                print(f"  {dv_vc_above_threshold.shape} <- Shape of clusters ABOVE threshold")

            # Replace cluster labels & names for current clusters that have too few subs in a cluster
            mask_subs_to_reassign = df_new_labels[col_new_cluster_val].isin(dv_vc_above_threshold.index)
            df_new_labels.loc[
                mask_subs_to_reassign,
                col_new_cluster_val
            ] = df_new_labels[mask_subs_to_reassign][c_]

            df_new_labels.loc[
                mask_subs_to_reassign,
                col_new_cluster_name
            ] = c_name_new

            df_new_labels.loc[
                mask_subs_to_reassign,
                col_new_cluster_prim_topic
            ] = df_labels[mask_subs_to_reassign][col_update_prim_topic]
    else:
        l_expected_aggs = ['aggregate_small_clusters', 'split_large_clusters']
        raise NotImplementedError(f"Agg strategy not implemented: {agg_strategy}.\n"
                                  f"  Expected one of: {l_expected_aggs}")

    # create new col as int for label so we can add a color scale when doing QA
    df_new_labels[col_new_cluster_val_int] = (
        df_new_labels[col_new_cluster_val]
        .str[-4:].astype(int)
    )

    if append_columns:
        df_new_labels = df_labels.merge(
            df_new_labels,
            how='left',
            on=l_ix,
        ).copy()

    l_cols_to_front = [
        'subreddit_id',
        'subreddit_name',
        col_new_cluster_val_int,
        col_new_cluster_topic_mix,
        'primary_topic',
        'rating_short',
        col_subreddit_topic_mix,
        'rating_name',
        'over_18',

        'geo_relevance_default',
        'relevance_percent_by_subreddit',
        'relevance_percent_by_country_standardized',
        'b_users_percent_by_subreddit',
        'e_users_percent_by_country_standardized',
        'd_users_percent_by_country_rank',

        'model_sort_order',
        'posts_for_modeling_count',
        col_new_cluster_val,
        col_new_cluster_name,
        col_new_cluster_prim_topic,
        'c_users_percent_by_country',
        'users_in_subreddit_from_country_l28',
        'total_users_in_country_l28',
        'total_users_in_subreddit_l28',
    ]
    l_cols_to_front = [c for c in l_cols_to_front if c in df_new_labels.columns]
    df_new_labels = df_new_labels[
        reorder_array(l_cols_to_front, df_new_labels.columns)
    ]

    if verbose:
        logging.info(f"{df_new_labels.shape} <- output shape")
    return df_new_labels


def create_dynamic_clusters_clean(
        df_dynamic_raw: pd.DataFrame,
        col_model_sort_order: str = 'model_sort_order',
        col_new_cluster_val: str = 'cluster_label',
        col_new_cluster_val_int: str = 'cluster_label_int',
        col_new_cluster_name: str = 'cluster_label_k',
        col_new_cluster_topic_mix: str = 'cluster_topic_mix',
        col_subreddit_topic_mix: str = 'subreddit_full_topic_mix',
        col_subs_in_cluster_count: str = 'subs_in_cluster_count',
        col_list_cluster_names: str = 'list_cluster_subreddit_names',
        col_new_cluster_prim_topic: str = 'cluster_majority_primary_topic',
        col_exclude_from_qa: str = 'exclude_from_qa',
        val_exclude_from_qa: str = 'exclude from QA',
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """After we add some columns to identify NSFW subs run this fxn
    to re-order columns & exclude subs that have been marked as NSFW
    """
    # create new column for reddit link, makes QA easier
    col_link_to_sub = 'link_to_sub'
    link_prefix_ = 'www.reddit.com/r/'
    df_dynamic_raw[col_link_to_sub] = link_prefix_ + df_dynamic_raw['subreddit_name']

    # create lookup for rating col, might need to refresh it later in google sheets, though
    col_rated_e = 'rated E'
    # need to make it boolean for checkbox thing to work
    df_dynamic_raw[col_rated_e] = (
            df_dynamic_raw['rating_short'] == 'E'
    )

    # Set expected col order
    l_cols_clean_final_for_qa = [
        'subreddit_id',
        'subreddit_name',
        col_new_cluster_val_int,
        col_new_cluster_topic_mix,

        # insert inputs for QA cols
        'not country relevant',
        col_rated_e,
        'relevant to cluster/ other subreddits in cluster',
        'safe to show in relation to cluster',

        # cols for notes
        'country relevance notes',
        'rating or cluster notes',
        #  # 'cluster relevance notes',
        col_link_to_sub,

        col_subs_in_cluster_count,
        col_list_cluster_names,

        # why did a sub get marked as geo or culturally relevant?
        #  can use them to sort
        'posts_for_modeling_count',
        'users_l7',
        'geo_relevance_default',
        'relevance_percent_by_subreddit',
        'relevance_percent_by_country_standardized',
        'b_users_percent_by_subreddit',
        'e_users_percent_by_country_standardized',

        'allow_discovery',
        'rating_name',

        'over_18',
        'rating_short',
        'primary_topic',
        col_subreddit_topic_mix,

        'd_users_percent_by_country_rank',

        'c_users_percent_by_country',
        'users_in_subreddit_from_country_l28',

        col_model_sort_order,
        col_new_cluster_val,
        col_new_cluster_name,
        col_new_cluster_prim_topic,
    ]
    # Add other cols at the end if they're not explicitly added
    cols_to_check = [c for c in df_dynamic_raw.columns if not c.startswith('k_')]
    l_cols_clean_final_for_qa = (
            l_cols_clean_final_for_qa +
            [c for c in cols_to_check if c not in l_cols_clean_final_for_qa]
    )

    # copy existing columns from raw +
    l_cols_clean_existing = [c for c in l_cols_clean_final_for_qa if c in df_dynamic_raw.columns]
    l_cols_clean_new = [c for c in l_cols_clean_final_for_qa if c not in df_dynamic_raw.columns]

    # reorder cols in raw DF so it's easier to switch between the two tabs
    df_dynamic_clean = df_dynamic_raw[
        reorder_array(l_cols_clean_existing, df_dynamic_raw.columns)
    ]

    # only copy subs that aren't in excluded clusters!!
    df_dynamic_clean = (
        df_dynamic_raw
        [df_dynamic_raw[col_exclude_from_qa] != val_exclude_from_qa]
        [l_cols_clean_existing]
        .copy()
    )
    # Add new columns and initialize them with empty strings
    df_dynamic_clean[l_cols_clean_new] = ''

    # re-order & rename the columns so their easier to see in google sheets
    # Sorty by cluster label b/c sometimes a sub won't be clustered dynamically next to closest neighbors!
    # Also sort by users_l7 so that we know which subreddits are the most popular/valuable per cluster
    l_cols_sort_by_ = [col_new_cluster_val, 'users_l7']
    sort_ascending_ = [True, False]
    df_dynamic_clean = (
        df_dynamic_clean[l_cols_clean_final_for_qa]
        .sort_values(by=l_cols_sort_by_, ascending=sort_ascending_)
        # do final renaming only when saving, otherwise, it's a pain to adjust or look things up?
        .rename(columns={c: c.replace('_', ' ') for c in l_cols_clean_final_for_qa[:]})
    )
    df_dynamic_raw = (
        df_dynamic_raw
        .sort_values(by=l_cols_sort_by_, ascending=sort_ascending_)
    )

    return df_dynamic_raw, df_dynamic_clean


def get_primary_topic_mix_cols(
        df_labels: pd.DataFrame,
        l_cols_primary_topics: list = None,
        n_mix_start: int = 4,
        suffix_primary_topic_col: str = '_majority_primary_topic',
        suffix_new_topic_mix: str = '_topic_mix_nested',
        col_new_cluster_name: str = 'cluster_label_k',
        col_new_cluster_prim_topic: str = 'cluster_majority_primary_topic',
        col_full_depth_mix_count: str = 'subreddit_full_topic_mix_count',
        l_ix: list = None,
        verbose: bool = False,
        tqdm_log_col_iterations: bool = True,
) -> pd.DataFrame:
    """For a given depth of the list of primary topic columns, return them
    in a single column as a string that combines all the nested topics without repeats

    General idea:
    get subreddit ID as index & stack all the cluster majority primary topics as a row (long)
    exclude the first N primary topics b/c those will be broad & noisy
    then for each  final cluster (last row),
    - drop duplicates (keep first)
    - groupby subreddit ID & get a list of the new primary topics (w/o dupes)
    - convert primary topics to string (instead of list)
    - assign the list to a new column

    Only start appending after first few cols, otherwise we can get weird results
    b/c labels change too quickly & get dominated by largest primary topics as we decrease k
    """
    if l_ix is None:
        l_ix = ['subreddit_id', 'subreddit_name']
    if l_cols_primary_topics is None:
        l_cols_primary_topics = sorted([
            c for c in df_labels.columns if all([c.startswith('k'), c.endswith(suffix_primary_topic_col)])
        ])

    l_cols_new_topic_mix = [c.replace(suffix_primary_topic_col, suffix_new_topic_mix) for c in l_cols_primary_topics]

    # For the first N cols, primary topic is the same as the input primary topic
    if verbose:
        logging.info(f"  Assigning base topic mix cols")
    df_topic_mix_final = df_labels[l_ix].copy()
    df_topic_mix_final[l_cols_new_topic_mix[:n_mix_start + 1]] = (
        df_labels[l_cols_primary_topics[:n_mix_start + 1]].copy()
    )

    # =====================
    # Now get the deepest topic first
    # ===
    # This way we know which ones stay the same, so we don't need to loop a bunch
    # NOTE: slices & indexing do slightly different things
    #  so I need to add or subtract by one to get the correct col name
    if verbose:
        logging.info(f"  Creating deepest base topic mix col...")
    ix_max_ = len(l_cols_primary_topics)  # max for slice = len(cols)
    ix_col_max_ = ix_max_ - 1  # max to get final col name = len(cols) - 1
    col_topic_mix_deep = l_cols_new_topic_mix[-1]

    df_topic_mix_deepest = (
        df_labels
        [l_ix + l_cols_primary_topics[n_mix_start:ix_col_max_]]
        .set_index(l_ix, append=False)
        .stack()
        .to_frame()
        .reset_index()
        .rename(columns={
            'level_0': 'index',
            'level_1': col_new_cluster_name,  # level_num depends on whether we drop index or include name
            'level_2': col_new_cluster_name,
            0: col_new_cluster_prim_topic
        }
        )

        # sort by sub + majority topic level so we ensure order before dropping dupes
        .sort_values(by=['subreddit_id', col_new_cluster_name], ascending=True)

        # drop duplicates for sub + topic
        .drop_duplicates(subset=['subreddit_id', col_new_cluster_prim_topic], keep='first')

        # aggregate by subreddit & get a list of the topics
        .groupby(l_ix)
        .agg(
            **{
                col_topic_mix_deep: (col_new_cluster_prim_topic, list),
                col_full_depth_mix_count: (col_new_cluster_prim_topic, 'nunique'),
            }
        )
    )
    # Convert the list column to a string
    #  Use ' | '.join() instead of string replace to avoid replacing comas in primary topics
    df_topic_mix_deepest[col_topic_mix_deep] = (
        df_topic_mix_deepest[col_topic_mix_deep]
        .apply(lambda x: ' | '.join(x))
        .astype(str)
        .str.replace("'", "")
    )

    # merge base + deeepest
    df_topic_mix_final = df_topic_mix_final.merge(
        df_topic_mix_deepest,
        how='outer',
        on=l_ix,
    )

    # ===
    # Iterate through other subs that have mixed topics
    mask_constant_topic_mix = df_topic_mix_final[col_full_depth_mix_count] == 1
    mask_subreddits_with_multiple_topics = (
        df_labels['subreddit_id'].isin(
            df_topic_mix_final[~mask_constant_topic_mix]['subreddit_id']
        )
    )

    # To optimize later, we could create columns in deepest to newest and only
    #  calculate values for those that have multiple values... but for now
    #  just iterate through all of them
    # had to mess with +/- 1 here so that we don't calculate the same col twice

    iter_cols = list(np.arange(n_mix_start + 1, ix_max_ - 1))[::-1]
    if tqdm_log_col_iterations:
        # only use TQDM if verbose=True, otherwise we can get a long list of tqdm progress bars
        iter_cols = tqdm(iter_cols)
    if verbose:
        logging.info(f"  Iterating through additional subs with multiple topics...")
    for ix_col_ in iter_cols:
        ix_slice_end_ = ix_col_ + 1
        col_topic_mix_iter_ = l_cols_new_topic_mix[ix_col_]

        df_mix_iter_ = (
            df_labels[mask_subreddits_with_multiple_topics]
            [l_ix + l_cols_primary_topics[n_mix_start:ix_slice_end_]]
            .set_index(l_ix, append=False)
            .stack()
            .to_frame()
            .reset_index()
            .rename(
                columns={
                    'level_0': 'index',
                    'level_1': col_new_cluster_name,  # level_num depends on whether we drop index or include name
                    'level_2': col_new_cluster_name,
                    0: col_new_cluster_prim_topic
                }
            )
            # sort by sub + majority topic level so we ensure order before dropping dupes
            .sort_values(by=['subreddit_id', col_new_cluster_name], ascending=True)

            # drop duplicates for sub + topic
            .drop_duplicates(subset=['subreddit_id', col_new_cluster_prim_topic], keep='first')

            # aggregate by subreddit & get a list of the topics
            .groupby(l_ix)
            .agg(
                **{
                    col_topic_mix_iter_: (col_new_cluster_prim_topic, list),
                }
            )
        )

        # Convert the list column to a string
        #  Use ' | '.join() instead of string replace to avoid replacing comas in primary topics
        df_mix_iter_[col_topic_mix_iter_] = (
            df_mix_iter_[col_topic_mix_iter_]
            .apply(lambda x: ' | '.join(x))
            .astype(str)
            .str.replace("'", "")
        )

        # merge base + current iter
        df_topic_mix_final = df_topic_mix_final.merge(
            df_mix_iter_,
            how='outer',
            on=l_ix,
        )
    del df_mix_iter_

    # ===
    # Finally, assign the topics for subs that have a constant topic
    df_topic_mix_final.loc[
        mask_constant_topic_mix,
        l_cols_new_topic_mix[n_mix_start + 1: -1]
    ] = df_topic_mix_final[mask_constant_topic_mix][col_topic_mix_deep]

    gc.collect()
    return df_topic_mix_final[
        reorder_array(l_ix + [col_full_depth_mix_count] + l_cols_new_topic_mix,
                      df_topic_mix_final.columns)
    ]


def reshape_df_to_get_1_cluster_per_row(
        df_labels: pd.DataFrame,
        col_counterpart_count: str = 'counterpart_count',
        col_list_cluster_names: str = 'list_cluster_subreddit_names',
        col_list_cluster_ids: str = 'list_cluster_subreddit_ids',
        col_new_cluster_val: str = 'cluster_label',
        col_new_cluster_val_int: str = 'cluster_label_int',
        col_new_cluster_name: str = 'cluster_label_k',
        col_new_cluster_topic: str = 'cluster_topic_mix',
        get_one_column_per_sub_id: bool = False,
        l_groupby_cols: iter = None,
        agg_subreddit_ids: bool = True,
        l_sort_cols: str = None,
        verbose: bool = False,
) -> pd.DataFrame:
    """Take a df with clusters and reshape it so it's easier to review
    by taking a long df (1 row=1 subredddit) and reshaping so that
    1=row = 1 cluster
    """
    if l_groupby_cols is None:
        l_groupby_cols = [col_new_cluster_val, col_new_cluster_name, col_new_cluster_topic, col_new_cluster_val_int]
        l_groupby_cols = [c for c in l_groupby_cols if c in df_labels]

    if l_sort_cols is None:
        l_sort_cols = [col_new_cluster_val]

    d_aggs = {
        col_counterpart_count: ('subreddit_id', 'nunique'),
        col_list_cluster_names: ('subreddit_name', list),
    }
    if agg_subreddit_ids:
        d_aggs[col_list_cluster_ids] = ('subreddit_id', list)

    df_cluster_per_row = (
        df_labels
        .groupby(l_groupby_cols)
        .agg(
            **d_aggs
        )
        .sort_values(by=l_sort_cols, ascending=True)
        .reset_index()
    )
    print(f"{df_cluster_per_row.shape}  <- df.shape")

    if get_one_column_per_sub_id:
        # Convert the list of subs into a df & merge back with original sub (each sub should be in a new column)
        df_cluster_per_row = (
            df_cluster_per_row
            .merge(
                pd.DataFrame(df_cluster_per_row[col_list_cluster_ids].to_list()).fillna(''),
                how='left',
                left_index=True,
                right_index=True,
            )
        )

    # when convertion to JSON for gspread, it's better to conver the list into a string
    # and to remove the brackets
    for col_list_ in [col_list_cluster_names, col_list_cluster_ids]:
        try:
            # Treating as a string is faster than .apply() to process each item in list
            #    .apply(lambda x: ', '.join(x))
            df_cluster_per_row[col_list_] = (
                df_cluster_per_row[col_list_]
                .astype(str)
                .str[1:-1]
                .str.replace("'", "")
            )
        except KeyError:
            pass

    return df_cluster_per_row


def convert_distance_or_ab_to_list_for_fpr(
        df: pd.DataFrame,
        convert_to_ab: bool = True,
        col_counterpart_count: str = 'counterpart_count',
        col_list_cluster_names: str = 'list_cluster_subreddit_names',
        col_list_cluster_ids: str = 'list_cluster_subreddit_ids',
        l_cols_for_seeds: List[str] = None,
        l_cols_for_clusters: List[str] = None,
        col_new_cluster_val: str = 'cluster_label',
        col_new_cluster_name: str = 'cluster_label_k',
        col_new_cluster_prim_topic: str = 'cluster_majority_primary_topic',
        col_model_sort_order: str = 'model_sort_order',
        col_primary_topic: str = 'primary_topic',
        col_sort_by: str = None,
        verbose: bool = False,
) -> pd.DataFrame:
    """Take a df_distances or df_ab and reshape it to get output needed for an FPR
    TODO(djb): this might be better as a method for a cluster class... right now each function feels
      disjointed and I need to pass the same column names back and forth a few times
    """
    if convert_to_ab:
        if l_cols_for_seeds is None:
            l_cols_for_seeds = [
                'subreddit_id', 'subreddit_name',
                col_model_sort_order, col_primary_topic,
                col_new_cluster_val, 'cluster_label_k', col_new_cluster_prim_topic,
            ]
        if l_cols_for_clusters is None:
            l_cols_for_clusters = [
                'subreddit_id', 'subreddit_name',
                col_new_cluster_val
            ]
        if col_sort_by is None:
            col_sort_by = col_model_sort_order
        else:
            if col_sort_by not in l_cols_for_seeds:
                l_cols_for_seeds.append(col_sort_by)

        if verbose:
            print(l_cols_for_seeds)
            print(l_cols_for_clusters)

        df_ab = (
            df[l_cols_for_seeds].copy()
            .merge(
                df[l_cols_for_clusters],
                how='left',
                on=[col_new_cluster_val],
                suffixes=('_seed', '_cluster')
            )
        )
        if verbose:
            print(f"  {df_ab.shape} <- df_ab.shape raw")
        # Set name of columns to be used for aggregation
        col_sub_name_a = 'subreddit_name_seed'
        col_sub_id_a = 'subreddit_id_seed'
        col_sub_name_b = 'subreddit_name_cluster'
        col_sub_id_b = 'subreddit_id_cluster'
        # Remove matches to self b/c that makes no sense as a recommendation
        df_ab = df_ab[
            df_ab[col_sub_id_a] != df_ab[col_sub_id_b]
            ]
        print(f"  {df_ab.shape} <- df_ab.shape after removing matches to self")
    else:
        raise NotImplementedError(f"reshape for df_distances not implemented")

    # update default groupby cols with input seeds & col_sort_by
    l_groupby_cols = [
        col_model_sort_order, col_sub_id_a, col_sub_name_a,
        col_new_cluster_val, col_new_cluster_name
    ]
    for c_ in l_cols_for_seeds:
        if c_ in ['subreddit_name', 'subreddit_id'] + l_groupby_cols:
            continue
        else:
            l_groupby_cols.append(c_)
    if col_sort_by not in l_groupby_cols:
        l_groupby_cols.append(col_sort_by)
    if verbose:
        print(f"  Groupby cols:\n    {l_groupby_cols}")

    df_a_to_b_list = (
        df_ab
        .groupby(l_groupby_cols)
        .agg(
            **{
                col_counterpart_count: (col_sub_id_b, 'nunique'),
                col_list_cluster_names: (col_sub_name_b, list),
                col_list_cluster_ids: (col_sub_id_b, list),
            }
        )
        .reset_index()
        # .rename(columns={'subreddit_name_a': 'subreddit_name_de',
        #                  'subreddit_id_a': 'subreddit_id_de'})
        .sort_values(by=[col_sort_by, ], ascending=True)
        .drop([col_model_sort_order], axis=1)
    )

    # when converting to JSON for gspread it's better to convert the list into a string
    # and to remove the brackets. Otherwise we can get errors.
    for c_ in [col_list_cluster_names, col_list_cluster_ids]:
        df_a_to_b_list[c_] = (
            df_a_to_b_list[c_]
            .astype(str)
            .str[1:-1]
            .str.replace("'", "")
        )
    print(f"  {df_a_to_b_list.shape} <- df_a_to_b.shape")
    return df_a_to_b_list


def create_linkage_for_dendrogram(model) -> pd.DataFrame:
    """
    Create linkage matrix from an Sklearn model (e.g., AgglomerativeCluster)
    We can use this matrix to plot a dendogram and create cluster labels using fcluster.
    """
    # create the counts of samples under each node
    counts = np.zeros(model.children_.shape[0])
    n_samples = len(model.labels_)
    for i, merge in enumerate(model.children_):
        current_count = 0
        for child_idx in merge:
            if child_idx < n_samples:
                current_count += 1  # leaf node
            else:
                current_count += counts[child_idx - n_samples]
        counts[i] = current_count

    linkage_matrix = pd.DataFrame(
        np.column_stack(
            [model.children_,
             model.distances_,
             counts]
        ),
        columns=['children_0', 'children_1', 'distance', 'count'],
    ).astype({
        'children_0': int,
        'children_1': int,
        'distance': float,
        'count': int,
    })

    return linkage_matrix


def fancy_dendrogram(
        Z: Union[pd.DataFrame, np.ndarray],
        max_d: float = None,
        annotate_above: float = 0,
        plot_title: str = 'Hierarchical Clustering Dendrogram (truncated)',
        xlabel: str = 'item index OR (cluster size)',
        ylabel: str = 'distance',
        dist_fontsize: float = 13,
        save_path: Union[str, Path] = None,
        **kwargs
):
    """Wrapper around dendogram diagram that adds distances & cut off
    TODO(djb): fix orientation right or left:
        - The axis labels are flipped
        - The distances are in the wront place
    """
    if max_d and 'color_threshold' not in kwargs:
        kwargs['color_threshold'] = max_d

    fig = plt.figure(figsize=(14, 8))
    ddata = dendrogram(Z, **kwargs)

    if not kwargs.get('no_plot', False):
        plt.title(plot_title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        for i, d, c in zip(ddata['icoord'], ddata['dcoord'], ddata['color_list']):
            x = 0.5 * sum(i[1:3])
            y = d[1]
            if y > annotate_above:
                plt.plot(x, y, 'o', c=c)
                # original format: "%.3g"
                plt.annotate(f"{y:.1f}", (x, y), xytext=(0, -5),
                             textcoords='offset points',
                             fontsize=dist_fontsize,
                             va='top', ha='center')
        if max_d:
            plt.axhline(y=max_d, c='k')

    if save_path is not None:
        plt.savefig(
            save_path,
            dpi=200, bbox_inches='tight', pad_inches=0.2
        )
    return ddata


def plot_elbow_and_get_k(
        Z: Union[pd.DataFrame, np.ndarray],
        n_clusters_to_check: int = 500,
        figsize: tuple = (16, 9),
        plot_title: str = 'Cluster Distances & Optimal k',
        xlabel: str = 'Number of clusters (k)',
        ylabel1: str = 'Distance between clusters',
        ylabel2: str = 'Acceleration of distances',
        col_optimal_k: str = 'optimal_k_for_interval',
        save_path: Union[str, Path] = None,
        return_optimal_ks: bool = False,
        xlim: tuple = (-4, 104),
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, dict]]:
    """Use 'elbow' method to get an optimal value of k-clusters"""
    # create a 2ndary y-axis because the acceleration units tend to be
    #  much smaller than distance, which makes it hard to see the differences
    fig, ax1 = plt.subplots(sharex='all', figsize=figsize)
    ax2 = ax1.twinx()

    try:
        last = Z[-n_clusters_to_check:, 2]
    except TypeError:
        last = Z.to_numpy()[-n_clusters_to_check:, 2]

    # reverse order of distances (low-k first)
    last_rev = last[::-1]
    idxs = np.arange(1, len(last) + 1)
    ax1.plot(idxs, last_rev, label='distance')

    acceleration = np.diff(last, 2)  # 2nd derivative of the distances
    acceleration_rev = acceleration[::-1]

    # create a df to track k, acceleration, and best-k in n interval
    df_accel = (
        pd.DataFrame(
            {'acceleration': acceleration_rev}
        )
        .reset_index()
        .assign(index=lambda x: x['index'] + 2)
        .rename(columns={'index': 'k'})
    )

    k_intervals = [
        # (2, 10),  # This one is so generic it's kind of useless
        (10, 20),
        (20, 40),
        (40, 50),
        (50, 60),
        (60, 70),
        (70, 80),
        (80, 100),

        (100, 250),
        (250, 500),
        (500, 750),
        (750, 1000),

        (1000, 1350),
        (1350, 1700),
        (1700, 2000),

        (2000, 2350),
        (2350, 2700),
        (2700, 3000),

        (3000, 3200),
        (3400, 3600),
        (3600, 3800),
        (3800, 3900),
        (3900, 4000),
        (4000, 4200),
    ]
    k_intervals_below_xlim = len([tup_ for tup_ in k_intervals if tup_[1] <= xlim[1]])
    viridis = cm.get_cmap('viridis', k_intervals_below_xlim)

    d_optimal_k = dict()
    for i, k_tup_ in enumerate(k_intervals):
        # there seems to be a bug where "between" will always match both edges of the boundary
        #  so we need to manually reduce boundaries ourselves *sigh*
        k_min = k_tup_[0]
        k_max = k_tup_[1] - 1
        mask_interval_coT = df_accel['k'].between(k_min, k_max, inclusive='both')
        interval_name = f"{k_tup_[0]:04d}_to_{k_tup_[1]:04d}"

        try:
            df_accel.loc[
                (df_accel.index == df_accel[mask_interval_coT]['acceleration'].idxmax()),
                col_optimal_k
            ] = interval_name

            k_ = df_accel.loc[
                (df_accel.index == df_accel[mask_interval_coT]['acceleration'].idxmax()),
                'k'
            ].values[0]

            # only add values to optimal dict if k is within the range we're searching
            d_optimal_k[interval_name] = dict()
            d_optimal_k[interval_name]['k'] = int(k_)  # convert to int b/c np.int can create errors
            d_optimal_k[interval_name]['col_prefix'] = f"k{k_:04d}"

            if k_tup_[1] <= xlim[1]:
                plt.axvline(x=k_, linestyle="--", label=f"k={k_}", color=viridis(i / k_intervals_below_xlim))
        except Exception as e:
            logging.warning(f"{e}")

    plt.title(plot_title)
    ax1.set_xlabel(xlabel)
    ax1.set_ylabel(ylabel1)

    ax2.plot(idxs[:-2] + 1, acceleration_rev, label='acceleration', color='orange')
    ax2.set_ylabel(ylabel2)

    # set xlim at lower bound than ~500 clusters because the scale makes comparing them useless
    ax2.set_xlim(xlim)
    ax1.legend(loc='upper left', bbox_to_anchor=(1.06, .94))
    ax2.legend(loc='upper left', bbox_to_anchor=(1.06, .84))

    # change order - make sure distance is above acceleration
    ax1.set_zorder(ax2.get_zorder() + 1)
    ax1.set_frame_on(False)  # prevents ax1 from hiding ax2

    if save_path is not None:
        plt.savefig(
            save_path,
            dpi=200, bbox_inches='tight', pad_inches=0.2
        )
    # NOTE: if you plt.show() before saving, plt will create a new fig and won't be able to
    #  save the figure

    if return_optimal_ks:
        return df_accel, d_optimal_k
    else:
        return df_accel


# def calculate_metrics_with_ground_truth(
# ):
#     """"""
#     d_df_crosstab_labels = dict()
#     d_metrics = dict()
#     val_fill_pred_nulls = 'Meta/Reddit'
#
#     l_cols_ground_truth = [
#         # 'rating_name',
#         'primary_topic',
#     ]
#
#     df_labels_coF_meta = df_labels_coF.merge(
#         df_subs[l_ix_sub + l_cols_ground_truth],
#         how='left',
#         on=l_ix_sub,
#     ).copy()
#
#     l_cols_predicted = list()
#
#     # for interval_ in tqdm(intervals_to_test):
#     for interval_ in df_accel_coF[col_optimal_k].dropna().unique():
#         print(f"=== Interval: {interval_} ===")
#         col_cls_labels = f"{interval_}_labels"
#         d_df_crosstab_labels[col_cls_labels] = dict()
#         d_metrics[col_cls_labels] = dict()
#
#         for c_tl in l_cols_ground_truth:
#             # For some reason the nulls in this table are the string 'null'! ugh
#             mask_not_null_gt = ~(
#                     (df_labels_coF_meta[c_tl].isnull()) |
#                     (df_labels_coF_meta[c_tl] == 'null')
#             )
#             # print(f"  Nulls: {(~mask_not_null_gt).sum():,.0f}")
#             d_df_crosstab_labels[col_cls_labels][c_tl] = pd.crosstab(
#                 df_labels_coF_meta[mask_not_null_gt][col_cls_labels],
#                 df_labels_coF_meta[mask_not_null_gt][c_tl]
#             )
#
#             # Create new predicted column
#             col_pred_ = f"{interval_}-predicted-{c_tl}"
#             l_cols_predicted.append(col_pred_)
#             df_labels_coF_meta = df_labels_coF_meta.merge(
#                 (
#                     d_df_crosstab_labels[col_cls_labels][c_tl]
#                         # .drop('null', axis=1)
#                         .idxmax(axis=1)
#                         .to_frame()
#                         .rename(columns={0: col_pred_})
#                 ),
#                 how='left',
#                 left_on=col_cls_labels,
#                 right_index=True,
#             )
#
#             # Should be rare, but fill just in case?
#             # df_labels_coF_meta[col_pred_] = df_labels_coF_meta[col_pred_].fillna(val_fill_pred_nulls)
#
#             # =====================
#             # Calculate metrics:
#             # ===
#             #         print(
#             #             classification_report(
#             #                 y_true=df_labels_coF_meta[mask_not_null_gt][c_tl],
#             #                 y_pred=df_labels_coF_meta[mask_not_null_gt][col_pred_],
#             #                 zero_division=0,
#             #             )
#             #         )
#             for m_name, metric_ in d_metrics_and_names.items():
#                 d_metrics[col_cls_labels][c_tl] = dict()
#                 try:
#                     d_metrics[col_cls_labels][c_tl][m_name] = metric_(
#                         y_true=df_labels_coF_meta[mask_not_null_gt][c_tl],
#                         y_pred=df_labels_coF_meta[mask_not_null_gt][col_pred_],
#                     )
#                 except TypeError:
#                     d_metrics[col_cls_labels][c_tl][m_name] = metric_(
#                         labels_true=df_labels_coF_meta[mask_not_null_gt][c_tl],
#                         labels_pred=df_labels_coF_meta[mask_not_null_gt][col_pred_],
#                     )
#                 print(f"  Metric {m_name}: {d_metrics[col_cls_labels][c_tl][m_name]:,.4f}")


#
# ~ fin
#
