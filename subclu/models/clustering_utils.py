"""
Utils for clustering.

sklearn doesn't have tools out of the box to introspect hierarchical clusters
and scipy's tools need a little tweaking (like these fxns).

reference:
- Describe different ways to use scipy's tools
    - https://joernhees.de/blog/2015/08/26/scipy-hierarchical-clustering-and-dendrogram-tutorial/
"""
import logging
from pathlib import Path
from typing import Union, Tuple, List

from matplotlib import pyplot as plt
from matplotlib import cm
import numpy as np
import pandas as pd

from scipy.cluster.hierarchy import dendrogram

from ..utils.eda import reorder_array


def create_dynamic_clusters(
        df_labels: pd.DataFrame,
        agg_strategy: str = 'aggregate_small_clusters',
        min_subreddits_in_cluster: int = 5,
        l_cols_labels_input: list = None,
        col_new_cluster_val: str = 'cluster_label',
        col_new_cluster_name: str = 'cluster_label_k',
        col_new_cluster_prim_topic: str = 'cluster_majority_primary_topic',
        append_columns: bool = True,
        verbose: bool = False,
        redo_orphans: bool = True,
        orphan_increase: int = 2,
) -> pd.DataFrame:
    """For country to country clusters (DE to DE, FR to FR)
    we need to resize the clusters because some might be too big and others too small
    some might even be orphan.

    Use this function to create a new set of columns to use for FPRs
    and similar use cases.
    """
    if l_cols_labels_input is None:
        l_cols_labels_input = [c for c in df_labels.columns if c.endswith('_label')]

    # Create new cols that have zero-padding so we can concat and sort them
    l_cols_labels_new = [f"{c}_nested" for c in l_cols_labels_input]
    # print(l_cols_labels_input)
    # print(l_cols_labels_new)
    df_new_labels = pd.DataFrame(index=df_labels.index)
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

    # Default algo works from smallest cluster to highest cluster (bottom up)
    if agg_strategy == 'aggregate_small_clusters':
        # print(f"initial label: {l_cols_labels_new[-1]}")
        df_new_labels[col_new_cluster_val] = df_new_labels[l_cols_labels_new[-1]]
        df_new_labels[col_new_cluster_name] = l_cols_labels_new[-1].replace('_nested', '')
        df_new_labels[col_new_cluster_prim_topic] = df_labels[
            l_cols_labels_new[-1].replace('_label_nested', '_majority_primary_topic')
        ]

        for c_ in sorted(l_cols_labels_new[:-1], reverse=True):
            if verbose:
                print(c_)
            c_name_new = c_.replace('_nested', '')
            col_update_prim_topic = c_.replace('_label_nested', '_majority_primary_topic')

            # find which current clusters are below threshold
            df_vc = df_new_labels[col_new_cluster_val].value_counts()
            dv_vc_below_threshold = df_vc[df_vc <= min_subreddits_in_cluster]
            if verbose:
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
                col_new_cluster_prim_topic
            ] = df_labels[mask_subs_to_reassign][col_update_prim_topic]

        if redo_orphans:
            # TODO(djb)
            # Try to reassign ONLY the clusters where we have orphans
            df_vc_orphans = df_vc[df_vc <= 1]
            cluster_id_orphans = df_vc_orphans.index

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
            if verbose:
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

    if append_columns:
        df_new_labels = df_labels.merge(
            df_new_labels,
            how='left',
            left_index=True,
            right_index=True,
        ).copy()

    l_cols_to_front = [
        'subreddit_id',
        'subreddit_name',
        col_new_cluster_prim_topic,
        'primary_topic',
        'rating_short',
        'rating_name',
        'over_18',
        col_new_cluster_val,
        col_new_cluster_name,

        'model_sort_order',
        'posts_for_modeling_count',
    ]
    l_cols_to_front = [c for c in l_cols_to_front if c in df_new_labels.columns]
    df_new_labels = df_new_labels[
        reorder_array(l_cols_to_front, df_new_labels.columns)
    ]

    return df_new_labels


def reshape_df_to_get_1_cluster_per_row(
        df_labels: pd.DataFrame,
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
        get_one_column_per_sub_id: bool = False,
        verbose: bool = False,
) -> pd.DataFrame:
    """Take a df with clusters and reshape it so it's easier to review
    by taking a long df (1 row=1 subredddit) and reshaping so that
    1=row = 1 cluster
    """
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

    df_cluster_per_row = (
        df_labels
        .groupby([col_new_cluster_name, col_new_cluster_val, col_new_cluster_prim_topic])
        .agg(
            **{
                col_counterpart_count: ('subreddit_id', 'nunique'),
                col_list_cluster_names: ('subreddit_name', list),
                col_list_cluster_ids: ('subreddit_id', list),
            }
        )
        .sort_values(by=[col_new_cluster_val], ascending=True)
        .reset_index()
    )
    print(df_cluster_per_row.shape)

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
            .drop(['list_of_subs'], axis=1)
        )

    # when convertion to JSON for gspread, it's better to conver the list into a string
    # and to remove the brackets
    for col_list_ in [col_list_cluster_names, col_list_cluster_ids]:
        df_cluster_per_row[col_list_] = (
            df_cluster_per_row[col_list_]
            .astype(str)
            .str[1:-1]
            .str.replace("'", "")
        )

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

    df_a_to_b_list = (
        df_ab
        .groupby([col_model_sort_order, col_sub_name_a, col_sub_id_a,
                  col_new_cluster_val, col_new_cluster_name])
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
        .sort_values(by=[col_model_sort_order, ], ascending=True)
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
