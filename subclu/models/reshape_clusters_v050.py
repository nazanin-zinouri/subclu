"""
Utils to reshape cluster outputs to create FPRs for version 0.5.0

We can call this from the command line to run as a script or we can run from
a notebook (colab or jupyter) where we can view logs.

This script requires access to
- BigQuery to read cluster labels
- GCS to save:
    - FPR files (JSON)
    - log files (parquet) [these will be used to create tables in BQ and dashboards]

The fastest way to get the queries from BQ into a pandas dataframe is using colab's bigquery magic
  but those are hard to parameterize, so we'll take a hit in speed, but our queries will
  be in source control.
"""
from datetime import datetime
import gc
import logging
from logging import info
from typing import Union, Tuple, List

# import hydra
from tqdm import tqdm

import numpy as np
import pandas as pd

from google.cloud import bigquery

from .clustering_utils import (
    create_dynamic_clusters
)
# from ..utils.eda import (
#     reorder_array,
# )


# TODO(djb): use hydra to set default parameter values & run from CLI
# @hydra.main(config_path='../config', config_name="vectorize_subreddits_test")


class CreateFPRs:
    """
    Class to vectorize text, assumes input is a data loader class + args for the data class
    For now it works with USE-multilingual. In the future we want to try different model types
    """
    def __init__(
            self,
            target_countries: float,
            output_bucket: str,
            gcs_output_path: str,
            cluster_labels_table: str,
            qa_table: str,
            qa_pt: str,
            geo_relevance_table: str,
            geo_min_users_percent_by_subreddit_l28: float = 0.14,
            geo_min_country_standardized_relevance: float = 2.4,
            partition_dt: str = "(CURRENT_DATE() - 2)",

            col_new_cluster_val: str = 'cluster_label',
            col_new_cluster_name: str = 'cluster_label_k',
            col_new_cluster_val_int: str = 'cluster_label_int',
            col_new_cluster_prim_topic: str = 'cluster_majority_primary_topic',
            col_new_cluster_topic_mix: str = 'cluster_topic_mix',
            verbose: bool = False,
            **kwargs
    ) -> None:
        """

        Args:
            target_countries:
                List of countries to run through process
            output_bucket:
                Where to save JSON & parquet outputs
            gcs_output_path:
                Path withint bucket to save JSON & parquet outputs
            cluster_labels_table:
                BigQuery table that contains model's cluster labels
            qa_table:
                BigQuery table that contains QA logic
            qa_pt:
                partition date for QA table (best = latest available date with ratings & topics)
            geo_relevance_table:
                BigQuery table with geo-relevance scores
            geo_min_users_percent_by_subreddit_l28:
                Min threshold to mark a subreddit as relevant to a country & add to FPR
            geo_min_country_standardized_relevance:
                Min threshold to mark a subreddit as relevant to a country & add to FPR
            partition_dt:
                Partition date for for `over_18` and activity tables
            col_new_cluster_val:
                column for new dynamic cluster value (int: 55)
            col_new_cluster_name:
                column for new dynamic cluster name (k_0050_label)
            col_new_cluster_prim_topic:
                column for dynamic cluster's primary topic
            col_new_cluster_topic_mix:
                column that captures nested primary topic mix
            verbose:
                whether to show additional log outputs
            **kwargs:

        Returns: None
            The data is saved to a bucket as JSON & parquet
        """
        self.target_countries = target_countries
        self.output_bucket = output_bucket
        self.gcs_output_path = gcs_output_path

        self.cluster_labels_table = cluster_labels_table
        self.partition_dt = partition_dt

        self.qa_table = qa_table
        self.qa_pt = qa_pt

        self.geo_relevance_table = geo_relevance_table
        self.geo_min_users_percent_by_subreddit_l28 = geo_min_users_percent_by_subreddit_l28
        self.geo_min_country_standardized_relevance = geo_min_country_standardized_relevance

        self.col_new_cluster_val = col_new_cluster_val
        self.col_new_cluster_name = col_new_cluster_name
        self.col_new_cluster_val_int = col_new_cluster_val_int
        self.col_new_cluster_prim_topic = col_new_cluster_prim_topic
        self.col_new_cluster_topic_mix = col_new_cluster_topic_mix

        self.verbose = verbose

        # set start time so we can use timestamp when saving outputs
        self.run_id = f"{datetime.utcnow().strftime('%Y-%m-%d_%H%M%S')}"

        # For now, save straight to GCS, in the future shift to mlflow
        #  so we'd have to save to local first
        # For full path we'd need to append `gcs://{self.output_bucket}/`
        self.gcs_output_path_this_run = (
            f"{self.gcs_output_path}/{self.run_id}"
        )

    def create_fprs(self) -> None:
        """High level method to generate FPRs for all input countries"""
        for country_code_ in tqdm(self.target_countries):
            info(f"== Country: {country_code_} ==")
            self.create_fpr_(country_code_)

    def create_fpr_(
            self,
            country_code,
            optimal_k_search: iter = None,
            verbose: bool = False,
            fpr_verbose: bool = False,
    ) -> dict:
        """
        Create fpr output for a single country

        Save outputs to a dict in case we want to analyze/pull data for a country
        """
        if optimal_k_search is None:
            optimal_k_search = np.arange(5, 11)

        d_df_fpr = {
            'df_labels_target': None,
        }

        info(f"Getting geo-relevant subreddits in model for {country_code}...")
        df_labels_target = get_geo_relevant_subreddits_and_cluster_labels(
            target_country=country_code,
            cluster_labels_table=self.cluster_labels_table,
            qa_table=self.qa_table,
            qa_pt=self.qa_pt,
            geo_relevance_table=self.geo_relevance_table,
            geo_min_users_percent_by_subreddit_l28=self.geo_min_users_percent_by_subreddit_l28,
            geo_min_country_standardized_relevance=self.geo_min_country_standardized_relevance,
            partition_dt=self.partition_dt,
        )
        # Exclude these subs either as seeds or recommendations
        _L_COVID_TITLE_KEYWORDS_TO_EXCLUDE_FROM_FPRS_ = [
            'covid',
            'coronavirus',
        ]
        # subreddit-name matches
        for word_ in _L_COVID_TITLE_KEYWORDS_TO_EXCLUDE_FROM_FPRS_:
            df_labels_target = (
                df_labels_target[~df_labels_target['subreddit_name'].str.contains(word_, na=False)]
            )
        info(f"  {df_labels_target.shape} <- Shape AFTER dropping subreddits with covid in title")

        d_df_fpr['df_labels_target'] = df_labels_target
        n_label_target_subs = len(df_labels_target)
        if n_label_target_subs != df_labels_target['subreddit_id'].nunique():
            raise Exception('subreddit_ID is NOT unique')
        if n_label_target_subs != df_labels_target['subreddit_name'].nunique():
            raise Exception('subreddit_NAME is NOT unique')

        info(f"Finding optimal k (#) of clusters...")
        df_optimal_min_check, n_min_subs_in_cluster_optimal = get_table_for_optimal_dynamic_cluster_params(
            df_labels_target=df_labels_target,
            col_new_cluster_val=self.col_new_cluster_val,
            col_new_cluster_name=self.col_new_cluster_name,
            col_new_cluster_prim_topic=self.col_new_cluster_prim_topic,
            col_new_cluster_topic_mix=self.col_new_cluster_topic_mix,
            min_subs_in_cluster_list=optimal_k_search,
            verbose=False,
            return_optimal_min_subs_in_cluster=True,
        )
        info(f"  {n_min_subs_in_cluster_optimal} <-- Optimal k")
        if verbose:
            info(f"\n{df_optimal_min_check}")

        info(f"Assigning clusters based on optimal k...")
        n_mix_start = 2  # how soon to start showing topic mix
        # l_ix = ['subreddit_id', 'subreddit_name']
        # col_subreddit_topic_mix = 'subreddit_full_topic_mix'
        # col_full_depth_mix_count = 'subreddit_full_topic_mix_count'
        # suffix_new_topic_mix = '_topic_mix_nested'

        df_labels_target_dynamic = create_dynamic_clusters(
            df_labels_target,
            agg_strategy='aggregate_small_clusters',
            min_subreddits_in_cluster=n_min_subs_in_cluster_optimal,
            l_cols_labels_input=None,
            col_new_cluster_val=self.col_new_cluster_val,
            col_new_cluster_name=self.col_new_cluster_name,
            col_new_cluster_prim_topic=self.col_new_cluster_prim_topic,
            n_mix_start=n_mix_start,
            col_new_cluster_topic_mix=self.col_new_cluster_topic_mix,
            verbose=False,
            tqdm_log_col_iterations=False,
        )
        d_df_fpr['df_labels_target_dynamic'] = df_labels_target_dynamic
        if verbose:
            df_cluster_summary_ = get_dynamic_cluster_summary(
                df_labels_target_dynamic,
                return_dict=False,
            )
            info(f"\n{df_cluster_summary_}")

        info(f"Getting cluster summary at cluster_level...")
        df_summary_cluster = get_fpr_cluster_per_row_summary(
            df_labels_target_dynamic,
            verbose=fpr_verbose,
        )
        d_df_fpr['df_summary_cluster'] = df_summary_cluster

        d_fpr_summary = self.get_top_level_stats_from_cluster_summary_(df_summary_cluster)
        # TODO(djb): create FPR output
        df_fpr, dict_fpr = get_fpr_df_and_dict(
            df_labels_target_dynamic,
            target_country_code=country_code,
            verbose=fpr_verbose,
        )
        d_df_fpr['df_fpr'] = df_fpr
        d_df_fpr['dict_fpr'] = dict_fpr

        # TODO(djb): compare summary v. FPR output
        #  - same sub_ids in seeds
        #  - same sub_ids in recommendations

        # TODO(djb): save JSON output

        # TODO(djb): save df cluster summary

        # TODO(djb): save core columns from df_labels_target_dynamic
        #  e.g., we don't need the k-cluster IDs or primary topic labels

        return d_df_fpr

    @staticmethod
    def get_top_level_stats_from_cluster_summary_(
            df_summary_cluster: pd.DataFrame,
    ) -> dict:
        """Summarize df_summary cluster for a country
        use this summary to log (to screen) total results AND to do QA with FPR output.

        This specific dict won't be saved b/c we can reconstruct it later.
        """
        mask_agg_orphan_subs = df_summary_cluster['orphan_clusters']

        # Add summary stats to dict so we can save it & reference it later
        n_agg_seed_subs_total = df_summary_cluster['seed_subreddit_count'].sum()
        n_agg_seed_subs_rec = df_summary_cluster[~mask_agg_orphan_subs]['seed_subreddit_count'].sum()

        n_agg_rec_subs_total = df_summary_cluster['recommend_subreddit_count'].sum()
        n_agg_rec_subs_rec = df_summary_cluster[~mask_agg_orphan_subs]['recommend_subreddit_count'].sum()

        n_agg_subs_missing_topic = df_summary_cluster['missingTopic_subreddit_count'].sum()
        n_agg_orphan_seed_subs = df_summary_cluster[mask_agg_orphan_subs]['seed_subreddit_count'].sum()
        n_agg_orphan_rec_subs = df_summary_cluster[mask_agg_orphan_subs]['recommend_subreddit_count'].sum()

        n_total_clusters = df_summary_cluster['cluster_label'].nunique()
        n_clusters_wo_orphans = df_summary_cluster[~mask_agg_orphan_subs]['cluster_label'].nunique()

        d_fpr_summary = dict()
        d_fpr_summary['clusters_total'] = n_total_clusters
        d_fpr_summary['clusters_wo_orphans'] = n_clusters_wo_orphans

        d_fpr_summary['seed_subreddits_total'] = n_agg_seed_subs_total
        d_fpr_summary['seed_subreddits_wo_orphans'] = n_agg_seed_subs_rec
        d_fpr_summary['recommend_subreddits'] = n_agg_rec_subs_total
        d_fpr_summary['recommend_subreddits_wo_orphans'] = n_agg_rec_subs_rec

        d_fpr_summary['missingTopic_subreddits'] = n_agg_subs_missing_topic
        d_fpr_summary['orphan_seed_subreddits'] = n_agg_orphan_seed_subs
        d_fpr_summary['orphan_recommend_subreddits'] = n_agg_orphan_rec_subs

        for k, v in d_fpr_summary.items():
            try:
                info(f"  {v:6,.0f} <- {k}")
            except ValueError:
                info(f"  {v} <- {k}")

        return d_fpr_summary


def get_fpr_cluster_per_row_summary(
        df_labels: pd.DataFrame,
        col_new_cluster_val: str = 'cluster_label',
        col_new_cluster_val_int: str = 'cluster_label_int',
        col_new_cluster_name: str = 'cluster_label_k',
        col_new_cluster_topic: str = 'cluster_topic_mix',
        l_groupby_cols: iter = None,
        prefix_seed: str = 'seed',
        prefix_recommend: str = 'recommend',
        prefix_adf: str = 'discoveryF',
        prefix_private: str = 'private',
        prefix_review_topic: str = 'missingTopic',
        suffix_col_count: str = 'subreddit_count',
        suffix_col_list_sub_names: str = 'subreddit_names_list',
        suffix_col_list_sub_ids: str = 'subreddit_ids_list',
        l_sort_cols: str = None,
        verbose: bool = True,
) -> pd.DataFrame:
    """Take a df with clusters and reshape it so it's easier to review
    by taking a long df (1 row=1 subredddit) and reshaping so that
    1=row = 1 cluster
    """
    if l_groupby_cols is None:
        # add pt & qa_pt so that we can have a trail to debug diffs
        l_groupby_cols = [
            'pt', 'qa_pt', 'geo_country_code', 'country_name',
            col_new_cluster_val, col_new_cluster_name, col_new_cluster_topic, col_new_cluster_val_int
        ]
        l_groupby_cols = [c for c in l_groupby_cols if c in df_labels]

    if l_sort_cols is None:
        l_sort_cols = [col_new_cluster_val]

    # create counts for SEED subreddits
    df_seeds = reshape_df_1_cluster_per_row(
        df_labels,
        prefix_list_and_name_cols=prefix_seed,
        l_groupby_cols=l_groupby_cols,
        l_sort_cols=l_sort_cols,
        col_new_cluster_val=col_new_cluster_val,
        col_new_cluster_val_int=col_new_cluster_val_int,
        col_new_cluster_name=col_new_cluster_name,
        col_new_cluster_topic=col_new_cluster_topic,
        suffix_col_count=suffix_col_count,
        suffix_col_list_sub_names=suffix_col_list_sub_names,
        suffix_col_list_sub_ids=suffix_col_list_sub_ids,
        agg_subreddit_ids=True,
        verbose=False,
    )
    n_seed_subreddits = df_labels['subreddit_id'].nunique()
    df_cluster_per_row = df_seeds.copy()

    # Create agg for subreddits to RECOMMEND
    mask_private_subs = df_labels['type'] == 'private'
    n_private_subs = mask_private_subs.sum()

    mask_adf_subs = df_labels['allow_discovery'] == 'f'
    n_adf_subs = mask_adf_subs.sum()

    mask_remove_or_review = df_labels['combined_filter'] != 'recommend'
    mask_review_missing_topic = df_labels['combined_filter_detail'] == 'review-missing_topic'
    n_review_missing_topic = mask_review_missing_topic.sum()

    mask_recommend_subs = ~(mask_private_subs | mask_adf_subs | mask_remove_or_review)
    n_recommend_subs = mask_recommend_subs.sum()

    # Recommend by excluding: private & allow_discovery='f'
    if verbose:
        info(f"{n_seed_subreddits:6,.0f} <- {prefix_seed.upper()} subreddits")
        info(f"{n_recommend_subs:6,.0f} <- {prefix_recommend.upper()} subs (includes orphans)")
        info(f"{n_review_missing_topic:6,.0f} <- {prefix_review_topic} subreddits")
        info(f"  {n_adf_subs:4,.0f} <- discover=f subs")
        info(f"  {n_private_subs:4,.0f} <- private subs")

    if n_recommend_subs != n_seed_subreddits:
        df_cluster_per_row = (
            df_cluster_per_row
            .merge(
                reshape_df_1_cluster_per_row(
                    df_labels[mask_recommend_subs],
                    prefix_list_and_name_cols=prefix_recommend,
                    l_groupby_cols=l_groupby_cols,
                    l_sort_cols=l_sort_cols,
                    col_new_cluster_val=col_new_cluster_val,
                    col_new_cluster_val_int=col_new_cluster_val_int,
                    col_new_cluster_name=col_new_cluster_name,
                    col_new_cluster_topic=col_new_cluster_topic,
                    suffix_col_count=suffix_col_count,
                    suffix_col_list_sub_names=suffix_col_list_sub_names,
                    suffix_col_list_sub_ids=suffix_col_list_sub_ids,
                    agg_subreddit_ids=True,
                    verbose=False,
                ),
                how='left',
                on=l_groupby_cols,
            )
        )
    else:
        df_cluster_per_row[f"{prefix_recommend}_{suffix_col_count}"] = (
            df_cluster_per_row[f"{prefix_seed}_{suffix_col_count}"]
        )
        df_cluster_per_row[f"{prefix_recommend}_{suffix_col_list_sub_names}"] = (
            df_cluster_per_row[f"{prefix_seed}_{suffix_col_list_sub_names}"]
        )
        df_cluster_per_row[f"{prefix_recommend}_{suffix_col_list_sub_ids}"] = (
            df_cluster_per_row[f"{prefix_seed}_{suffix_col_list_sub_ids}"]
        )

    # New orphan definition: clusters where we only have 1 subreddit to recommend
    #  example: 2 seeds and 0 recommend:
    #    if there are 2 subs in a cluster
    #       - one has discovery=f
    #       - the other is private
    #    then we'd have 0 subs to recommend
    mask_orphan_subs = (
        (df_cluster_per_row[f"{prefix_seed}_{suffix_col_count}"] <= 1) |
        (df_cluster_per_row[f"{prefix_recommend}_{suffix_col_count}"].fillna(0) <= 0)
    )
    df_cluster_per_row['orphan_clusters'] = mask_orphan_subs

    # subs with review-missing topic will be the largest reason for missing so add them first
    if n_review_missing_topic > 0:
        df_cluster_per_row = (
            df_cluster_per_row
            .merge(
                reshape_df_1_cluster_per_row(
                    df_labels[mask_review_missing_topic],
                    prefix_list_and_name_cols=prefix_review_topic,
                    l_groupby_cols=l_groupby_cols,
                    l_sort_cols=l_sort_cols,
                    col_new_cluster_val=col_new_cluster_val,
                    col_new_cluster_val_int=col_new_cluster_val_int,
                    col_new_cluster_name=col_new_cluster_name,
                    col_new_cluster_topic=col_new_cluster_topic,
                    suffix_col_count=suffix_col_count,
                    suffix_col_list_sub_names=suffix_col_list_sub_names,
                    agg_subreddit_ids=False,
                    verbose=False,
                ),
                how='left',
                on=l_groupby_cols,
            )
        )
    else:
        df_cluster_per_row[f"{prefix_review_topic}_{suffix_col_count}"] = 0
        df_cluster_per_row[f"{prefix_review_topic}_{suffix_col_list_sub_names}"] = np.nan

    # create agg for DISCOVERY=f subreddits
    if n_adf_subs > 0:
        df_cluster_per_row = (
            df_cluster_per_row
            .merge(
                reshape_df_1_cluster_per_row(
                    df_labels[mask_adf_subs],
                    prefix_list_and_name_cols=prefix_adf,
                    l_groupby_cols=l_groupby_cols,
                    l_sort_cols=l_sort_cols,
                    col_new_cluster_val=col_new_cluster_val,
                    col_new_cluster_val_int=col_new_cluster_val_int,
                    col_new_cluster_name=col_new_cluster_name,
                    col_new_cluster_topic=col_new_cluster_topic,
                    suffix_col_count=suffix_col_count,
                    suffix_col_list_sub_names=suffix_col_list_sub_names,
                    agg_subreddit_ids=False,
                    verbose=False,
                ),
                how='left',
                on=l_groupby_cols,
            )
        )
    else:
        df_cluster_per_row[f"{prefix_adf}_{suffix_col_count}"] = 0
        df_cluster_per_row[f"{prefix_adf}_{suffix_col_list_sub_names}"] = np.nan

    # create agg for PRIVATE subreddits
    if n_private_subs > 0:
        df_cluster_per_row = (
            df_cluster_per_row
            .merge(
                reshape_df_1_cluster_per_row(
                    df_labels[mask_private_subs],
                    prefix_list_and_name_cols=prefix_private,
                    l_groupby_cols=l_groupby_cols,
                    l_sort_cols=l_sort_cols,
                    col_new_cluster_val=col_new_cluster_val,
                    col_new_cluster_val_int=col_new_cluster_val_int,
                    col_new_cluster_name=col_new_cluster_name,
                    col_new_cluster_topic=col_new_cluster_topic,
                    suffix_col_count=suffix_col_count,
                    suffix_col_list_sub_names=suffix_col_list_sub_names,
                    agg_subreddit_ids=False,
                    verbose=False,
                ),
                how='left',
                on=l_groupby_cols,
            )
        )
    else:
        df_cluster_per_row[f"{prefix_private}_{suffix_col_count}"] = 0
        df_cluster_per_row[f"{prefix_private}_{suffix_col_list_sub_names}"] = np.nan

    # fillna all count subs with zero & cast as int
    for prefx_ in [prefix_recommend, prefix_adf, prefix_private, prefix_review_topic]:
        c_count_col_ = f"{prefx_}_{suffix_col_count}"
        try:
            df_cluster_per_row[c_count_col_] = df_cluster_per_row[c_count_col_].fillna(0)
            df_cluster_per_row[c_count_col_] = df_cluster_per_row[c_count_col_].astype(int)
        except Exception as e:
            print(e)

    info(f"{df_cluster_per_row.shape}  <- df.shape full summary")

    return df_cluster_per_row


def reshape_df_1_cluster_per_row(
        df_labels: pd.DataFrame,
        prefix_list_and_name_cols: str = None,
        suffix_col_count: str = 'subreddit_count',
        suffix_col_list_sub_names: str = 'subreddit_names_list',
        suffix_col_list_sub_ids: str = 'subreddit_ids_list',
        col_new_cluster_val: str = 'cluster_label',
        col_new_cluster_val_int: str = 'cluster_label_int',
        col_new_cluster_name: str = 'cluster_label_k',
        col_new_cluster_topic: str = 'cluster_topic_mix',
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
        # add pt & qa_pt so that we can have a trail to debug diffs
        l_groupby_cols = [
            'pt', 'qa_pt',
            col_new_cluster_val, col_new_cluster_name, col_new_cluster_topic, col_new_cluster_val_int
        ]
        l_groupby_cols = [c for c in l_groupby_cols if c in df_labels]

    if l_sort_cols is None:
        l_sort_cols = [col_new_cluster_val]

    if prefix_list_and_name_cols is not None:
        col_sub_count = f"{prefix_list_and_name_cols}_{suffix_col_count}"
        col_list_sub_names = f"{prefix_list_and_name_cols}_{suffix_col_list_sub_names}"
        col_list_sub_ids = f"{prefix_list_and_name_cols}_{suffix_col_list_sub_ids}"
    else:
        col_sub_count = suffix_col_count
        col_list_sub_names = suffix_col_list_sub_names
        col_list_sub_ids = suffix_col_list_sub_ids

    d_aggs = {
        col_sub_count: ('subreddit_id', 'nunique'),
        col_list_sub_names: ('subreddit_name', list),
    }
    if agg_subreddit_ids:
        d_aggs[col_list_sub_ids] = ('subreddit_id', list)

    df_cluster_per_row = (
        df_labels
        .groupby(l_groupby_cols)
        .agg(
            **d_aggs
        )
        .sort_values(by=l_sort_cols, ascending=True)
        .reset_index()
    )
    if verbose:
        info(f"  {df_cluster_per_row.shape}  <- df.shape, {prefix_list_and_name_cols}")

    # when converting to JSON for gspread, it's better to convert the list into a string
    # and to remove the brackets
    for col_list_ in [col_list_sub_names, col_list_sub_ids]:
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


def get_fpr_df_and_dict(
        df: pd.DataFrame,
        target_country_code: str = None,
        col_counterpart_count: str = 'subs_in_cluster_count',
        col_list_cluster_names: str = 'list_cluster_subreddit_names',
        col_list_cluster_ids: str = 'list_cluster_subreddit_ids',
        l_cols_for_seeds: List[str] = None,
        l_cols_for_clusters: List[str] = None,
        col_new_cluster_val: str = 'cluster_label',
        col_new_cluster_name: str = 'cluster_label_k',
        col_sort_by: str = None,
        verbose: bool = True,
) -> Tuple[pd.DataFrame, dict]:
    """
    Take a df with cluster labels and create 2 things:
     - a df to record/check the output
     - a dict (we can conver to JSON) for the actual FPR output

    Args:
        df:
        target_country_code:
            country code to append to dict (JSON) output for FPR
        col_counterpart_count:
            col with count of subs to recommend for a seed
        col_list_cluster_names:
            col with list of subreddit names to recommend
        col_list_cluster_ids:
            col with list of subreddit IDs to recommend
        l_cols_for_seeds:
            columns for seeds, for FPR we only really need subreddit ID & col_new_cluster_val
        l_cols_for_clusters:
            columns we need as outputs for FPR. Only need subreddit_id.
            Add subreddit_name for human qa/visual check
        col_new_cluster_val:
        col_new_cluster_name:
        col_sort_by:
        verbose:

    Returns:

    """
    if l_cols_for_seeds is None:
        l_cols_for_seeds = [
            'subreddit_id', 'subreddit_name',
            col_new_cluster_val, col_new_cluster_name,
        ]
    # make sure cols are in input df
    l_cols_for_seeds = [c for c in l_cols_for_seeds if c in df.columns]

    if l_cols_for_clusters is None:
        l_cols_for_clusters = [
            'subreddit_id', 'subreddit_name',
            # need cluster val because we'll join 2 df's on it
            col_new_cluster_val
        ]
    if col_sort_by is None:
        col_sort_by = col_new_cluster_val

    if col_sort_by not in l_cols_for_seeds:
        l_cols_for_seeds.append(col_sort_by)

    if verbose:
        info(f"  Cols for seeds:\n  {l_cols_for_seeds}")
        info(f"  Cols for clusters:\n  {l_cols_for_clusters}")

    # Check that all subs are rated="E" & NOT over_18
    #  this should've been done upstream, but good to check again
    mask_rated_e = df['rating_short'] == 'E'
    mask_over_18 = df['over_18'] == 't'
    mask_remove_from_fpr = (~mask_rated_e & mask_over_18)
    n_remove_from_fpr = mask_remove_from_fpr.sum()
    if n_remove_from_fpr > 0:
        logging.error(f"There are {n_remove_from_fpr} subs with incorrect ratings to remove")
        raise Exception(f"There are {n_remove_from_fpr} subs with incorrect ratings to remove")
    df_clean = df[~mask_remove_from_fpr].copy()

    # Create mask for subreddits to RECOMMEND
    mask_private_subs = df_clean['type'] == 'private'
    # n_private_subs = mask_private_subs.sum()
    mask_adf_subs = df_clean['allow_discovery'] == 'f'
    # n_adf_subs = mask_adf_subs.sum()
    mask_remove_or_review = df_clean['combined_filter'] != 'recommend'

    mask_recommend_subs = ~(mask_private_subs | mask_adf_subs | mask_remove_or_review)
    # n_recommend_subs = mask_recommend_subs.sum()

    suffix_rec = 'recommend'
    df_ab = (
        df[l_cols_for_seeds].copy()
        .merge(
            df[mask_recommend_subs][l_cols_for_clusters],
            how='left',
            on=[col_new_cluster_val],
            suffixes=('_seed', f"_{suffix_rec}")
        )
    )
    if verbose:
        info(f"  {df_ab.shape} <- df_ab.shape raw")

    # Set name of columns to be used for aggregation
    col_sub_name_a = 'subreddit_name_seed'
    col_sub_id_a = 'subreddit_id_seed'
    col_sub_name_b = f'subreddit_name_{suffix_rec}'
    col_sub_id_b = f'subreddit_id_{suffix_rec}'
    # Remove matches to self b/c that makes no sense as a recommendation
    #  This also gets rid of orphan subreddits/clusters
    df_ab = df_ab[
        df_ab[col_sub_id_a] != df_ab[col_sub_id_b]
        ]
    if verbose:
        info(f"  {df_ab.shape} <- df_ab.shape after removing matches to self")

    # Create groupby cols that include input seeds & col_sort_by
    # NOTE that pandas will drop any rows that have nulls in a groupby column!
    l_groupby_cols = [
        col_sub_id_a, col_sub_name_a,
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
        info(f"  Groupby cols:\n    {l_groupby_cols}")

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
        .sort_values(by=[col_sort_by, ], ascending=True)
    )

    # TODO(djb): Convert to FPR format! desired output:
    #  {"DE": {subreddit_seed: [list_of_subreddits], subreddit_seed: [list_of_subreddits]}}
    #  We'll covert it to dict here because we want to check the dict against
    #  the aggregate summary. Then we'll conver the dict to JSON when we save it
    # if we want to output as JSON(string):
    #   - set seed_id as index column (key)
    #   - select only the list of subreddit ids (series)
    #   - orient='index'
    # if we want to output dict: orient='dict'
    #  - set seed_id as index column (key)
    #  - orient='dict'
    #  - select the list of subreddits from the output dict
    d_fpr_raw = (
        df_a_to_b_list[['subreddit_id_seed', col_list_cluster_ids]]
        .set_index('subreddit_id_seed')
        .to_dict(orient='dict')
    )
    d_fpr = dict()
    if target_country_code is not None:
        # append the country code so it's easier to add to FPR file
        d_fpr[target_country_code] = d_fpr_raw[col_list_cluster_ids].copy()
    else:
        d_fpr['GEO_SIMS'] = d_fpr_raw[col_list_cluster_ids].copy()

    # when converting to JSON for gspread it's better to convert the list into a string
    # and to remove the brackets. Otherwise we can get errors.
    for c_ in [col_list_cluster_names, col_list_cluster_ids]:
        df_a_to_b_list[c_] = (
            df_a_to_b_list[c_]
            .astype(str)
            .str[1:-1]
            .str.replace("'", "")
        )
    info(f"  {df_a_to_b_list.shape} <- df_fpr.shape")
    return df_a_to_b_list, d_fpr


def get_geo_relevant_subreddits_and_cluster_labels(
        target_country: str,
        cluster_labels_table: str,
        qa_table: str,
        qa_pt: str,
        geo_relevance_table: str,
        geo_min_users_percent_by_subreddit_l28: float = 0.14,
        geo_min_country_standardized_relevance: float = 2.4,
        partition_dt: str = "(CURRENT_DATE() - 2)",
        project_name: str = None
) -> pd.DataFrame:
    """
    Query to get both:
    - geo-relevant subs for target country
    - cluster labels (df_labels)
    """
    if not partition_dt.startswith("(CURRENT_DATE("):
        # Add quote marks in case we pass a string that needs to be converted to DATE
        partition_dt = fr"'{partition_dt}'"

    sql_query = f"""
    -- Get country-relevant subreddits for FPRs + flags from CA QA
    DECLARE TARGET_COUNTRY_CODE STRING DEFAULT '{target_country}';
    DECLARE MIN_COUNTRY_STANDARDIZED_RELEVANCE NUMERIC DEFAULT 2.3;
    DECLARE MIN_USERS_PCT_L28_REL NUMERIC DEFAULT 0.14;
    
    DECLARE PARTITION_DT DATE DEFAULT {partition_dt};
    
    -- Check sensitive topics in case labels have changed since CA QA step
    DECLARE SENSITIVE_TOPICS DEFAULT [
        'Addiction Support'
        , 'Activism'
        , 'Culture, Race, and Ethnicity', 'Fitness and Nutrition'
        , 'Gender', 'Mature Themes and Adult Content', 'Medical and Mental Health'
        , 'Military'
        , "Men's Health", 'Politics', 'Sexual Orientation'
        , 'Trauma Support', "Women's Health"
    ];
    
    WITH
    target_geo_subs AS (
        SELECT
            PARTITION_DT AS pt
            , "{qa_pt}" as qa_pt
            , geo.subreddit_id
            , ars.users_l7
            , geo.geo_country_code
            , geo.country_name
            , geo.subreddit_name
            , geo.geo_relevance_default
            , geo.relevance_combined_score
            , geo.users_percent_by_subreddit_l28
            , geo.users_percent_by_country_standardized
            , nt.primary_topic
            , nt.rating_short
            , qa.predicted_rating
            , qa.predicted_topic
            , slo.allow_discovery
            , slo.over_18
            , slo.type
            , qa.combined_filter_detail
            , qa.combined_filter
            , qa.combined_filter_reason
            , qa.taxonomy_action
    
            , geo.relevance_percent_by_subreddit
            , geo.relevance_percent_by_country_standardized
    
        FROM `{qa_table}` AS qa
            LEFT JOIN (
                SELECT *
                FROM `{geo_relevance_table}`
                WHERE geo_country_code = TARGET_COUNTRY_CODE
            ) AS geo
                ON geo.subreddit_id = qa.subreddit_id
    
            LEFT JOIN (
                SELECT *
                FROM `data-prod-165221.all_reddit.all_reddit_subreddits`
                WHERE DATE(pt) = PARTITION_DT
            ) AS ars
                ON qa.subreddit_name = LOWER(ars.subreddit_name)
    
            LEFT JOIN (
                SELECT * FROM `data-prod-165221.cnc.shredded_crowdsource_topic_and_rating`
                WHERE pt = PARTITION_DT
            ) AS nt
                ON qa.subreddit_id = nt.subreddit_id
            LEFT JOIN (
                SELECT *
                FROM `data-prod-165221.ds_v2_postgres_tables.subreddit_lookup`
                -- Get latest partition
                WHERE dt = PARTITION_DT
            ) AS slo
                ON qa.subreddit_id = slo.subreddit_id
    
        WHERE 1=1
            AND qa.pt = "{qa_pt}"
            -- Pick subreddits relevant to target country under at least one metric/threshold
            --   Use the numeric values in case the defined threshold change
            AND geo.geo_country_code = TARGET_COUNTRY_CODE
            AND (
                geo_relevance_default = TRUE
                OR users_percent_by_subreddit_l28 >= {geo_min_users_percent_by_subreddit_l28}
                OR users_percent_by_country_standardized >= {geo_min_country_standardized_relevance}
                -- Try the combined score to include a few more relevant subreddits
                OR relevance_combined_score >= 0.175
            )
    
            -- Only include subs we can use as seeds OR subs we should recommend
            AND (
                qa.combined_filter = 'recommend'
                -- We can still use allow_discover=f for seeds
                OR (
                    qa.combined_filter = 'remove'
                    AND qa.combined_filter_reason = 'allow_discovery_f'
                )
                -- We can use subs with missing topic as seeds
                OR (
                    qa.combined_filter = 'review'
                    AND qa.combined_filter_reason = 'missing_topic'
                )
            )
            AND qa.subreddit_name != 'profile'
            AND COALESCE(slo.type, '') IN ('private', 'public', 'restricted')
            AND COALESCE(slo.verdict, 'f') != 'admin-removed'
            AND COALESCE(is_spam, FALSE) = FALSE
            AND COALESCE(slo.over_18, 'f') = 'f'
            AND COALESCE(quarantine, FALSE) = FALSE
            AND COALESCE(nt.rating_short, '') = "E"
            AND COALESCE(nt.primary_topic, '') NOT IN UNNEST(SENSITIVE_TOPICS)
    
        ORDER BY geo.relevance_combined_score DESC, geo.users_percent_by_subreddit_l28 DESC
    )
    , cluster_labels AS (
        SELECT
            sc.subreddit_id
    
            -- Exclude clusters that are overly broad... these don't provide meaningful recommendations
            , sc.* EXCEPT(
                subreddit_id, subreddit_name, primary_topic, __index_level_0__
                , k_0010_label, k_0012_label, k_0020_label, k_0025_label, k_0030_label, k_0040_label
                , k_0049_label
                , k_0010_majority_primary_topic, k_0012_majority_primary_topic, k_0020_majority_primary_topic
                , k_0025_majority_primary_topic, k_0030_majority_primary_topic, k_0040_majority_primary_topic
                , k_0049_majority_primary_topic
            )
        FROM `{cluster_labels_table}` sc
    )
    
    SELECT
        geo.*
        , lbl.*
    FROM target_geo_subs AS geo
        -- inner join so that we only have subs that are BOTH relevant & in model
        INNER JOIN cluster_labels AS lbl
            ON geo.subreddit_id = lbl.subreddit_id
    ;
    """

    bq_client = bigquery.Client(project=project_name)
    df_ = bq_client.query(sql_query).to_dataframe()

    info(f" {df_.shape}  <- df_shape")
    return df_


def get_table_for_optimal_dynamic_cluster_params(
        df_labels_target: pd.DataFrame,
        col_new_cluster_val: str = 'cluster_label',
        col_new_cluster_name: str = 'cluster_label_k',
        col_new_cluster_prim_topic: str = 'cluster_majority_primary_topic',
        col_new_cluster_topic_mix: str = 'cluster_topic_mix',
        min_subs_in_cluster_list: iter = None,
        col_num_orph_subs: str = 'num_orphan_subreddits',
        col_num_subs_mean: str = 'num_subreddits_per_cluster_mean',
        col_num_subs_median: str = 'num_subreddits_per_cluster_median',
        return_optimal_min_subs_in_cluster: bool = False,
        verbose: bool = False,
        tqdm_log_col_iterations: bool = False,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, int]]:
    """We want to balance two things:
    - prevent orphan subreddits
    - prevent clusters that are too large to be meaningful

    In order to do this at a country level, we'll be better off starting with smallest clusters
    and rolling up until we have at least N subreddits in one cluster.
    """
    if min_subs_in_cluster_list is None:
        min_subs_in_cluster_list = [4, 5, 6, 7, 8, 9, 10]

    # Rely on upstream query to cut-off recommendations at ~k=50
    #  using clusters broader than that results in low quality outputs
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
            tqdm_log_col_iterations=tqdm_log_col_iterations,
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




#
# ~ fin
#
