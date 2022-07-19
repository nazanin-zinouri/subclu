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
    ) -> dict:
        """
        Create fpr output for a single country

        Save outputs to a dict in case we want to analyze/pull data for a country
        """
        d_fpr = {
            'df_labels_target': None,
            'df_labels_target_dynamic_raw': None
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
        d_fpr['df_labels_target'] = df_labels_target

        info(f"Finding optimal k (#) of clusters...")
        df_optimal_min_check, n_min_subs_in_cluster_optimal = get_table_for_optimal_dynamic_cluster_params(
            df_labels_target=df_labels_target,
            col_new_cluster_val=self.col_new_cluster_val,
            col_new_cluster_name=self.col_new_cluster_name,
            col_new_cluster_prim_topic=self.col_new_cluster_prim_topic,
            col_new_cluster_topic_mix=self.col_new_cluster_topic_mix,
            min_subs_in_cluster_list=np.arange(4, 11),
            verbose=False,
            return_optimal_min_subs_in_cluster=True,
        )
        info(f"  {n_min_subs_in_cluster_optimal} <-- Optimal k")
        info(f"\n{df_optimal_min_check}")  # .rename(columns={c: c.split('_') for c in df_optimal_min_check.columns}))

        info(f"Assigning clusters based on optimal k...")
        n_mix_start = 2  # how soon to start showing topic mix
        # l_ix = ['subreddit_id', 'subreddit_name']
        # col_subreddit_topic_mix = 'subreddit_full_topic_mix'
        # col_full_depth_mix_count = 'subreddit_full_topic_mix_count'
        # suffix_new_topic_mix = '_topic_mix_nested'
        # col_new_cluster_val_int = 'cluster_label_int'

        df_labels_target_dynamic_raw = create_dynamic_clusters(
            df_labels_target,
            agg_strategy='aggregate_small_clusters',
            min_subreddits_in_cluster=n_min_subs_in_cluster_optimal,
            l_cols_labels_input=None,
            col_new_cluster_val=self.col_new_cluster_val,
            col_new_cluster_name=self.col_new_cluster_name,
            col_new_cluster_prim_topic=self.col_new_cluster_prim_topic,
            n_mix_start=n_mix_start,
            col_new_cluster_topic_mix=self.col_new_cluster_topic_mix,
            # col_subreddit_topic_mix=self.col_subreddit_topic_mix,
            # col_full_depth_mix_count=self.col_full_depth_mix_count,
            # suffix_new_topic_mix=self.suffix_new_topic_mix,
            # l_ix=l_ix,
            verbose=False,
            tqdm_log_col_iterations=False,
        )
        d_fpr['df_labels_target_dynamic_raw'] = df_labels_target_dynamic_raw

        return d_fpr




# Exclude these subs either as seeds or recommendations
_L_COVID_TITLE_KEYWORDS_TO_EXCLUDE_FROM_FPRS_ = [
    'covid',
    'coronavirus',
]


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
            geo.subreddit_id
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
    
            -- Exclude subs we should recommend
            AND (
                qa.combined_filter = 'recommend'
                -- We can still use allow_discover=f for seeds
                OR (
                    qa.combined_filter = 'remove'
                    AND qa.combined_filter_reason = 'allow_discovery_f'
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
        col_rating_latest: str = 'rating_short',
        col_over_18_latest: str = 'over_18',
        col_allow_discovery_latest: str = 'allow_discovery',
        print_qa_check: bool = True,
        additional_qa_keywords: List[str] = None,
) -> pd.DataFrame:
    """Apply expected filters to df
    NOTE: this does NOT check primary topics.
    We assume that primary topics have already been checked in upstream SQL, otherwise
    it's a pain to keep the sensitive primary topic list synced everywhere.

    For v0.5.0 we're not removing allow-discovery=false here. We want to keep them
    as SEEDS, but we'll continue NOT recommending them (as expected).
    """
    mask_rated_e = df[col_rating_latest] == 'E'
    mask_not_over_18 = df[col_over_18_latest] != 't'
    mask_allows_discovery = df[col_allow_discovery_latest] != 'f'

    mask_clean_for_fpr = (
            mask_rated_e &
            mask_not_over_18
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

    print(f"Subs to only use as seeds (discovery=f)")
    print(f"  {df[mask_allows_discovery]['subreddit_name']}")

    return df_clean



#
# ~ fin
#
