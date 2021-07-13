"""
This script takes model outputs and helps create queries/formats that we can
push to BQ.
Once we have embeddings & distances, we need to push data back to BigQuery so that
people can use this data and we can share on Mode.

Steps:
- load distance matrix for subreddits
    - set self-distance to null b/c we don't want weird results
- Reshape to: pair-wise distances
    - subA, subB, distance -- .unstack()!!
    - drop self distance (subA == subB)
- load subreddit metadata
    - might need to create new method to to load post metadata & create aggregates


4 outputs:
- File: raw-ish data
    - Append subreddit names & IDs only
    - distance, subA, subA_id, subB, subB_id
- SQL statement to create table from this file


- ready for Mode / dashboards
    - limit to only to 20 closest distances for each sub
        - e.g., subA should only have 15 closest distances
    - besides IDs, append manual labels
    - for screen-view counts, subscriptions, etc:
        - CREATE A VIEW so that it's updated dynamically with data from previous day!
- SQL statement to create talbe from this file


# For interactive (ipython) work:
%load_ext autoreload
%autoreload 2

# fix logging in ipython/jupyter
from subclu.utils.eda import setup_logging
setup_logging()

from subclu.data.data_loaders import LoadSubreddits
from subclu.utils.eda import reorder_array
"""
from datetime import datetime
from logging import info
from typing import Tuple

import pandas as pd

from .data_loaders import LoadSubreddits
from ..utils.eda import reorder_array


def get_sql_to_create_table_from_parquet(
        table_name: str,
        file_path: str,
        bucket_name: str = 'i18n-subreddit-clustering',
        data_format: str = 'PARQUET',
) -> str:
    """Standardize creating SQL tables

    Example for paths:
    - single file:
        data/models/fse/manual_merge_2021-06-07_17/df_one_file.parquet
    - files matching pattern:
        data/models/fse/manual_merge_2021-06-07_17/df_*.parquet
    - folder. NOTE: ALL FILES IN FOLDER MUST HAVE THE SAME COLUMNS/FORMAT
        data/models/fse/manual_merge_2021-06-07_17/

    CREATE OR REPLACE EXTERNAL TABLE `reddit-employee-datasets.david_bermejo.post_embeddings_v002_tsne2`
    OPTIONS (
      format='PARQUET',
      uris=["gs://i18n-subreddit-clustering/data/models/fse/df_one_file.parquet"]
    )
    """
    return (
        f"""
        CREATE OR REPLACE EXTERNAL TABLE `reddit-employee-datasets.david_bermejo.{table_name}`
        OPTIONS (
          format={data_format},
          uris=["gs://{bucket_name}/{file_path}"]
        )
        ;
        """
    )


def reshape_distances_for_bigquery(
        path_distance_matrix: str,
        path_outputs: str,
        path_subreddit_metadata: str = 'subreddits/2021-06-01',
        bucket_name: str = 'i18n-subreddit-clustering',
        col_manual_labels: str = 'manual_topic_and_rating',
        output_table_prefix: str = 'subreddit_distance_model_v002',
        top_subs_to_keep: int = 20,
) -> Tuple[dict, pd.DataFrame, pd.DataFrame]:
    """Assumes we're always reading data from GCS
    Output in same location as distance matrix.

    # Testing values:
    path_distance_matrix = 'data/models/fse/manual_merge_2021-06-07_17/df_subs_similarity-name_index-167_by_167.parquet'
    path_outputs = 'data/models/fse/manual_merge_2021-06-07_17'
    path_subreddit_metadata = 'subreddits/2021-06-01'
    bucket_name = 'i18n-subreddit-clustering'
    col_manual_labels = 'manual_topic_and_rating'
    output_table_prefix = 'subreddit_distance_model_v002'
    top_subs_to_keep = 20
    """
    info(f"Load distance matrix...")
    df_dist = pd.read_parquet(f"gs://{bucket_name}/{path_distance_matrix}")

    # Reshape distances to pair-wise & rename columns
    df_dist_pair = (
        df_dist.unstack()
        .reset_index()
        .rename(
            columns={'level_0': 'subreddit_name_a',
                     'level_1': 'subreddit_name_b',
                     0: 'cosine_distance',
                     }
        )
        .sort_values(by=['subreddit_name_a', 'cosine_distance'], ascending=[True, False])
    )
    df_dist_pair = df_dist_pair[df_dist_pair['subreddit_name_a'] != df_dist_pair['subreddit_name_b']]

    info(f"Load subreddit metadata...")
    df_subs = LoadSubreddits(
        bucket_name=bucket_name,
        folder_path=path_subreddit_metadata,
        columns=None,
        col_new_manual_topic=col_manual_labels,
    ).read_apply_transformations_and_merge_post_aggs()

    # Merge meta with similarity dfs
    # ===
    info(f"Merge distance + metadata...")
    l_meta_basic = [
        'subreddit_name',
        'subreddit_id',
        col_manual_labels,
        'German_posts_percent',
        'post_median_word_count',
    ]
    df_dist_pair_meta = (
        df_dist_pair
        .merge(
            df_subs[l_meta_basic].set_index('subreddit_name'),
            left_on=['subreddit_name_a'],
            right_index=True,
        )
        .merge(
            df_subs[l_meta_basic].set_index('subreddit_name'),
            left_on=['subreddit_name_b'],
            right_index=True,
            suffixes=('_a', '_b')
        )
        .sort_values(by=['subreddit_name_a', 'cosine_distance'], ascending=[True, False])
    )
    df_dist_pair_meta = df_dist_pair_meta[
        reorder_array(
            ['cosine_distance', 'subreddit_name_a', 'subreddit_name_b'],
            sorted(df_dist_pair_meta.columns)
        )
    ]

    info(f"Create new df to keep only top {top_subs_to_keep} subs by distance...")
    df_dist_pair_meta_top_only = (
        df_dist_pair_meta
        .sort_values(by=['cosine_distance'], ascending=False)
        .groupby('subreddit_name_a')
        .head(top_subs_to_keep)
        .sort_values(by=['subreddit_name_a', 'cosine_distance'], ascending=[True, False])
    )

    shape_full = df_dist_pair_meta.shape
    shape_top = df_dist_pair_meta_top_only.shape
    table_dt_stamp = datetime.utcnow().strftime('%Y%m%d')
    d_map_file_meta = {
        'df_dist_full': {
            'table_name': f"{output_table_prefix}_full_{table_dt_stamp}",
            'file_name': f"{path_outputs}/df_distance_pair_meta_full-{table_dt_stamp}-{shape_full[0]}_by_{shape_full[1]}.parquet",
        },
        'df_dist_top_only': {
            'table_name': f"{output_table_prefix}_top_only_{table_dt_stamp}",
            'file_name': f"{path_outputs}/df_distance_pair_meta_top_only-{table_dt_stamp}-{shape_top[0]}_by_{shape_top[1]}.parquet",
        },
    }

    # Bigquery will read the index as a column, so let's set our own rather than getting a weird
    #  column like `__index_level_0__`
    df_dist_pair_meta.set_index(['subreddit_id_a', 'subreddit_id_b']).to_parquet(
        f"gs://{bucket_name}/{d_map_file_meta['df_dist_full']['file_name']}"
    )
    df_dist_pair_meta_top_only.set_index(['subreddit_id_a', 'subreddit_id_b']).to_parquet(
        f"gs://{bucket_name}/{d_map_file_meta['df_dist_top_only']['file_name']}"
    )

    d_map_file_meta['df_dist_full']['sql'] = get_sql_to_create_table_from_parquet(
        table_name=d_map_file_meta['df_dist_full']['table_name'],
        file_path=d_map_file_meta['df_dist_full']['file_name'],
        bucket_name=bucket_name,
        data_format='PARQUET',
    )

    d_map_file_meta['df_dist_top_only']['sql'] = get_sql_to_create_table_from_parquet(
        table_name=d_map_file_meta['df_dist_top_only']['table_name'],
        file_path=d_map_file_meta['df_dist_top_only']['file_name'],
        bucket_name=bucket_name,
        data_format='PARQUET',
    )

    [print(d_['sql']) for d_ in d_map_file_meta.values()]

    # TODO(djb) get permission to run sql from VM so I can update SQL from script
    return d_map_file_meta, df_dist_pair_meta, df_dist_pair_meta_top_only


def reshape_distances_to_pairwise_bq(
        df_distance_matrix: pd.DataFrame,
        df_sub_metadata: pd.DataFrame,
        col_new_manual_topic: str = 'manual_topic_and_rating',
        index_name: str = 'subreddit_name',
        top_subs_to_keep: int = 20,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    This function assumes that the distance and metadata dfs have already been loaded
    somewhere else, it's up to user where/how to load and save.

    This one is better suited for use in an mlflow job.

    NOTE:
    # Bigquery will read the index as a column, so let's set our own rather than getting a weird
    #  column like `__index_level_0__`
    """
    # Rename index & column names, BEFORE .unstack() to prevent name collisions
    #  i.e., if they both have the same name, we'll get a ValueError

    # Reshape distances to pair-wise & rename columns
    df_dist_pair = (
        df_distance_matrix
        .rename_axis(f'{index_name}_a', axis='columns')
        .rename_axis(f'{index_name}_b', axis='index')
        .unstack()
        .reset_index()
        .rename(
            columns={
                # 'level_0': 'subreddit_name_a',
                # 'level_1': 'subreddit_name_b',
                0: 'cosine_distance',
            }
        )
        .sort_values(by=['subreddit_name_a', 'cosine_distance'], ascending=[True, False])
    )
    df_dist_pair = df_dist_pair[df_dist_pair['subreddit_name_a'] != df_dist_pair['subreddit_name_b']]

    if df_sub_metadata is not None:
        # Merge meta with similarity dfs
        # ===
        info(f"Merge distance + metadata...")
        l_meta_basic = [
            'subreddit_name',
            'subreddit_id',
            col_new_manual_topic,
            'German_posts_percent',
            'post_median_word_count',
        ]
        df_dist_pair = (
            df_dist_pair
            .merge(
                df_sub_metadata[l_meta_basic].set_index('subreddit_name'),
                left_on=['subreddit_name_a'],
                right_index=True,
            )
            .merge(
                df_sub_metadata[l_meta_basic].set_index('subreddit_name'),
                left_on=['subreddit_name_b'],
                right_index=True,
                suffixes=('_a', '_b')
            )
            .sort_values(by=['subreddit_name_a', 'cosine_distance'], ascending=[True, False])
        )

    df_dist_pair = df_dist_pair[
        reorder_array(
            ['cosine_distance', 'subreddit_name_a', 'subreddit_name_b'],
            sorted(df_dist_pair.columns)
        )
    ]

    info(f"Create new df to keep only top {top_subs_to_keep} subs by distance...")
    df_dist_pair_top_only = (
        df_dist_pair
        .sort_values(by=['cosine_distance'], ascending=False)
        .groupby('subreddit_name_a')
        .head(top_subs_to_keep)
        .sort_values(by=['subreddit_name_a', 'cosine_distance'], ascending=[True, False])
    )
    info(f"  {df_dist_pair.shape} <- df_dist_pair_meta.shape (before setting index)")
    info(f"  {df_dist_pair.shape} <- df_dist_pair_meta_top_only.shape (before setting index)")

    try:
        return (
            df_dist_pair.set_index(['subreddit_id_a', 'subreddit_id_b']),
            df_dist_pair_top_only.set_index(['subreddit_id_a', 'subreddit_id_b'])
        )
    except KeyError:
        return df_dist_pair, df_dist_pair_top_only


# TODO(djb) make it a script to run from command line?


#
# ~fin
#
