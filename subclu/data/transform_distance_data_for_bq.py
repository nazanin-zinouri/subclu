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

For interactive (ipython) work:
%load_ext autoreload
%autoreload 2
from subclu.utils.eda import setup_logging
setup_logging()
"""
from typing import Union

import pandas as pd

from .data_loaders import LoadSubreddits
from ..utils.eda import reorder_array
# from subclu.data.data_loaders import LoadSubreddits
# from subclu.utils.eda import reorder_array


#
# Load data
# ===



def reshape_distances_for_bigquery(
        path_distance_matrix: str,
        path_outputs: str,
        path_subreddit_metadata: str = 'subreddits/2021-06-01',
        bucket_name: str = 'i18n-subreddit-clustering',
        col_manual_labels: str = 'manual_topic_and_rating',
) -> Union[pd.DataFrame, str, pd.DataFrame, str]:
    """Assumes we're always reading data from GCS
    Output in same location as distance matrix.
    """
    path_distance_matrix = 'data/models/fse/manual_merge_2021-06-07_17/df_subs_similarity-name_index-167_by_167.parquet'
    path_outputs = 'data/models/fse/manual_merge_2021-06-07_17/'
    path_subreddit_metadata = 'subreddits/2021-06-01'
    bucket_name = 'i18n-subreddit-clustering'
    col_manual_labels = 'manual_topic_and_rating'

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

    # calculate the top only at the end
    # df_dist_pair_top_only = (
    #     df_dist_pair
    #     .sort_values(by=['cosine_distance'], ascending=False)
    #     .groupby('subreddit_name_a')
    #     .head(20)
    #     .sort_values(by=['subreddit_name_a', 'cosine_distance'], ascending=[True, False])
    # )

    # load sub-level metadata
    df_subs = LoadSubreddits(
        bucket_name=bucket_name,
        folder_path=path_subreddit_metadata,
        columns=None,
        col_new_manual_topic=col_manual_labels,
    ).read_apply_transformations_and_merge_post_aggs()

    # Merge meta with similarity dfs
    # ===
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

    df_dist_pair_meta_top_only = (
        df_dist_pair_meta
        .sort_values(by=['cosine_distance'], ascending=False)
        .groupby('subreddit_name_a')
        .head(20)
        .sort_values(by=['subreddit_name_a', 'cosine_distance'], ascending=[True, False])
    )

