"""
Use these functions to prepare the cluster data for QA in a spreadsheet.

In the short term a spreadsheet is the easiest/best thing we can use to collaborate
for QA.

In the long term, maybe we can add some automation or tooling around it?
- e.g., similar to the tagging system or mechanical Turk to verify clusters.
"""
from logging import info
from typing import List

import numpy as np
import pandas as pd
from tqdm.auto import tqdm


def reshape_for_distance_qa(
        df_subs_distance: pd.DataFrame,
        df_geo: pd.DataFrame,
        df_ambassador: pd.DataFrame,
        top_n_cols_by_distance: int = 50,

        col_manual_topic: str = 'manual_topic_and_rating',
        col_ger_subs_count: str = 'german_subs_in_cluster',
        col_ger_or_ambassador: str = 'german_or_ambassador_sub',
        col_cluster_users_l28_sum: str = 'users_l28_for_cluster',
        col_cluster_primary_topics: str = 'primary_topics_in_cluster',

) -> pd.DataFrame:
    """Take input dfs and reshape them to create a df that we can use for manual QA.

    Output will prob get saved as a CSV that I can then use to create a Google spreadsheet
    so we can collaborate & QA together.
    """
    # columns used for new-cols & aggregations
    # col_manual_topic = 'manual_topic_and_rating'
    # col_ger_subs_count = 'german_subs_in_cluster'
    # col_ger_or_ambassador = 'german_or_ambassador_sub'
    # col_cluster_users_l28_sum = 'users_l28_for_cluster'
    # col_cluster_primary_topics = 'primary_topics_in_cluster'

    df_subs_distance_qa = df_subs_distance.copy()

    info(f"{df_subs_distance_qa.shape}  <- Original df_distance shape")
    info(f"Append geo-country code + ambassador flags to sub-A")
    # this join is expensive, so only do sub-a first, and only do sub-b AFTER we've filtered
    df_subs_distance_qa = append_german_relevant_cols_to_distance_df(
        df_subs_distance_qa=df_subs_distance_qa,
        df_geo=df_geo,
        df_ambassador=df_ambassador,
        cols_sub_name_to_merge=['subreddit_name_a'],
        col_ger_or_ambassador=col_ger_or_ambassador,
    )

    info(f"Keep only subs-A that are German relevant")
    df_subs_distance_qa = df_subs_distance_qa[
        df_subs_distance_qa['german_or_ambassador_sub_a'] == 'yes'
    ]
    info(f"{df_subs_distance_qa.shape}  <- Shape after keeping only German-relevant sub-a")

    info(f"Keep only top N subs by distance")
    #  we'll use a mix of distance + cluster to come up with final list
    df_subs_distance_qa_top = (
        df_subs_distance_qa
        .sort_values(by=['subreddit_name_a', 'cosine_distance'], ascending=[True, False])
        .groupby('subreddit_name_a')
        .head(top_n_cols_by_distance)
        .reset_index(drop=True)
    ).copy()

    info(f"Append German-relevant columns to sub-B")
    df_subs_distance_qa_top = append_german_relevant_cols_to_distance_df(
        df_subs_distance_qa=df_subs_distance_qa_top,
        df_geo=df_geo,
        df_ambassador=df_ambassador,
        cols_sub_name_to_merge=['subreddit_name_b'],
        col_ger_or_ambassador=col_ger_or_ambassador,
    )

    info(f"{df_subs_distance_qa_top.shape}  <- Shape after keeping only top {top_n_cols_by_distance} per sub-a")

    return df_subs_distance_qa_top


def append_german_relevant_cols_to_distance_df(
        df_subs_distance_qa: pd.DataFrame,
        df_geo: pd.DataFrame,
        df_ambassador: pd.DataFrame,
        cols_sub_name_to_merge: List,
        col_ger_or_ambassador: str = 'german_or_ambassador_sub',
) -> pd.DataFrame:
    """NOTE: this fxn changes the input df"""
    # expected format for: cols_sub_name_to_merge
    # cols_sub_name_to_merge = ['subreddit_name_a', 'subreddit_name_b']
    for sub_x in tqdm(cols_sub_name_to_merge):
        sub_suffix = sub_x.split('_')[-1]
        df_subs_distance_qa = df_subs_distance_qa.merge(
            df_geo[['subreddit_name', 'geo_country_code']]
            .rename(columns={'subreddit_name': sub_x, 'geo_country_code': f'geo_country_code_{sub_suffix}'}),
            how='left',
            on=sub_x,
        )

        # Add columns to flag DE & ambassador subs
        df_subs_distance_qa[f'ambassador_sub_{sub_suffix}'] = np.where(
            df_subs_distance_qa[sub_x].isin(df_ambassador['subreddit_name']),
            'yes',
            'no',
        )
        df_subs_distance_qa[f"{col_ger_or_ambassador}_{sub_suffix}"] = np.where(
            (
                (df_subs_distance_qa[f'ambassador_sub_{sub_suffix}'] == 'yes') |
                (df_subs_distance_qa[f'geo_country_code_{sub_suffix}'] == 'DE')
            ),
            'yes',
            'no',
        )
    return df_subs_distance_qa

#
# ~fin
#
