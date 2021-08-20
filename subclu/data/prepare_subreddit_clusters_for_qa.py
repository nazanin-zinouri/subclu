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

from ..utils.eda import reorder_array


def reshape_for_distance_qa(
        df_subs_distance: pd.DataFrame,
        df_geo: pd.DataFrame,
        df_ambassador: pd.DataFrame,
        df_subs_cluster: pd.DataFrame = None,
        df_counterpart_seeds: pd.DataFrame = None,
        top_n_subs_by_distance: int = 10,
        top_n_subs_in_cluster: int = 20,
        top_n_subs_not_german: int = 5,

        col_cluster_id: str = 'cluster_id_agg_ward_cosine_200',
        col_manual_topic: str = 'manual_topic_and_rating',
        col_ger_subs_count: str = 'german_subs_in_cluster',
        col_ger_or_ambassador: str = 'german_or_ambassador_sub',
        col_subs_in_same_cluster: str = 'subreddit_a_and_b_in_same_cluster',
        col_cluster_users_l28_sum: str = 'users_l28_for_cluster',
        col_cluster_primary_topics: str = 'primary_topics_in_cluster',

) -> pd.DataFrame:
    """Take input dfs and reshape them to create a df that we can use for manual QA.

    Output will prob get saved as a CSV that I can then use to create a Google spreadsheet
    so we can collaborate & QA together.

    The original process took around 34 seconds to run for first N steps. With this fxn
    I brought it down to ~28 seconds.
    """
    n_subs_to_limit = min([(top_n_subs_not_german + top_n_subs_by_distance + top_n_subs_in_cluster) * 5,
                           1000])

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
        .head(n_subs_to_limit)
        .reset_index(drop=True)
    ).copy()
    info(f"{df_subs_distance_qa_top.shape}  <- Shape after keeping only top {n_subs_to_limit} per sub-a")

    info(f"Append German-relevant columns to sub-B")
    df_subs_distance_qa_top = append_german_relevant_cols_to_distance_df(
        df_subs_distance_qa=df_subs_distance_qa_top,
        df_geo=df_geo,
        df_ambassador=df_ambassador,
        cols_sub_name_to_merge=['subreddit_name_b'],
        col_ger_or_ambassador=col_ger_or_ambassador,
    )

    info(f"Append cluster-metadata to both subs-A & B")
    df_subs_distance_qa_top = append_german_relevant_cols_to_distance_df(
        df_subs_distance_qa=df_subs_distance_qa_top,
        df_subs_cluster=df_subs_cluster,
        cols_sub_name_to_merge=['subreddit_name_a', 'subreddit_name_b'],
        cols_cluster_to_merge=None,  # None="default" cols
    )

    info(f"Create new col to check whether sub-a & sub-b are in same cluster")
    df_subs_distance_qa_top[col_subs_in_same_cluster] = (
        df_subs_distance_qa_top[f"{col_cluster_id}_a"] ==
        df_subs_distance_qa_top[f"{col_cluster_id}_b"]
    )

    info(f"Move sub-B's in same cluster higher up / append them")
    # how do I check whether subs are in same cluster...?
    # maybe sort, groupby, & use .head() in two steps:
    #  - keep top 6 by distance
    #  - keep top 20 by cluster ID
    #  - remove duplicates (if subs are in same cluster AND top N closest)
    mask_subs_in_same_cluster = df_subs_distance_qa_top[col_subs_in_same_cluster]
    mask_non_german_subs_b = (
        df_subs_distance_qa_top[f"{col_ger_or_ambassador}_b"] == 'no'
    )
    df_subs_distance_qa_final = pd.concat(
        [
            # first keep the closest subs, whether or not they're in the same cluster
            df_subs_distance_qa_top
            .sort_values(by=['subreddit_name_a', 'cosine_distance'], ascending=[True, False])
            .groupby('subreddit_name_a')
            .head(top_n_subs_by_distance),

            # Now keep the non-german, closest subs (in case a cluster has many German subs)
            #  otherwise we might only get a list of German subs w/o counterparts
            df_subs_distance_qa_top[mask_non_german_subs_b]
            .sort_values(by=['subreddit_name_a', 'cosine_distance'], ascending=[True, False])
            .groupby('subreddit_name_a')
            .head(top_n_subs_not_german),

            # Now keep subs that are in the same cluster
            df_subs_distance_qa_top[mask_subs_in_same_cluster]
            .sort_values(by=['subreddit_name_a', 'cosine_distance'], ascending=[True, False])
            .groupby('subreddit_name_a')
            .head(top_n_subs_in_cluster),
        ],
        axis=0,
        ignore_index=True
    )

    info(f"Sort rows before output")
    # want to keep subreddits in similar clusters next to each other
    df_subs_distance_qa_final = (
        df_subs_distance_qa_final
        .drop_duplicates(subset=['subreddit_name_a', 'subreddit_name_b'])
        .sort_values(
            by=[
                'cluster_has_german_subs_and_mostly_sfw_a',
                'users_l28_a',
                'german_subs_in_cluster_a',
                'cluster_id_agg_ward_cosine_200_a',
                'subreddit_name_a', col_subs_in_same_cluster,
                f"{col_ger_or_ambassador}_b", 'cosine_distance'
            ],
            ascending=[
                False,
                False,
                False,
                True,
                True, True,
                True, False
            ])
        .reset_index(drop=True)
    )

    info(f"{df_subs_distance_qa_final.shape}  <- "
         f"Shape after keeping only top {top_n_subs_by_distance} by distance and"
         f" {top_n_subs_in_cluster} subs in cluster per sub-a")

    info(f"Add seed counterpart subs...")

    l_cols_to_front = [
        'cosine_distance',
        'subreddit_name_a',
        'subreddit_name_b',
        col_subs_in_same_cluster,
        f"{col_ger_or_ambassador}_a",
        f"{col_ger_or_ambassador}_b",

        'rating_b',
        'topic_b',

        'subreddit_title_a',
        'subreddit_public_description_a',
        'subreddit_title_b',
        'subreddit_public_description_b',

        'primary_post_language_b',
        'primary_post_language_percent_b',
        'German_posts_percent_b',

        'primary_post_language_a',
        'primary_post_language_percent_a',
        'German_posts_percent_a',


        'german_subs_in_cluster_a',

        'subreddit_url_a',
        'subreddit_url_with_google_translate_a',
        'subreddit_url_b',
        'subreddit_url_with_google_translate_b',

    ]
    df_subs_distance_qa_final = df_subs_distance_qa_final[
        reorder_array(l_cols_to_front, df_subs_distance_qa_final.columns)
    ]

    # maybe only keep to 25 instead of top 50?

    return df_subs_distance_qa_final


def append_german_relevant_cols_to_distance_df(
        df_subs_distance_qa: pd.DataFrame,
        df_geo: pd.DataFrame = None,
        df_ambassador: pd.DataFrame = None,
        df_subs_cluster: pd.DataFrame = None,
        cols_sub_name_to_merge: List = None,
        cols_cluster_to_merge: List = None,
        col_ger_or_ambassador: str = 'german_or_ambassador_sub',
) -> pd.DataFrame:
    """NOTE: this fxn changes the input df"""
    # expected format for: cols_sub_name_to_merge
    # cols_sub_name_to_merge = ['subreddit_name_a', 'subreddit_name_b']
    if df_geo is not None:
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

    if df_subs_cluster is not None:
        if cols_cluster_to_merge is None:
            # TODO(djb): define the cluster columns to merge
            cols_cluster_to_merge = [
                'subreddit_name',  # merge on this column
                'cluster_id_agg_ward_cosine_200',
                'german_subs_in_cluster',
                'cluster_has_german_subs_and_mostly_sfw',
                'German_posts_percent',

                'subreddit_title',
                'subreddit_public_description',
                'subreddit_url',
                'subreddit_url_with_google_translate',
                'users_l28',
                'posts_l28',
            ]
        for sub_x in tqdm(cols_sub_name_to_merge):
            sub_suffix = sub_x.split('_')[-1]
            df_subs_distance_qa = df_subs_distance_qa.merge(
                df_subs_cluster[cols_cluster_to_merge]
                .rename(columns={c: f'{c}_{sub_suffix}' for c in cols_cluster_to_merge}),
                how='left',
                on=sub_x,
            )

    return df_subs_distance_qa

#
# ~fin
#
