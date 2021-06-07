"""
Utils to improve/streamline finding similarity and other common metrics

"""
import logging
from typing import Union, List
import pandas as pd


def get_most_similar_items(
        item_name: str,
        df_similarity: pd.DataFrame,
        df_metadata: pd.DataFrame,
        top_n: int = 10,
        col_meta_merge: str = 'subreddit_name',
        meta_cols_to_merge: Union[list, str] = 'subreddit_cols',
) -> pd.DataFrame:
    """Given a subreddit_name & df_similarity, return most similar items.

    df_meta optional, but will help give more context to similar subs/items
    Assumptions:
        df_similarity = a square matrix where index & columns have the same name and order.

    """
    if (top_n is None) or (top_n == 'all'):
        top_n = len(df_similarity)
    if meta_cols_to_merge is None:
        meta_cols_to_merge = df_metadata.columns
    elif meta_cols_to_merge == 'subreddit_cols':
        meta_cols_to_merge = [
            'subreddit_name',
            'new_topic_and_rating',
            'rating',
            'over_18',
            'whitelist_status',
            'users_l28',
            'subscribers',
            'posts_l28',
            'subreddit_title',
            'subreddit_public_description',
    ]

    try:
        df_most_sim = df_similarity[item_name].sort_values(ascending=False).to_frame()
    except KeyError:
        logging.error(f"Value not in df_similarity: {item_name}")
        return pd.DataFrame()

    if df_metadata is not None:
        df_most_sim = df_most_sim.merge(
            df_metadata[meta_cols_to_merge],
            how='left',
            left_index=True,
            right_on=[col_meta_merge]
        ).reset_index(drop=True)

    return df_most_sim.head(top_n)
