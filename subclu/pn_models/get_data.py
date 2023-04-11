"""
Utilities & queryes to get data needed for PN models
"""
from typing import Any, Union
import json

import polars as pl
import numpy as np


def query_user_tos() -> str:
    """Generate query to get user Time on subreddit
    We can add parameter later if needed to extend windows.
    Ideally we could convert to rows in BQ, but I couldn't get to do it
    after trying for ~30 mins, so process it in python.
    """
    q_ = """
    
    """
    return q_


def reshape_tos_for_df(
        user_id: str,
        tos_str_dict: str,
        tos_col_name: str = 'tos_pct',
) -> dict:
    """Take the nested dict in a df and reshape it so that we can get a long df
    where each row is a user+subreddit Time on Sub percentage
    """
    d_tos_in = json.loads(tos_str_dict)

    d_out = {
        'user_id': [user_id] * len(d_tos_in),
        'subreddit_id': list(),
        tos_col_name: list(),
    }

    for sub_id, tos_pc in d_tos_in.items():
        d_out['subreddit_id'].append(sub_id)
        d_out[tos_col_name].append(tos_pc)

    return d_out


def reshape_embeddings_for_df(
        subreddit_id: str,
        subreddit_name: str,
        embeddings: np.array,
        embedding_col_prefix: str = 'embedding',
) -> dict:
    """
    Take the nested embedding data and convert it to a flat df so it's easy to manipulate & multiply
    """
    d_out = {
        'subreddit_id': subreddit_id,
        'subreddit_name': subreddit_name,
    }

    for i, emb_ in enumerate(embeddings):
        d_out[f"{embedding_col_prefix}{i:03,.0f}"] = emb_

    return d_out


def delayed_select_for_polars(
        pl_df: pl.LazyFrame,
        select_kwargs: Any,
) -> pl.DataFrame:
    """Use this function as a fix for polars.lazy() that breaks when selecting an unnested struct
    If we apply this function with dask.delayed() we can compute the long df_tos in 10 parallel jobs(!)

    dask -> compute()
    polars -> collect()
    """
    return (
        pl_df
        .collect()
        .select(
            select_kwargs
        )
    )
