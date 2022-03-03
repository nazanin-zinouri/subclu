"""
Utilities to compress embeddings and prep for visualizations
"""
import pandas as pd
import numpy as np


def add_metadata_to_tsne(
        tsne_array: np.array,
        df_v_sub: pd.DataFrame,
        df_sub_meta: pd.DataFrame,
        df_labels: pd.DataFrame = None,
        l_ix_sub: iter = None,
        l_cols_to_fill: iter = None,
        l_cols_labels: iter = None,
) -> pd.DataFrame:
    """append metadata to raw tsne array, assuming we haven't changed
    the order of the array (df_v_sub)
    """
    if l_ix_sub is None:
        l_ix_sub = ['subreddit_name', 'subreddit_id' ,]
    if l_cols_to_fill is None:
        l_cols_to_fill = [
            'primary_topic',
            'rating_name',
            'whitelist_status',
            'over_18',

            'subreddit_language',
            'primary_post_language',
            'geo_relevant_countries',

            'primary_post_type',
        ]
    if all([l_cols_labels is None, df_labels is not None]):
        l_cols_labels = l_ix_sub + ['model_sort_order'] + [
            c for c in df_labels.columns if c.endswith('_label')
        ]

    df_emb_svd2 = pd.DataFrame(tsne_array)
    df_emb_svd2 = df_emb_svd2.rename(columns={c: f"tsne_{c}" for c in df_emb_svd2.columns})
    # print(df_emb_svd2.shape)

    df_emb_svd2_meta = (
        df_v_sub[l_ix_sub]
        .merge(
            df_emb_svd2,
            how='right',
            left_index=True,
            right_index=True,
        )
        .merge(
            df_sub_meta,
            how='left',
            left_on=l_ix_sub,
            right_on=l_ix_sub,
        )
    )
    if df_labels is not None:
        df_emb_svd2_meta = df_emb_svd2_meta.merge(
            df_labels[l_cols_labels],
            how='left',
            on=l_ix_sub,
        )
    # null values create problems in plotly
    for c_ in l_cols_to_fill:
        df_emb_svd2_meta[c_] = df_emb_svd2_meta[c_].fillna('null')

    # print(df_emb_svd2_meta.shape)

    return df_emb_svd2_meta


#
# ~ fin
#
