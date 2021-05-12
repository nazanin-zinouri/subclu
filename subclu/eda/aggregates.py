"""
Utils to reshape data to understand language aggregates.
For example, count and percentage of posts in a given language.

"""
import numpy as np
import pandas as pd

from ..utils.eda import value_counts_and_pcts


def compare_raw_v_weighted_language(
        df: pd.DataFrame,
        col_lang_raw: str = 'language',
        col_lang_weighted: str = 'weighted_language',
        suffixes: tuple = ('_raw', '_weigthed')
) -> pd.DataFrame:
    """"""

    df_lang_agg = (
        value_counts_and_pcts(
            df,
            [col_lang_raw],
            top_n=None,
            return_df=True,
            cumsum=True,
        )
        .merge(
            value_counts_and_pcts(
                df,
                [col_lang_weighted],
                top_n=None,
                return_df=True,
                cumsum=False,
            ),
            how='outer',
            suffixes=suffixes,
            left_index=True,
            right_index=True,
        )
    )
    df_lang_agg = (
        df_lang_agg
        .rename(columns={'cumulative_percent': f'cumulative_percent{suffixes[0]}'})
        .sort_values(by=['cumulative_percent_raw'], ascending=True)
    )

    df_lang_agg['count_diff'] = (
            df_lang_agg[f'count{suffixes[1]}'] - df_lang_agg[f'count{suffixes[0]}']
    )
    df_lang_agg['percent_diff'] = (
            df_lang_agg[f'percent{suffixes[1]}'] - df_lang_agg[f'percent{suffixes[0]}']
    )
    return df_lang_agg


def get_language_by_sub_wide(
        df: pd.DataFrame,
        col_sub_name: str = 'subreddit_name',
        col_lang_weighted: str = 'weighted_language_top',
        col_total_posts: str = 'total_posts_count',
) -> pd.DataFrame:
    """"""
    df_lang_by_sub = value_counts_and_pcts(
        df,
        [col_sub_name, col_lang_weighted],
        top_n=None,
        return_df=True,
        cumsum=False,
        sort_index_ascending=True,
    ).drop('percent', axis=1).reset_index()

    # convert from long to wide
    df_lang_by_sub = df_lang_by_sub.set_index([col_sub_name, col_lang_weighted]).unstack()
    df_lang_by_sub.columns = df_lang_by_sub.columns.droplevel(0)
    df_lang_by_sub.columns.name = None
    df_lang_by_sub = df_lang_by_sub.rename(
        columns={c: f"{c}_count" for c in df_lang_by_sub.columns if c != col_total_posts}
    )

    # Get totals & percents
    df_lang_by_sub = df_lang_by_sub.fillna(0)
    df_lang_by_sub[col_total_posts] = np.sum(df_lang_by_sub, axis=1)

    for cc in [c for c in df_lang_by_sub.columns if ((c.endswith('_count')) and (c != col_total_posts))]:
        df_lang_by_sub[cc.replace('_count', '_percent')] = (
                df_lang_by_sub[cc] /
                df_lang_by_sub[col_total_posts]
        )

    return df_lang_by_sub.sort_values(
        by=col_total_posts, ascending=False
    )


def get_language_by_sub_long(
        df: pd.DataFrame,
        col_sub_name: str = 'subreddit_name',
        col_lang_weighted: str = 'weighted_language_top',
        col_total_posts: str = 'total_posts_count',
) -> pd.DataFrame:
    # Way 2: keep as long & create percentage ROWS before converting to cols
    #  This way is preferred to create plots
    df_lang_by_sub = value_counts_and_pcts(
        df,
        [col_sub_name, col_lang_weighted],
        top_n=None,
        return_df=True,
        cumsum=False,
        sort_index_ascending=True,
    ).drop('percent', axis=1).reset_index()

    # for each sub get the total posts, then percent of posts per language
    for sub_ in df_lang_by_sub[col_sub_name].unique():
        mask_sub = df_lang_by_sub[col_sub_name] == sub_

        sub_total_sum = np.sum(df_lang_by_sub[mask_sub]['count'])

        df_lang_by_sub.loc[mask_sub, col_total_posts] = sub_total_sum

        df_lang_by_sub.loc[mask_sub,'percent'] = (
            df_lang_by_sub[mask_sub]['count'] / sub_total_sum
        )

    df_lang_by_sub = df_lang_by_sub.sort_values(
        by=[col_total_posts, col_sub_name],
        ascending=[False, True]
    )
    return df_lang_by_sub

#
# ~ fin
#
