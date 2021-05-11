"""
Utils to reshape data to understand language aggregates.
For example, count and percentage of posts in a given language.

"""
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
    df_lang_agg = df_lang_agg.rename(columns={'cumulative_percent': f'cumulative_percent{suffixes[0]}'})

    df_lang_agg['count_diff'] = (
            df_lang_agg[f'count{suffixes[1]}'] - df_lang_agg[f'count{suffixes[0]}']
    )
    df_lang_agg['percent_diff'] = (
            df_lang_agg[f'percent{suffixes[1]}'] - df_lang_agg[f'percent{suffixes[1]}']
    )
    return df_lang_agg

