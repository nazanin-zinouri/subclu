"""
Utilities to explore geo & cultural relevance metrics

Note that this requires ipython/jupyter, which may not be listed as a
requirement in some VMs
"""
from typing import Union

import pandas as pd
from IPython.display import display

from .eda import (
    style_df_numeric, reorder_array,
    # counts_describe, value_counts_and_pcts,
)


def show_geo_score_for_sub_single_table_new_metrics(
        subreddit: str,
        df_geo_baseline: pd.DataFrame,
        df_geo_new: pd.DataFrame,
        df_lang_by_subreddit: pd.DataFrame,
        l_cols_base_merge: iter = None,
        l_cols_new_pcts: iter = None,
        l_cols_lang_single: iter = None,
        col_sort_by: str = 'e_users_percent_by_country_standardized',
        pct_cols='default',
        top_n_pct_subreddit: int = 5,
        top_n_pct_country: int = 5,
        return_merged_df: bool = False,
) -> Union[None, pd.DataFrame]:
    sub_ = subreddit
    """display geo-relevance scores for input sub
    include the standardized geo-relevance score
    """
    # ========================
    # Define default columns
    # ===
    if l_cols_base_merge is None:
        l_cols_base_merge = [
            # 'subreddit_id',
            'subreddit_name',
            'country_name',
        ]

    if l_cols_new_pcts is None:
        l_cols_new_pcts = l_cols_base_merge + [
            'b_users_percent_by_subreddit',
            'c_users_percent_by_country',
            'd_users_percent_by_country_rank',
            'e_users_percent_by_country_standardized',
            'users_percent_by_country_avg',
            'num_of_countries_with_visits_l28',
            'users_in_subreddit_from_country_l28',
            # 'total_users_in_subreddit_l28',
            'total_users_in_country_l28',
        ]

    if l_cols_lang_single is None:
        l_cols_lang_single = [
            'subreddit_name',
            'language_name',
            'language_percent',
            'language_rank',
            'language_count',
            'weighted_language',
        ]

    if pct_cols == 'default':
        pct_cols = [
            'b_users_percent_by_subreddit',
            'c_users_percent_by_country',
            'users_percent_by_country_avg',
        ]
        pct_labels = ''
    else:
        pct_labels = None

    print(f"\n\n=== Subreddit: {sub_} ===")
    print(f"\nTop languages, by post [L28]")
    display(
        style_df_numeric(
            df_lang_by_subreddit[df_lang_by_subreddit['subreddit_name'] == sub_]
            .sort_values(by=['language_rank'], ascending=True)
            [l_cols_lang_single]
            .head(5),
            rename_cols_for_display=True,
            int_cols=False,
            l_bar_simple=['b_users_percent_by_subreddit',
                          'c_users_percent_by_country',
                          'd_users_percent_by_country_rank',
                          ],
        ).hide_index()
    )

    # print(f"Geo-relevance default [40% users in subreddit, daily]")
    # We don't need additional filters in df_base_ because we assume that it already only
    #  includes subreddits that meet the 40% criteria
    df_base_ = (
        df_geo_baseline[df_geo_baseline['subreddit_name'] == sub_]
        [l_cols_base_merge]
        .assign(geo_relevance_default=True)
    )
    if len(df_base_) == 0:
        # Need to create a df in case no geo-relevant countries
        df_base_ = pd.DataFrame(columns=l_cols_base_merge + ['geo_relevance_default'])

    # print(f"\nTop by % of subreddit [L28]")
    s_top_pct_sub = (
        df_geo_new[df_geo_new['subreddit_name'] == sub_]
        .sort_values(by=['b_users_percent_by_subreddit'], ascending=False)
        ['country_name']
        .head(top_n_pct_subreddit)
    )

    # print(f"\nTop by % of country [L28]")
    s_top_pct_country = (
        df_geo_new[df_geo_new['subreddit_name'] == sub_]
        .sort_values(by=['c_users_percent_by_country'], ascending=False)
        ['country_name']
        .head(top_n_pct_country)
    )

    # print(f"\nTop by % of country [L28]")
    s_top_pct_country_standard = (
        df_geo_new[df_geo_new['subreddit_name'] == sub_]
        .sort_values(by=['e_users_percent_by_country_standardized'], ascending=False)
        ['country_name']
        .head(top_n_pct_subreddit)
    )

    df_top_new_metrics = (
        df_geo_new[
            (df_geo_new['subreddit_name'] == sub_) &
            (df_geo_new['country_name'].isin(
                set(s_top_pct_sub) | set(s_top_pct_country) |
                set(s_top_pct_country_standard)
            ))
            ]
        [l_cols_new_pcts]
    )

    # merge 2 dfs together
    df_merged = df_base_.merge(
        df_top_new_metrics,
        how='outer',
        on=l_cols_base_merge,
    ).fillna({'country_name': 'NULL', }).fillna(False)

    df_merged = df_merged[
        reorder_array(l_cols_base_merge, df_merged.columns)
    ]

    display(
        style_df_numeric(
            df_merged
            .sort_values(by=[col_sort_by], ascending=False)
            .reset_index(drop=True)
            ,
            rename_cols_for_display=True,
            int_cols=False,
            pct_cols=pct_cols,
            pct_labels=pct_labels,
            pct_digits=3,
            l_bar_simple=['b_users_percent_by_subreddit',
                          'c_users_percent_by_country',
                          'd_users_percent_by_country_rank',
                          'e_users_percent_by_country_standardized',
                          ],
        )  # .hide_index()
    )
    if return_merged_df:
        return df_merged


#
# ~ fin
#
