"""
Utilities to explore geo & cultural relevance metrics

Note that this requires ipython/jupyter, which may not be listed as a
requirement in some VMs
"""
from functools import partial
from typing import Union

import pandas as pd
from IPython.display import display

from .eda import (
    style_df_numeric, reorder_array,
    # counts_describe, value_counts_and_pcts,
)


def color_boolean(val):
    if val == True:
        return "background-color: #01B500; font-weight: bold;"  # green
    else:
        # if False, lighten the color of the text
        return "color: #8f8f8f"


def highlight_b(val, threshold=0.15):
    if val >= threshold:
        return "color:purple; font-weight: bold;"
    else:
        return ''


def highlight_e(val, threshold=2.0):
    if val >= threshold:
        return "color:purple; font-weight: bold;"
    else:
        return ''


def display_top_subs_in_country(
        country: str,
        df_geo_new: pd.DataFrame,
        col_sort: str,
        l_cols_country_disp: iter = None,
        top_n: int = 20,
):
    """Display top subs for each country sorted by input col"""
    if l_cols_country_disp is None:
        l_cols_country_disp = [
            'subreddit_name',
            # 'subreddit_id',
            # 'geo_country_code',
            'country_name',
            # 'geo_region',
            'b_users_percent_by_subreddit',
            'c_users_percent_by_country',
            'd_users_percent_by_country_rank',
            'e_users_percent_by_country_standardized',
            'users_percent_by_country_avg',
            # 'users_percent_by_country_stdev',
            'num_of_countries_with_visits_l28',
            'users_in_subreddit_from_country_l28',
            'total_users_in_country_l28',
            # 'total_users_in_subreddit_l28',
            # 'over_18',
            # 'verdict',
            # 'type',
        ]

    display(
        style_df_numeric(
            df_geo_new
            [df_geo_new['country_name'] == country]
            .sort_values(by=[col_sort], ascending=False)
            [l_cols_country_disp]
            .head(top_n)
            .reset_index(drop=True)
            ,
            rename_cols_for_display=True,
            int_cols=False,
            pct_digits=3,
            pct_cols=['b_users_percent_by_subreddit',
                      'c_users_percent_by_country',
                      'users_percent_by_country_avg',
                      ],
            pct_labels='',
            l_bar_simple=['b_users_percent_by_subreddit',
                          'c_users_percent_by_country',
                          'e_users_percent_by_country_standardized',
                          ],
        )  # .hide_index()
    )


def show_geo_score_for_sub_single_table_new_metrics(
        subreddit_name: str,
        df_geo_new: pd.DataFrame,
        df_geo_baseline: pd.DataFrame = None,
        df_lang_by_subreddit: pd.DataFrame = None,
        l_cols_base_merge: iter = None,
        l_cols_new_pcts: iter = None,
        l_cols_lang_single: iter = None,
        col_sort_by: str = 'e_users_percent_by_country_standardized',
        pct_cols='default',
        top_n_pct_subreddit: int = 5,
        top_n_pct_country: int = 5,
        return_merged_df: bool = False,
        col_a: str = 'geo_relevance_default',
        col_b: str = 'b_users_percent_by_subreddit',
        col_e: str = 'e_users_percent_by_country_standardized',
        col_new_score_combined: str = 'relevance_combined_score',
        col_new_score_tier: str = 'relevance_glocal_tier',
        col_b_bool: str = 'relevance_percent_by_subreddit',
        col_e_bool: str = 'relevance_percent_by_country_standardized',
        b_threshold: float = 0.15,
        e_threshold: float = 2.0,
        print_section_titles: bool = True,
        rename_cols_for_mode: bool = False,
        l_cols_to_front: iter = None,
) -> Union[None, pd.DataFrame]:
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
    if l_cols_to_front is None:
        l_cols_to_front = l_cols_base_merge + [
            col_new_score_tier,
            col_new_score_combined,
            col_b,
            col_e,
            col_a,
            col_b_bool,
            col_e_bool,
            'users_in_subreddit_from_country_l28',
            'num_of_countries_with_visits_l28',
        ]

    if l_cols_new_pcts is None:
        l_cols_new_pcts = l_cols_base_merge + [
            'relevance_percent_by_subreddit',
            'relevance_percent_by_country_standardized',
            'b_users_percent_by_subreddit',
            'e_users_percent_by_country_standardized',
            'c_users_percent_by_country',
            'd_users_percent_by_country_rank',
            'users_percent_by_country_avg',
            'users_in_subreddit_from_country_l28',
            # 'total_users_in_subreddit_l28',
            'total_users_in_country_l28',
            'num_of_countries_with_visits_l28',
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

    if print_section_titles:
        print(f"\n\n=== Subreddit: {subreddit_name} ===")

    if df_lang_by_subreddit is not None:
        if print_section_titles:
            print(f"\nTop languages, by post [L28]")
        display(
            style_df_numeric(
                df_lang_by_subreddit[df_lang_by_subreddit['subreddit_name'] == subreddit_name]
                    .sort_values(by=['language_rank'], ascending=True)
                [l_cols_lang_single]
                    .head(5),
                rename_cols_for_display=True,
                int_cols=False,
                l_bar_simple=['b_users_percent_by_subreddit', 'c_users_percent_by_country'],
            ).hide_index()
        )

    # print(f"Geo-relevance default [40% users in subreddit, daily]")
    # We don't need additional filters in df_base_ because we assume that it already only
    #  includes subreddits that meet the 40% criteria
    if df_geo_baseline is not None:
        df_base_ = (
            df_geo_baseline[df_geo_baseline['subreddit_name'] == subreddit_name]
            [l_cols_base_merge]
            .assign(geo_relevance_default=True)
        )
    else:
        # if we didn't get a df_baseline, we should expect the baseline col to be in df_geo_new
        df_base_ = (
            df_geo_new[
                (df_geo_new['subreddit_name'] == subreddit_name) &
                (df_geo_new['geo_relevance_default'] == True)
            ]
            [l_cols_base_merge]
            .assign(geo_relevance_default=True)
        )
    if len(df_base_) == 0:
        # Need to create a df in case no geo-relevant subs in country
        df_base_ = pd.DataFrame(columns=l_cols_base_merge + ['geo_relevance_default'])

    # print(f"\nTop by % of subreddit [L28]")
    s_top_pct_sub = (
        df_geo_new[df_geo_new['subreddit_name'] == subreddit_name]
        .sort_values(by=['b_users_percent_by_subreddit'], ascending=False)
        ['country_name']
        .head(top_n_pct_subreddit)
    )

    # print(f"\nTop by % of country [L28]")
    s_top_pct_country = (
        df_geo_new[df_geo_new['subreddit_name'] == subreddit_name]
        .sort_values(by=['c_users_percent_by_country'], ascending=False)
        ['country_name']
        .head(top_n_pct_country)
    )

    # print(f"\nTop by % of country [L28]")
    s_top_pct_country_standard = (
        df_geo_new[df_geo_new['subreddit_name'] == subreddit_name]
        .sort_values(by=['e_users_percent_by_country_standardized'], ascending=False)
        ['country_name']
        .head(top_n_pct_subreddit)
    )

    df_top_new_metrics = (
        df_geo_new[
            (df_geo_new['subreddit_name'] == subreddit_name) &
            (df_geo_new['country_name'].isin(
                set(s_top_pct_sub) | set(s_top_pct_country) |
                set(s_top_pct_country_standard)
            ))
            ]
        # Exclude geo_relevance default b/c it could create a duplicate column
        [[c for c in l_cols_new_pcts if 'geo_relevance_default' not in c]]
    )

    # merge 2 dfs together
    df_merged = df_base_.merge(
        df_top_new_metrics,
        how='outer',
        on=l_cols_base_merge,
    ).fillna({'country_name': 'NULL', }).fillna(False)

    df_merged = df_merged[
        # Move some cols to front
        reorder_array(l_cols_to_front, df_merged.columns)
    ]

    l_bar_simple = [
        'b_users_percent_by_subreddit',
        'c_users_percent_by_country',
        'd_users_percent_by_country_rank',
        'e_users_percent_by_country_standardized',
        col_new_score_combined,
    ]
    l_bar_simple = [c for c in l_bar_simple if c in df_merged.columns]
    if rename_cols_for_mode:
        d_rename_cols_sub_examples = {
            col_a: col_a.replace('_', '<br>'),
            col_b_bool: col_b_bool.replace('_', '<br>'),
            col_e_bool: col_e_bool.replace('_', '<br>'),
            col_b: 'b users<br>percent by<br>subreddit',
            col_e: 'e users<br>percent by<br>country<br>standardized',
            'c_users_percent_by_country': 'c users<br>percent by<br>country',
            'd_users_percent_by_country_rank': 'd users<br>percent by<br>country rank',

            'users_percent_by_country_avg': 'users<br>percent_by<br>country_avg',
            'users_in_subreddit_from_country_l28': 'users<br>in_subreddit<br>from_country_l28',
            'total_users_in_subreddit_l28': 'total_users<br>in_subreddit_l28',
            'total_users_in_country_l28': 'total users<br>in country l28',
            'num_of_countries_with_visits_l28': 'num_of<br>countries_with<br>visits_l28',
        }
        df_merged = df_merged.rename(columns=d_rename_cols_sub_examples)

        pct_cols = [d_rename_cols_sub_examples.get(c, c) for c in pct_cols]
        col_sort_by = d_rename_cols_sub_examples[col_sort_by]
        l_bar_simple = [d_rename_cols_sub_examples.get(c, c) for c in l_bar_simple]
        l_bool_style_map_ = [
            d_rename_cols_sub_examples[col_a],
            d_rename_cols_sub_examples[col_b_bool],
            d_rename_cols_sub_examples[col_e_bool],
        ]
        l_b_style_map_ = [d_rename_cols_sub_examples[col_b]]
        l_e_style_map_ = [d_rename_cols_sub_examples[col_e]]
    else:
        l_bool_style_map_ = [
            col_a.replace('_', ' '),
            col_b_bool.replace('_', ' '),
            col_e_bool.replace('_', ' '),
        ]
        l_b_style_map_ = [col_b.replace('_', ' ')]
        l_e_style_map_ = [col_e.replace('_', ' ')]

    partial_b = partial(highlight_b, threshold=b_threshold)
    partial_e = partial(highlight_e, threshold=e_threshold)
    display(
        style_df_numeric(
            df_merged
            .sort_values(by=[col_sort_by], ascending=False)
            .reset_index(drop=True)
            ,
            rename_cols_for_display=True,
            # int_cols=False,
            int_labels=[
                'total_users_in', 'users_in_subreddit_from_country_l28',
                'by_country_rank', 'users_l7',  'in_subreddit_l28',
                'country rank', 'countries_with<br>visits_l28', 'not_removed<br>l28',
                'from_country_l28', 'in country l28',
            ],
            pct_cols=pct_cols,
            pct_labels=pct_labels,
            pct_digits=3,
            l_bar_simple=l_bar_simple,
        )
        .applymap(color_boolean, subset=l_bool_style_map_)
        .applymap(partial_b, subset=l_b_style_map_)
        .applymap(partial_e, subset=l_e_style_map_)
        # .hide_index()
    )
    if return_merged_df:
        return df_merged


def show_geo_score_for_sub_single_table(
        subreddit_name: str,
        df_geo_baseline: pd.DataFrame,
        df_geo_new: pd.DataFrame,
        df_lang_by_subreddit: pd.DataFrame,
        l_cols_base_merge: iter = None,
        l_cols_new_pcts: iter = None,
        l_cols_lang_single: iter = None,
        col_sort_by: str = 'b_users_percent_by_subreddit',
        pct_cols='default',
        top_n_pct_subreddit: int = 5,
        top_n_pct_country: int = 5,
) -> None:
    """display geo-relevance scores for input sub. OLD/DEPRECATED!
    use `show_geo_score_for_subreddit_namesingle_table_new_metrics` instead
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

    print(f"\n\n=== Subreddit: {subreddit_name} ===")
    print(f"\nTop languages, by post [L28]")
    display(
        style_df_numeric(
            df_lang_by_subreddit[df_lang_by_subreddit['subreddit_name'] == subreddit_name]
            .sort_values(by=['language_rank'], ascending=True)
            [l_cols_lang_single]
            .head(5),
            rename_cols_for_display=True,
            int_cols=False,
            l_bar_simple=['b_users_percent_by_subreddit', 'c_users_percent_by_country'],
        ).hide_index()
    )

    # print(f"Geo-relevance default [40% users in subreddit, daily]")
    df_base_ = (
        df_geo_baseline[df_geo_baseline['subreddit_name'] == subreddit_name]
        [l_cols_base_merge]
        .assign(geo_relevance_default=True)
    )
    if len(df_base_) == 0:
        df_base_ = pd.DataFrame(columns=l_cols_base_merge + ['a_geo_relevance_default'])

    # print(f"\nTop by % of subreddit [L28]")
    s_top_pct_sub = (
        df_geo_new[df_geo_new['subreddit_name'] == subreddit_name]
        .sort_values(by=['b_users_percent_by_subreddit'], ascending=False)
        ['country_name']
        .head(top_n_pct_subreddit)
    )

    # print(f"\nTop by % of country [L28]")
    s_top_pct_country = (
        df_geo_new[df_geo_new['subreddit_name'] == subreddit_name]
        .sort_values(by=['c_users_percent_by_country'], ascending=False)
        ['country_name']
        .head(top_n_pct_country)
    )

    df_top_new_metrics = (
        df_geo_new[
            (df_geo_new['subreddit_name'] == subreddit_name) &
            (df_geo_new['country_name'].isin(set(s_top_pct_sub.to_list()) | set(s_top_pct_country.to_list())))
            ]
        [l_cols_new_pcts]
    )

    # merge 2 dfs together
    df_merged = df_base_.merge(
        df_top_new_metrics,
        how='outer',
        on=l_cols_base_merge,
    ).fillna(False)
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


def show_geo_score_for_sub(
        subreddit: str,
        df_geo_baseline: pd.DataFrame,
        df_geo_new: pd.DataFrame,
        df_lang_by_subreddit: pd.DataFrame,
        l_cols_base_disp_check: iter = None,
        l_cols_geo_new_disp_check: iter = None,
        l_cols_lang_disp_check: iter = None,
        top_n_pct_subreddit: int = 5,
        top_n_pct_country: int = 5,
) -> None:
    sub_ = subreddit
    """display geo-relevance scores for input sub"""
    if l_cols_base_disp_check is None:
        l_cols_base_disp_check = [
            'subreddit_id',
            'subreddit_name',
            'geo_country_code',
            'country_name',
            'users_l7',
            'posts_not_removed_l28',
        ]

    if l_cols_lang_disp_check is None:
        l_cols_lang_disp_check = [
            'subreddit_id',
            'subreddit_name',
            'language_name',
            'language_percent',
            'language_rank',
            'language_count',
            'weighted_language',

            # 'langauge_in_use_multilingual',
        ]
    if l_cols_geo_new_disp_check is None:
        l_cols_geo_new_disp_check = [
            'subreddit_name',
            'country_name',
            'c_users_percent_by_country',
            'b_users_percent_by_subreddit',
            'users_in_subreddit_from_country_l28',
            'total_users_in_country_l28',
            'total_users_in_subreddit_l28',
        ]

    print(f"\n\n=== Subreddit: {sub_} ===")
    print(f"Geo-relevance default [40% users in subreddit, daily]")
    display(
        style_df_numeric(
            df_geo_baseline[df_geo_baseline['subreddit_name'] == sub_]
            [l_cols_base_disp_check],
            rename_cols_for_display=True,
            int_cols=False,
        ).hide_index()
    )
    print(f"\nTop languages, by post [L28]")
    display(
        style_df_numeric(
            df_lang_by_subreddit[df_lang_by_subreddit['subreddit_name'] == sub_]
            .sort_values(by=['language_rank'], ascending=True)
            [l_cols_lang_disp_check]
            .head(5),
            rename_cols_for_display=True,
            int_cols=False,
            l_bar_simple=['b_users_percent_by_subreddit', 'c_users_percent_by_country'],
        ).hide_index()
    )
    print(f"\nTop by % of subreddit [L28]")
    display(
        style_df_numeric(
            df_geo_new[df_geo_new['subreddit_name'] == sub_]
            .sort_values(by=['b_users_percent_by_subreddit'], ascending=False)
            [l_cols_geo_new_disp_check]
            .head(top_n_pct_subreddit),
            rename_cols_for_display=True,
            int_cols=False,
            l_bar_simple=['b_users_percent_by_subreddit', 'c_users_percent_by_country'],
        ).hide_index()
    )
    print(f"\nTop by % of country [L28]")
    display(
        style_df_numeric(
            df_geo_new[df_geo_new['subreddit_name'] == sub_]
            .sort_values(by=['c_users_percent_by_country'], ascending=False)
            [l_cols_geo_new_disp_check]
            .head(top_n_pct_country),
            rename_cols_for_display=True,
            int_cols=False,
            pct_digits=3,
            l_bar_simple=['b_users_percent_by_subreddit', 'c_users_percent_by_country'],
        ).hide_index()
    )


#
# ~ fin
#
