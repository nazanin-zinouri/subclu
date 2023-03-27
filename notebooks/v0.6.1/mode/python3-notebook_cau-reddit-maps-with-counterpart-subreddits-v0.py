# Python Notebook - [CAU] Reddit Maps with Counterpart Subreddits [v0]

datasets

# looks like jupyter-dash is already isntalled, but it doesn't seem to work with inline hosting :/
# !pip install jupyter-dash -t "/tmp"

import logging
from typing import Union, List, Any, Optional, Tuple, Dict

import numpy as np
import pandas as pd
from pandas.io.formats.style import Styler

from jupyter_dash import JupyterDash
import plotly
import plotly.express as px

# ===============
# Logging & Misc
# ===
def reorder_array(
        items_to_front: list,
        array
):
    """
    if the array is a dataframe, re-order the columns
    if array is list-like, re-order it
    """
    try:
        return items_to_front + array.columns.drop(items_to_front).to_list()
    except AttributeError:
        set_found = set(items_to_front) & set(array)
        set_missing = set(items_to_front) - set_found
        if len(set_found) != len(items_to_front):
            logging.warning(f"Values missing {len(set_missing)}:\n{set_missing}")
            items_to_front = [itm for itm in items_to_front if itm in set_found]

        return items_to_front + [itm for itm in array if itm not in items_to_front]


# ===============
# Utils to improve EDA functions with pandas
# ===
def value_counts_and_pcts(
        df: Union[pd.DataFrame, pd.Series],
        cols: Union[str, List[str]] = None,
        count_type: str = None,
        sort_index: bool = False,
        index_group_cols: List[str] = None,
        pct_digits: int = 1,
        return_df: bool = False,
        top_n: Optional[int] = 15,
        cumsum: bool = True,
        cumsum_count: bool = False,
        add_col_prefix: bool = True,
        bar_pct: bool = True,
        bar_cumsum: bool = True,
        reset_index: bool = False,
        sort_index_ascending: bool = False,
        observed: bool = True,
        rename_cols_for_display: bool = False,
        int_labels: List[str] = None,
) -> Union[Styler, pd.DataFrame]:
    """
    Get value counts for a column in df as count & percentage in a single df
    If cols = a list of more than one column, then create counts grouped by
      these columns.
    If return_df=False,
        returns a Styler object (object to render HTML/CSS for displaying in jupyter)
    else
        return a df

    Args:
        df:
        cols:
            if one column, then do value_counts on it.
            if multiple columns, then groupby those columns & count them
        count_type:
        sort_index:
        index_group_cols:
        pct_digits:
        return_df:
        top_n:
        cumsum:
        cumsum_count:
        add_col_prefix:
        bar_pct:
        bar_cumsum:
        reset_index:
        sort_index_ascending:
        observed:
        rename_cols_for_display:
        int_labels:

    Returns:
    """
    if isinstance(cols, str):
        col = cols
        series = df[col]
    elif isinstance(df, pd.Series):
        col = df.name
        series = df
    else:
        col = None

    # labels are column suffixes if one col is evaluated
    # labels are column names if we're counting multiple columns
    if count_type is None:
        count_label = 'count'
        col_cumsum_count = 'cumulative_sum'
        pct_label = 'percent'
        cumsum_col = f"cumulative_percent"
    else:
        count_label = f"{count_type}_count"
        col_cumsum_count = f'cumulative_sum_of_{count_type}'
        pct_label = f"percent_of_{count_type}"
        cumsum_col = f"cumulative_percent_of_{count_type}"

    if (col is not None) & add_col_prefix:
        if count_type is None:
            cumsum_col = f"{col}-pct_cumulative_sum"
            col_cumsum_count = f"{col}-cumulative_sum"
        else:
            cumsum_col = f"{col}-cumulative_percent_of_{count_type}"
            col_cumsum_count = f"{col}-cumulative_sum_of_{count_type}"

    if col is not None:
        df_out = (
            series.value_counts(dropna=False)
            .to_frame()
            .merge((series.value_counts(dropna=False, normalize=True)),
                   how='outer', left_index=True, right_index=True,
                   suffixes=(f"-{count_label}", f"-{pct_label}")
                   )
        )
        if add_col_prefix:
            col_count = f"{col}-{count_label}"
            col_pct = f"{col}-{pct_label}"
        else:
            col_count = count_label
            col_pct = pct_label

            df_out = df_out.rename(
                columns={c: c.replace(f'{col}-', '') for c in df_out.columns}
            )

    else:
        # add .fillna('null') so that we can see null values by default
        #  if we don't add it, .size() will drop pd.nan values from value counts
        # we need a try/except in case some of the columns are `category` type
        try:
            df_out = (
                df[cols].fillna('null')
                .groupby(cols, observed=observed).size()
                .to_frame()
                .rename(columns={0: count_label})
                .sort_values(by=[count_label], ascending=False)
            )
        except ValueError:
            df_out = (
                df[cols]
                .groupby(cols, observed=observed).size()
                .to_frame()
                .rename(columns={0: count_label})
                .sort_values(by=[count_label], ascending=False)
            )
        df_out[pct_label] = df_out[count_label] / df_out[count_label].sum()
        col_count = count_label
        col_pct = pct_label

    df_out = df_out.head(top_n)

    # Sort index BEFORE calculating cum-sum
    if sort_index & (index_group_cols is None):
        df_out = df_out.sort_index(ascending=sort_index_ascending)
    elif sort_index & (index_group_cols is not None):
        df_out = sort_by_grouped_cols(
            df=df_out.reset_index(),
            sort_cols=[count_label],
            index_group_cols=index_group_cols,
            ascending=sort_index_ascending,
        )
    if cumsum_count:
        df_out[col_cumsum_count] = df_out[col_count].cumsum()
    if cumsum:
        df_out[cumsum_col] = df_out[col_pct].cumsum()

    # set formatting for integer cols
    if int_labels is None:
        int_labels = ['count', 'len']
    native_int_cols = {c for c in df_out.select_dtypes(include=['int']).columns}
    all_num_cols = {c for c in df_out.select_dtypes('number').columns} - native_int_cols
    int_cols = (
            native_int_cols |
            {c for c in all_num_cols if any(lbl in c for lbl in int_labels)} |
            {c for c in df_out.columns if count_label in c}
    )
    d_format = {c: "{:,.0f}" for c in int_cols}
    d_format.update({c: ''.join(["{", f":.{pct_digits}%", "}"])
                     for c in df_out.columns
                     if any(lbl in c for lbl in [pct_label, cumsum_col])
                     })

    # Reset index AFTER setting formatting for numeric cols in case index is
    #  string, but might match a numberic col pattern
    if reset_index & (col is not None):
        df_out = df_out.reset_index().rename(columns={'index': col})
    elif reset_index:
        df_out = df_out.reset_index()

    if rename_cols_for_display:
        df_out = (
            df_out
            .rename(columns={c: c.replace('_', ' ') for c in df_out.columns})
        )
        d_format = {k.replace('_', ' '): v for k, v in d_format.items()}

        # also need to rename some labels & cols for styling downstream
        pct_label = pct_label.replace('_', ' ')
        cumsum_col = cumsum_col.replace('_', ' ')

        # rename index as well:
        if not reset_index:
            ix_rename = {ix: ix.replace('_', ' ') for ix in df_out.index.names}
            df_out = df_out.rename_axis(index=ix_rename)

    if return_df:
        return df_out
    else:
        bar_subset = list()
        if bar_pct:
            bar_subset.append([c for c in df_out.columns if pct_label in c][0])
        if bar_cumsum & cumsum:
            bar_subset.append(cumsum_col)

        if bar_subset:
            return df_out.style.format(d_format).bar(subset=bar_subset, color="#95cff5")
        else:
            return df_out.style.format(d_format)



def style_df_numeric(
        df: pd.DataFrame,
        int_labels: List[str] = None,
        pct_labels: List[str] = None,
        int_cols: Union[List[str], bool] = None,
        pct_cols: Union[List[str], bool] = None,
        int_format: str = "{:,.0f}",
        pct_digits: int = 2,
        float_round: int = 2,
        currency_cols: List[str] = None,
        currency_format: str = "${:,.2f}",
        d_custom_style: dict = None,
        fillna_numeric: Any = None,
        fillna_obj: Any = None,
        na_rep: str = '-',
        rename_cols_for_display: bool = False,
        rename_cols_pairs: List[Tuple] = None,
        l_bars: List[dict] = None,
        l_bar_simple: List[str] = None,
        verbose: bool = False,
) -> Styler:
    """
    Format integer & percent columns in a dataframe & return a Styler object
    for displaying it in jupyter.
    l_bar example:
    l_bar_charts = [
        {'subset': [
            'patients_count-bin', 'percent_of_patients-bin',
            'cumulative-percent_of_patients',
            'patients_count-bin-5fold CV',
        ],
         'color': '#95cff5',
        },
        {'subset': [
            'negative_predicted_percent-Chart Review',
            'negative_predicted_percent-Gray Zone',
        ],
         'color': djb_utils.palette_economist('red_light'),
         'normalize_subset': True,
        },
    ]

    Args:
        df:
        int_labels:
        pct_labels:
        int_cols:
        pct_cols:
        int_format:
        pct_digits:
        float_round:
        currency_cols:
        currency_format:
        d_custom_style:
        fillna_numeric:
        fillna_obj:
        na_rep:
        rename_cols_for_display:
            replace underscores so it's easier to view/display column names
        rename_cols_pairs:
        l_bars:
        l_bar_simple:
        verbose:

    Returns: pd.Styler
    """
    if int_labels is None:
        int_labels = ['count']
    if pct_labels is None:
        pct_labels = ['percent', '_pct', '-pct']

    native_int_cols = {c for c in df.select_dtypes(include=['int']).columns}
    all_num_cols = {c for c in df.select_dtypes('number').columns} - native_int_cols
    # TODO(djb): calculate set of int & pct cols by label upfront & then exclude int_cols & pct_cols
    #  from them so that int_cols & pct_cols always override int_labels & pct_labels

    if int_cols is True:
        int_cols = set(all_num_cols)
    elif int_cols is False:
        int_cols = native_int_cols
    elif int_cols is None:
        int_cols = native_int_cols | {c for c in all_num_cols if any(lbl in c for lbl in int_labels)}
    else:
        int_cols = (native_int_cols | set(int_cols) |
                    {c for c in all_num_cols if any(lbl in c for lbl in int_labels)})

    if pct_cols is True:
        pct_cols = set(all_num_cols)
    elif pct_cols is None:
        pct_cols = {c for c in all_num_cols if any(lbl in c for lbl in pct_labels)} - int_cols
    else:
        pct_cols = ((set(pct_cols) | {c for c in all_num_cols if any(lbl in c for lbl in pct_labels)}) -
                    int_cols)

    if currency_cols is not None:
        currency_cols = {c for c in currency_cols}
    else:
        currency_cols = set()

    float_cols = all_num_cols - int_cols - pct_cols - currency_cols

    if verbose:
        info(f"all_num_cols: {all_num_cols}\n"
             f"float_cols: {float_cols}\n"
             f"int_cols: {int_cols}\n"
             f"pct_cols: {pct_cols}\n"
             f"currency_cols: {currency_cols}\n"
             )

    d_format = {c: int_format for c in int_cols}
    d_format.update({c: f"{{:,.{pct_digits}%}}" for c in pct_cols})
    d_format.update({c: f"{{:,.{float_round}f}}" for c in float_cols})
    d_format.update({c: currency_format for c in currency_cols})

    if d_custom_style is not None:
        d_format.update(d_custom_style)

    if fillna_numeric is not None:
        df[all_num_cols] = df[all_num_cols].fillna(fillna_numeric)

    if fillna_obj is not None:
        df[df.drop(all_num_cols, axis=1).columns] = (
            df[df.drop(all_num_cols, axis=1).columns].fillna(fillna_obj)
        )

    if rename_cols_for_display:
        if rename_cols_pairs is None:
            # add a space after a dash so google sheets can display columns better
            rename_cols_pairs = [('_', ' '), ('-', '- ')]

        def rename_col_fxn(
                col: str,
                rename_cols_pairs: list = rename_cols_pairs
        ):
            """Given a list of tuples, apply them to replace string patterns in col name"""
            if col is None:
                # In case we get an un-named index
                return ''
            else:
                col_new = col
                for tpl in rename_cols_pairs:
                    col_new = col_new.replace(*tpl)
                return col_new

        d_format = {rename_col_fxn(k): v for k, v in d_format.items()}
        df = df.rename(columns={c: rename_col_fxn(c) for c in df.columns})

        # rename index as well
        ix_rename = {ix: rename_col_fxn(ix) for ix in df.index.names}
        df = df.rename_axis(index=ix_rename)

    if verbose:
        info(f"Format dictionary:\n  {d_format}")

    if l_bar_simple is not None:
        if rename_cols_for_display:
            l_bar_simple = [rename_col_fxn(c) for c in l_bar_simple if rename_col_fxn(c) in df.columns]
        return df.style.format(d_format, na_rep=na_rep).bar(subset=l_bar_simple,
                                                            align='mid',
                                                            color=("#EB9073", "#95cff5"))

    elif l_bars is not None:
        # apply all bar chart formats before applying other formats
        df_styled = df.style
        for bar_kwargs in l_bars:
            bar_kwargs = copy.deepcopy(bar_kwargs)
            if rename_cols_for_display:
                subset = [rename_col_fxn(c) for c in bar_kwargs['subset'] if rename_col_fxn(c) in df.columns]
            else:
                subset = [c for c in bar_kwargs['subset'] if c in df.columns]
            # remove original subset because we'll pass the rest of the params
            _ = bar_kwargs.pop('subset')

            # Set min and max based on whole data set if flag is set
            if bar_kwargs.get('normalize_subset', False):
                bar_kwargs['vmin'] = df[subset].min().min()
                bar_kwargs['vmax'] = df[subset].max().max() * 1.02
            try:
                _ = bar_kwargs.pop('normalize_subset')
            except KeyError:
                pass

            try:
                df_styled = df_styled.bar(subset=subset, **bar_kwargs)

            except Exception as er:
                logging.warning(f"Missing key for bar-chart: {er}")

        return df_styled.format(d_format, na_rep=na_rep)

    else:
        return df.style.format(d_format, na_rep=na_rep)


def style_percent_float_or_int(
        x: Union[float, str, int],
        pct_digits: int = 1,
        float_digits: int = 4,
) -> str:
    """Style a number based on its value
    Useful when you have a column in a dataframe with numbers
    that you want to format differently based on their value
    """
    if isinstance(x, str):
        return x
    elif x <= 1.0:
        return f"{x:.{pct_digits}%}"
    elif int(x) == x:
        return f"{x:,.0f}"
    else:
        return f"{x:,.{float_digits}f}"


def sort_by_grouped_cols(
        df: pd.DataFrame,
        sort_cols: List[iter],
        index_group_cols: List[str],
        d_col_sort: dict = None,
        ascending: bool = False,
) -> pd.DataFrame:
    """Take a df & sort by columns & index group -- keeping multi-index groups together
    Sort by order of index_group_cols from left to right.
    Check for sort order in d_col_sort first, else, will sort by provided column
    """
    if d_col_sort is None:
        d_col_sort = dict()

    df_sorted = df.sort_values(by=sort_cols, ascending=ascending).copy()

    d_final_col_sort = dict()

    if len(index_group_cols) > 1:
        for ix_col in index_group_cols:
            try:
                d_final_col_sort[ix_col] = d_col_sort[ix_col]
            except KeyError:
                d_final_col_sort[ix_col] = (df_sorted
                                            .drop_duplicates(subset=[ix_col])
                                            [ix_col]
                                            .tolist()
                                            )
    else:
        # TODO(djb) - what if we want to sort by a single column of multi-index and keep the order
        #  by highest of that column?
        dummy_col = 'dummy_col'
        df_sorted[dummy_col] = np.arange(len(df_sorted))
        for ix_col in index_group_cols + [dummy_col]:
            try:
                d_final_col_sort[ix_col] = d_col_sort[ix_col]
            except KeyError:
                d_final_col_sort[ix_col] = (df_sorted
                                            .drop_duplicates(subset=[ix_col])
                                            [ix_col]
                                            .tolist()
                                            )

    ix_sort = list()
    for ix_tuple in product(*[v for v in (d_final_col_sort[k] for k in index_group_cols)]):
        ix_sort.append(ix_tuple)

    df_sorted = df_sorted.set_index(index_group_cols)

    # product() creates all combinations of index groups
    # use dropna() to keep only actual existing combinations
    return df_sorted.loc[ix_sort].dropna()


def counts_describe(
        df: pd.DataFrame,
        return_df: bool = False,
        pct_digits: int = 2,
        add_pct_cols: bool = True,
        drop_dtype_col: bool = False,
        verbose: bool = False,
) -> Union[Styler, pd.DataFrame]:
    """Describe for counts, uniques, and null values
    Prefer to use it over .describe() because it doesn't show most common values
    or ranges, which could leak info.
    Also helpful to see null values (which describe doesn't explicitly show)
    """
    if verbose:
        info(f"Calculate dtypes, counts, uniques, & nulls")
    df_out = (
        df.dtypes.to_frame().rename(columns={0: 'dtype'})
        .merge(df.count().to_frame().rename(columns={0: 'count'}),
               how='outer', left_index=True, right_index=True)
        .merge(df.nunique().to_frame().rename(columns={0: 'unique'}),
               how='outer', left_index=True, right_index=True)
        .merge(df.isnull().sum().to_frame().rename(columns={0: 'null-count'}),
               how='outer', left_index=True, right_index=True)
    )
    if add_pct_cols:
        if verbose:
            info(f"Calculate percentages")
        df_out['unique-percent'] = df_out['unique'] / df_out['count']
        df_out['null-percent'] = df_out['null-count'] / len(df)

        df_out = df_out[['dtype', 'count',
                         'unique', 'unique-percent',
                         'null-count', 'null-percent',
                         ]]

    if drop_dtype_col:
        df_out = df_out.drop('dtype', axis=1)

    if return_df:
        return df_out
    else:
        return style_df_numeric(df_out,
                                int_cols=['count', 'unique'],
                                pct_labels=['-percent'],
                                pct_digits=pct_digits,
                                )


# Subreddit Name annotations!
def sub_name_annotations(
    df_coordinates: pd.DataFrame,
    x: str,
    y: str,
    text_col: str,
    l_targets: list,
    df_ann: pd.DataFrame,
    df_counterparts: pd.DataFrame,
    rank_col: str = 'combined_rank',
    ann_max_rank: int = 3,
    counter_max_rank: int = 2,
    verbose: bool = False,
    opacity_target: float = 0.99,
    opacity_ann: float = 0.8,
    opacity_counter: float = 0.68,
    font_target_kwargs: dict = None,
    font_ann_kwargs: dict = None,
    font_counter_kwargs: dict = None,
) -> list:
    """Return a list of annotations for a plotly scatter plot
    We'll apply 3 layers of annotations:
    - the target subreddits
    - the nearest 3 subreddits (overall/global)
    - the top subreddit PER country
    """
    # Set default values for font formatting
    if font_target_kwargs is None:
        font_target_kwargs = {
            'color': '#ffffff',
            'size': 17
        }
    if font_ann_kwargs is None:
        # this dict has slightly different configs w/ the key=the rank
        #. we want closer subs to be easier to view
        font_ann_kwargs = {
            1: {
                'font': {
                    'color': '#d3d4d5',
                    'size': 14,
                },
                'opacity': 0.80,
            },
            2: {
                'font': {
                    'color': '#c3c4c5',
                    'size': 13,
                },
                'opacity': 0.75,
            },
            3: {
                'font': {
                    'color': '#b3b4b5',
                    'size': 12,
                },
                'opacity': 0.70,
            },
        }
    if font_counter_kwargs is None:
        font_counter_kwargs = {
            1: {
                'font': {
                    'color': '#a3a4a5',
                    'size': 9.5,
                },
                'opacity': 0.65,
            },
            2: {
                'font': {
                    'color': '#939495',
                    'size': 9,
                },
                'opacity': 0.6,
            },
            3: {
                'font': {
                    'color': '#939495',
                    'size': 8,
                },
                'opacity': 0.55,
            },
        }
        
    d_rename_annot_ = {
        c_x_: 'x',
        c_y_: 'y',
        'subreddit_name': 'text',
    }
    
    # only highlight ANNs that also aren't targets
    df_ann_picks = df_ann[(
        (df_ann[rank_col] <= ann_max_rank) &
        (~df_ann[text_col].isin(l_subs_target))
    )]
    # if sub is already a top nearest neighbor, don't plot again as a country-specific sub
    # sort by sub size & drop duplicates so that we don't plot the same sub twice
    #  if it's relevant to multiple countries
    df_counterpart_picks = (
        df_counterparts
        .sort_values(by=[rank_col], ascending=True)
        .drop_duplicates(subset=[text_col], keep='first')
        .copy()
    )
    df_counterpart_picks = df_counterpart_picks[(
        (df_counterpart_picks[rank_col] <= counter_max_rank) &
        (~df_counterpart_picks[text_col].isin(l_subs_target)) &
        (~df_counterpart_picks[text_col].isin(df_ann_picks[text_col]))
    )]
    if verbose:
        print(f"{len(l_targets)} <- Target subs")
        print(f"{df_ann_picks.shape} <- ANN picks")
        print(f"{df_counterpart_picks.shape} <- Counterpart picks")
    
    # for TARGET subs, keep the text ABOVE the actual location
    l_target_subs_ = (
        df_coordinates[(df_coordinates[text_col].isin(l_subs_target))]
        [[k_ for k_ in d_rename_annot_.keys()]]
        .assign(
            showarrow=False,
            xshift=0,
            yshift=lambda df_: np.abs(df_[c_y_] * 0.35),
            opacity=opacity_target,
            # font={'color': "#fdfdfd", 'size': 17},
        )
        .rename(columns=d_rename_annot_)
        .to_dict(orient='records')
    )
    # We need to update font formatting separately because it's nested dict that messes up pandas.to_dict()
    for a_ in l_target_subs_:
        a_['font'] = font_target_kwargs
    
    l_ann_subs_ = (
        df_coordinates[[k_ for k_ in d_rename_annot_.keys()]]
        .merge(
            df_ann_picks
            [[text_col, rank_col]],
            how='inner',
            on=text_col,
        )
        .assign(
            **{
                'showarrow': False,
                'xshift': lambda df_: np.select(
                    [df_[rank_col] == 2, df_[rank_col] == 3],
                    [-np.abs(df_[x] * 0.2), np.abs(df_[x] * 0.2)],
                    default=0,
                ),
                'yshift': lambda df_: np.select(
                    [df_[rank_col] == 1, df_[rank_col] == 2, df_[rank_col] == 3],
                    [-np.abs(df_[y] * 0.05), -np.abs(df_[y] * 0.15), -np.abs(df_[y] * 0.25)],
                    default=0,
                ),
                # 'opacity': opacity_ann,
            }
        )
        # .drop(columns=rank_col)
        .rename(columns=d_rename_annot_)
        .to_dict(orient='records')
    )
    # We need to update font formatting separately because it's nested dict that messes up pandas.to_dict()
    for a_ in l_ann_subs_:
        # update the size and color of the ANN based on the rank 
        a_['font'] = font_ann_kwargs[a_[rank_col]]['font']
        a_['opacity'] = font_ann_kwargs[a_[rank_col]]['opacity']
        
        # delete the rank b/c it's not needed for annotation
        a_.pop(rank_col)
    
    l_counterpart_subs_ = (
        df_coordinates
        [[k_ for k_ in d_rename_annot_.keys()]]
        .merge(
            df_counterpart_picks
            [[text_col, rank_col]],
            how='inner',
            on=text_col,
        )
        .assign(
            showarrow=False,
            xshift=0,
            yshift=lambda df_: -np.abs(df_[c_y_] * 0.20),
            opacity=opacity_counter,
        )
        # .drop(columns=rank_col)
        .rename(columns=d_rename_annot_)
        .to_dict(orient='records')
    )
    # We need to update font formatting separately because it's nested dict that messes up pandas.to_dict()
    for a_ in l_counterpart_subs_:
        # update the size and color of the ANN based on the rank 
        a_['font'] = font_counter_kwargs[a_[rank_col]]['font']
        a_['opacity'] = font_counter_kwargs[a_[rank_col]]['opacity']
        
        # delete the rank b/c it's not needed for annotation
        a_.pop(rank_col)
    
    l_final_annots = l_counterpart_subs_ + l_ann_subs_ + l_target_subs_
    if verbose:
        print(f"{len(l_final_annots):,.0f} <- Total annotations")
        print(f"** Sample Target **")
        for k_, v_ in l_target_subs_[0].items():
            print(f"    {k_}: {v_}")
        print(f"** Sample ANN **")
        for k_, v_ in l_ann_subs_[0].items():
            print(f"    {k_}: {v_}")
        print(f"** Sample Counterpart **")
        for k_, v_ in l_counterpart_subs_[0].items():
            print(f"    {k_}: {v_}")

    # The last annotations get rendered last, so they'll be on top of other annotations
    return l_final_annots


# c_x_ = 'tsne1_jitter'
# c_y_ = 'tsne2_jitter'
# l_new_annots = sub_name_annotations(
#     df_tsne_full_,
#     x=c_x_,
#     y=c_y_,
#     text_col='subreddit_name',
#     l_targets=l_subs_target,
#     df_ann=df_ann_global,
#     df_counterparts=df_counterparts,
#     verbose=True,
# )

df_tsne = datasets['01 tsne_projections'].copy()
print(df_tsne.shape)

# %%time

df_ann_global = datasets['b34f89548982']  # '03 ann by text'
print(df_ann_global.shape)

# %%time

df_ann_b = datasets['b4d6fd8ae636']
print(df_ann_b.shape)

# df_ann_global.head()

l_subs_target = df_ann_global['subreddit_name_seed'].unique()
print(f"{len(l_subs_target)} <- Target subs")

l_subs_ann_global = df_ann_global['subreddit_name'].unique()
print(f"{len(l_subs_ann_global)} <- Global ANN subs by TEXT")

l_subs_ann_global_b = df_ann_b['subreddit_name'].unique()
print(f"{len(l_subs_ann_global_b)} <- Global ANN subs by BEHAVIOR")

df_counterparts = datasets['fa2b22b53b65']
print(df_counterparts.shape)

# df_counterparts.head()

df_counterparts_agg = (
  df_counterparts[['subreddit_name_seed', 'country_name', 'subreddit_name']]  #.head(25)
  .groupby(['subreddit_name_seed', 'country_name',])
  .agg(
      **{
          'top_subreddits': ('subreddit_name', list)
      }
  )
  # .rename(columns={'subreddit_name_seed': 'subreddit_name'})
)

# split list to make display easier/better (<br> for display or \n)
df_counterparts_agg['top_subreddits'] = (
    df_counterparts_agg['top_subreddits']
    # .apply(lambda x: '\n'.join(f"r/{x},"))
    .apply(lambda x: ',\n'.join(x))
    .astype(str)
    .str.replace("'", "")
)
# reshape to wide: each country=1 column
# reorder country names
df_counterparts_agg_wide = df_counterparts_agg.unstack()
df_counterparts_agg_wide.index.name = 'subreddit_name'
df_counterparts_agg_wide.columns = df_counterparts_agg_wide.columns.droplevel(0)

df_counterparts_agg_wide = df_counterparts_agg_wide[
  reorder_array(['United States', 'United Kingdom', 'India'], df_counterparts_agg_wide.columns)
]


# style_df_numeric(df_counterparts_agg_wide.head(15))

# table to display counterparts
style_df_numeric(
  df_counterparts_agg_wide
).set_sticky(axis='index').set_sticky(axis='columns')

df_tsne['subreddit_name_display'] = np.where(
  df_tsne['subreddit_name'].isin(l_subs_target),
  df_tsne['subreddit_name'],
  '',
)

# add jitter to TSNE
def rand_uniform_seed(low, high, size=None, random_state=42):
    """This fxn makes it so that we can make the jitter repeatable.
    It replaces:
    np.random.uniform(jitter_start_, jitter_end_, size=len(df_tsne))
    """
    rs = np.random.RandomState(random_state)
    return rs.uniform(low=low, high=high, size=size)


jitter_x_start_ = 1.8
jitter_y_start_ = 2.1
# del jitter_end_ = 2.08
# del jitter_end_
df_tsne['tsne1_jitter'] = (
    df_tsne['tsne1'] * 
    rand_uniform_seed(jitter_x_start_, jitter_x_start_ + 0.05, size=len(df_tsne), random_state=42)
)
df_tsne['tsne2_jitter'] = (
    df_tsne['tsne2'] * 
    rand_uniform_seed(jitter_y_start_, jitter_y_start_ + 0.09, size=len(df_tsne), random_state=1337)
)

%%time

df_tsne['rank_by_topic_v2'] = (
  df_tsne
  .groupby(['curator_topic_v2'])
  ['users_l7'].rank(method='first', ascending=False)
).astype(int)

df_tsne = df_tsne.sort_values(
    by=['curator_topic', 'users_l7', 'curator_topic_v2', ], ascending=[True, False, True]
)
# df_tsne.head()

l_top_subs_by_topic_v2 = df_tsne[df_tsne['rank_by_topic_v2'] <= 1]['subreddit_name'].to_list()
l_top_subs_by_topic_v2 = l_top_subs_by_topic_v2 + df_tsne[df_tsne['rank_by_topic_v2'] == 10]['subreddit_name'].to_list()
print(len(l_top_subs_by_topic_v2))
print(l_top_subs_by_topic_v2[:5])

# display to pick other interesting subs to show by default
# 'leagueoflegends,cats,anime,diy,art,cars,dataisbeautiful,fitness,cooking,technology,lgbt'
# style_df_numeric(
#     df_tsne[df_tsne['rank_by_topic_v2'] <= 2]
#     [['rank_by_topic_v2', 'subreddit_name', 'users_l7', 'posts_l7', 'curator_topic', 'curator_topic_v2']]
# )

style_df_numeric(
  df_tsne[['users_l7', 'posts_l7']]
  .describe(
    percentiles=[0.1, 0.2, .25, .4, .5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 0.975, 0.99, 0.995, 0.999, 0.9997, 0.99985]
  ).T
)

# resize posts_l7 & users_l7 to improve plots
#  because using the raw value creates very uneven plots

df_tsne['users_l7_bins'] = pd.cut(
    df_tsne['users_l7'],
    bins=[
        -1, df_tsne['users_l7'].quantile(0.20),
        df_tsne['users_l7'].quantile(0.30),
        df_tsne['users_l7'].quantile(0.40),
        
        df_tsne['users_l7'].quantile(0.45),
        df_tsne['users_l7'].quantile(0.50),
        df_tsne['users_l7'].quantile(0.55),
        
        df_tsne['users_l7'].quantile(0.60),
        df_tsne['users_l7'].quantile(0.65),
        df_tsne['users_l7'].quantile(0.70),
        
        df_tsne['users_l7'].quantile(0.75),
        df_tsne['users_l7'].quantile(0.81),
        df_tsne['users_l7'].quantile(0.87),
        
        df_tsne['users_l7'].quantile(0.91),
        df_tsne['users_l7'].quantile(0.95),
        df_tsne['users_l7'].quantile(0.97),
        
        df_tsne['users_l7'].quantile(0.985),
        df_tsne['users_l7'].quantile(0.991),
        df_tsne['users_l7'].quantile(0.999),
        
        df_tsne['users_l7'].quantile(0.99970),
        df_tsne['users_l7'].quantile(0.99985),
        np.inf,
    ],
    labels=[
        0.2, 0.4, 0.5,
        0.6, 0.7, 0.8,
        1.0, 1.1, 1.2,
        1.3, 1.4, 1.5,
        1.8, 2.0, 2.3,
        3.3, 4.3, 9.3,
        20, 60, 120,
    ],
)

print(df_tsne['users_l7_bins'].nunique())
# value_counts_and_pcts(df_tsne['users_l7_bins'], top_n=None, sort_index=True, sort_index_ascending=True)


def sub_name_annotations(
    df_coordinates: pd.DataFrame,
    x: str,
    y: str,
    text_col: str,
    l_targets: list,
    df_ann: pd.DataFrame,
    df_counterparts: pd.DataFrame,
    rank_col: str = 'combined_rank',
    ann_max_rank: int = 3,
    counter_max_rank: int = 2,
    verbose: bool = False,
    opacity_target: float = 0.99,
    opacity_ann: float = 0.8,
    opacity_counter: float = 0.68,
    font_target_kwargs: dict = None,
    font_ann_kwargs: dict = None,
    font_counter_kwargs: dict = None,
) -> list:
    """Return a list of annotations for a plotly scatter plot
    We'll apply 3 layers of annotations:
    - the target subreddits
    - the nearest 3 subreddits (overall/global)
    - the top subreddit PER country
    """
    # Set default values for font formatting
    if font_target_kwargs is None:
        font_target_kwargs = {
            'color': '#ffffff',
            'size': 17
        }
    if font_ann_kwargs is None:
        # this dict has slightly different configs w/ the key=the rank
        #. we want closer subs to be easier to view
        font_ann_kwargs = {
            1: {
                'font': {
                    'color': '#d3d4d5',
                    'size': 14,
                },
                'opacity': 0.80,
            },
            2: {
                'font': {
                    'color': '#c3c4c5',
                    'size': 13,
                },
                'opacity': 0.75,
            },
            3: {
                'font': {
                    'color': '#b3b4b5',
                    'size': 12,
                },
                'opacity': 0.70,
            },
        }
    if font_counter_kwargs is None:
        font_counter_kwargs = {
            1: {
                'font': {
                    'color': '#a3a4a5',
                    'size': 9.5,
                },
                'opacity': 0.65,
            },
            2: {
                'font': {
                    'color': '#939495',
                    'size': 9,
                },
                'opacity': 0.6,
            },
            3: {
                'font': {
                    'color': '#939495',
                    'size': 8,
                },
                'opacity': 0.55,
            },
        }
        
    d_rename_annot_ = {
        c_x_: 'x',
        c_y_: 'y',
        'subreddit_name': 'text',
    }
    
    # only highlight ANNs that also aren't targets
    df_ann_picks = df_ann[(
        (df_ann[rank_col] <= ann_max_rank) &
        (~df_ann[text_col].isin(l_subs_target))
    )]
    # if sub is already a top nearest neighbor, don't plot again as a country-specific sub
    # sort by sub size & drop duplicates so that we don't plot the same sub twice
    #  if it's relevant to multiple countries
    df_counterpart_picks = (
        df_counterparts
        .sort_values(by=[rank_col], ascending=True)
        .drop_duplicates(subset=[text_col], keep='first')
        .copy()
    )
    df_counterpart_picks = df_counterpart_picks[(
        (df_counterpart_picks[rank_col] <= counter_max_rank) &
        (~df_counterpart_picks[text_col].isin(l_subs_target)) &
        (~df_counterpart_picks[text_col].isin(df_ann_picks[text_col]))
    )]
    if verbose:
        print(f"{len(l_targets)} <- Target subs")
        print(f"{df_ann_picks.shape} <- ANN picks")
        print(f"{df_counterpart_picks.shape} <- Counterpart picks")
    
    # for TARGET subs, keep the text ABOVE the actual location
    l_target_subs_ = (
        df_coordinates[(df_coordinates[text_col].isin(l_subs_target))]
        [[k_ for k_ in d_rename_annot_.keys()]]
        .assign(
            showarrow=False,
            xshift=0,
            yshift=lambda df_: np.abs(df_[c_y_] * 0.35),
            opacity=opacity_target,
            # font={'color': "#fdfdfd", 'size': 17},
        )
        .rename(columns=d_rename_annot_)
        .to_dict(orient='records')
    )
    # We need to update font formatting separately because it's nested dict that messes up pandas.to_dict()
    for a_ in l_target_subs_:
        a_['font'] = font_target_kwargs
    
    l_ann_subs_ = (
        df_coordinates[[k_ for k_ in d_rename_annot_.keys()]]
        .merge(
            df_ann_picks
            [[text_col, rank_col]],
            how='inner',
            on=text_col,
        )
        .assign(
            **{
                'showarrow': False,
                'xshift': lambda df_: np.select(
                    [df_[rank_col] == 2, df_[rank_col] == 3],
                    [-np.abs(df_[x] * 0.2), np.abs(df_[x] * 0.2)],
                    default=0,
                ),
                'yshift': lambda df_: np.select(
                    [df_[rank_col] == 1, df_[rank_col] == 2, df_[rank_col] == 3],
                    [-np.abs(df_[y] * 0.05), -np.abs(df_[y] * 0.15), -np.abs(df_[y] * 0.25)],
                    default=0,
                ),
                # 'opacity': opacity_ann,
            }
        )
        # .drop(columns=rank_col)
        .rename(columns=d_rename_annot_)
        .to_dict(orient='records')
    )
    # We need to update font formatting separately because it's nested dict that messes up pandas.to_dict()
    for a_ in l_ann_subs_:
        # update the size and color of the ANN based on the rank 
        a_['font'] = font_ann_kwargs[a_[rank_col]]['font']
        a_['opacity'] = font_ann_kwargs[a_[rank_col]]['opacity']
        
        # delete the rank b/c it's not needed for annotation
        a_.pop(rank_col)
    
    l_counterpart_subs_ = (
        df_coordinates
        [[k_ for k_ in d_rename_annot_.keys()]]
        .merge(
            df_counterpart_picks
            [[text_col, rank_col]],
            how='inner',
            on=text_col,
        )
        .assign(
            showarrow=False,
            xshift=0,
            yshift=lambda df_: -np.abs(df_[c_y_] * 0.20),
            opacity=opacity_counter,
        )
        # .drop(columns=rank_col)
        .rename(columns=d_rename_annot_)
        .to_dict(orient='records')
    )
    # We need to update font formatting separately because it's nested dict that messes up pandas.to_dict()
    for a_ in l_counterpart_subs_:
        # update the size and color of the ANN based on the rank 
        a_['font'] = font_counter_kwargs[a_[rank_col]]['font']
        a_['opacity'] = font_counter_kwargs[a_[rank_col]]['opacity']
        
        # delete the rank b/c it's not needed for annotation
        a_.pop(rank_col)
    
    l_final_annots = l_counterpart_subs_ + l_ann_subs_ + l_target_subs_
    if verbose:
        print(f"{len(l_final_annots):,.0f} <- Total annotations")
        print(f"** Sample Target **")
        for k_, v_ in l_target_subs_[0].items():
            print(f"    {k_}: {v_}")
        print(f"** Sample ANN **")
        for k_, v_ in l_ann_subs_[0].items():
            print(f"    {k_}: {v_}")
        print(f"** Sample Counterpart **")
        for k_, v_ in l_counterpart_subs_[0].items():
            print(f"    {k_}: {v_}")

    # The last annotations get rendered last, so they'll be on top of other annotations
    return l_final_annots


# c_x_ = 'tsne1_jitter'
# c_y_ = 'tsne2_jitter'
# l_new_annots = sub_name_annotations(
#     df_tsne_full_,
#     x=c_x_,
#     y=c_y_,
#     text_col='subreddit_name',
#     l_targets=l_subs_target,
#     df_ann=df_ann_global,
#     df_counterparts=df_counterparts,
#     verbose=True,
# )

# apply filters for plot
# For detailed map only focus on input subs & ANN.
#.  DO NOT include the global/top subs by topic
df_tsne_full_ = df_tsne[(
    (df_tsne['subreddit_name'].isin(l_subs_target)) |
    (df_tsne['subreddit_name'].isin(l_subs_ann_global)) |
    (df_tsne['subreddit_name'].isin(df_counterparts['subreddit_name']))  # | (df_tsne['subreddit_name'].isin(l_top_subs_by_topic_v2))
)]

s_plot_name_ = f"Similar Subreddits for Search"
c_text_display = 'subreddit_name_display'
c_x_ = 'tsne1_jitter'
c_y_ = 'tsne2_jitter'

# add Subreddit name annotations separately. This way text should always be on top of points
l_new_annots = sub_name_annotations(
    df_tsne_full_,
    x=c_x_,
    y=c_y_,
    text_col='subreddit_name',
    l_targets=l_subs_target,
    df_ann=df_ann_global,
    df_counterparts=df_counterparts,
    verbose=False,
)

# Set custom text & custom hover template
l_custom_text_ = [
    'subreddit_name',
    'curator_topic_v2',
    'users_l7',
]
sub_hovertemplate = "<br>".join(
    [
        "<b>%{customdata[0]}</b>",
        "<i>topic v2</i>: %{customdata[1]}",
        "<i>users L7</i>: %{customdata[2]:,.0f}",
    ]
)

fig = px.scatter(
    df_tsne_full_,
    x=c_x_, 
    y=c_y_,
    opacity=0.7,
    # title=s_plot_name_,
    size='users_l7_bins',  # 'users_l7_bins'
    color='curator_topic',  # 'curator_topic_sim',
    category_orders={
        'curator_topic': sorted(df_tsne_full_['curator_topic'].dropna().unique(), reverse=False),
        # 'curator_topic_sim': sorted(df_tsne_full_['curator_topic_sim'].dropna().unique(), reverse=False),
    },
    # text=c_text_display,
    hover_name='subreddit_name',
    # hover_data=[k_clusters_to_plot_],
    custom_data=l_custom_text_,
)
fig.update_layout(
    width=1100,
    height=650,
    # autosize=False,
    annotations=l_new_annots,
    yaxis=dict(showgrid=False, zeroline=False,),
    xaxis=dict(showgrid=False, zeroline=False),
    plot_bgcolor='#040404',  # dark-gray: '#1a1a1a' '#fcfcfc'
)

# CHANGE color of text
# option A: change color of all text the same
# Also: apply hover template
fig.update_traces(
    hovertemplate=sub_hovertemplate,
    marker_line_width=0,
    textposition='top center',
    textfont=dict(
        color='#fcfcfc',  # '#fcfcfc' '#1a1a1a'
        size=16,
        # bgcolor='#ababab',
        # opacity=0.2,
    ),
)

# option b: match color to the dot (point in the scatter)
# fig.for_each_trace(lambda t: t.update(textfont_color=t.marker.color, textposition='top center'))

# fig.show(renderer='png')
fig.show()


# apply filters for plot
# TODO(djb): create slider for min number of users & min # of posts
df_tsne_full_ = (
    df_tsne[(
      (df_tsne['users_l7'] >= 700) &
      (df_tsne['posts_l7'] >= 5)
  )]
  # .sort_values(by=['users_l7'], ascending=True)
)
s_plot_name_ = f"{len(df_tsne_full_):,.0f} Subreddits With Recent Activity"
c_text_display = 'subreddit_name_display'

# Set custom text & custom hover template
l_custom_text_ = [
    'subreddit_name',
    'curator_topic_v2',
    'users_l7',
]
sub_hovertemplate = "<br>".join(
    [
        "<b>%{customdata[0]}</b>",
        "<i>topic v2</i>: %{customdata[1]}",
        "<i>users L7</i>: %{customdata[2]:,.0f}",
    ]
)
# add Subreddit name annotations separately. This way text should always be on top of points
l_new_annots = sub_name_annotations(
    df_tsne_full_,
    x=c_x_,
    y=c_y_,
    text_col='subreddit_name',
    l_targets=l_subs_target,
    df_ann=df_ann_global,
    df_counterparts=df_counterparts,
    verbose=False,
)


fig = px.scatter(
    df_tsne_full_,
    x='tsne1_jitter', 
    y='tsne2_jitter',
    opacity=0.6,
    size='users_l7_bins',
    # title=s_plot_name_,
    color='curator_topic',
    category_orders={'curator_topic': sorted(df_tsne_full_['curator_topic'].dropna().unique(), reverse=True)},
    # text=c_text_display,
    hover_name='subreddit_name',
    # hover_data=[k_clusters_to_plot_],
    custom_data=l_custom_text_,
)
fig.update_layout(
    width=1100,
    height=650,
    legend_traceorder="reversed",
    annotations=l_new_annots,
    autosize=False,
    yaxis=dict(showgrid=False, zeroline=False,),
    xaxis=dict(showgrid=False, zeroline=False),
    plot_bgcolor='#040404',  # dark-gray: '#1a1a1a' '#fcfcfc'
)

# CHANGE color of text
# option A: change color of all text the same
# Also: apply hover template
fig.update_traces(
    hovertemplate=sub_hovertemplate,
    marker_line_width=0,
    textposition='top center',
    textfont=dict(
        color='#fcfcfc',  # '#fcfcfc' '#1a1a1a'
        size=17,
        # bgcolor='#ababab',
        # opacity=0.2,
    ),
)

# option b: match color to the dot (point in the scatter)
# fig.for_each_trace(lambda t: t.update(textfont_color=t.marker.color, textposition='top center'))

# fig.show(renderer='png')
fig.show()

# Full projection


# apply filters for plot
df_tsne_full_ = df_tsne[(
    (df_tsne['users_l7'] >= 500) &
    (df_tsne['posts_l7'] >= 1)
)]
s_plot_name_ = f"{len(df_tsne_full_):,.0f} Subreddits With Recent Activity"
c_text_display = 'subreddit_name_display'

# Set custom text & custom hover template
l_custom_text_ = [
    'subreddit_name',
    'curator_topic_v2',
    'users_l7',
]
sub_hovertemplate = "<br>".join(
    [
        "<b>%{customdata[0]}</b>",
        "<i>topic v2</i>: %{customdata[1]}",
        "<i>users L7</i>: %{customdata[2]:,.0f}",
    ]
)
# add Subreddit name annotations separately. This way text should always be on top of points
l_new_annots = sub_name_annotations(
    df_tsne_full_,
    x=c_x_,
    y=c_y_,
    text_col='subreddit_name',
    l_targets=l_subs_target,
    df_ann=df_ann_global,
    df_counterparts=df_counterparts,
    verbose=False,
)

fig = px.scatter(
    df_tsne_full_,
    x='tsne1_jitter', 
    y='tsne2_jitter',
    # color=k_clusters_to_plot_,
    opacity=0.7,
    size='users_l7',
    # title=s_plot_name_,
    color='curator_topic',
    category_orders={'curator_topic': sorted(df_tsne_full_['curator_topic'].dropna().unique(), reverse=True)},
    # text=c_text_display,
    hover_name='subreddit_name',
    # hover_data=[k_clusters_to_plot_],
    custom_data=l_custom_text_,
)
fig.update_layout(
    width=1100,
    height=800,
    # autosize=False,
    annotations=l_new_annots,
    legend_traceorder="reversed",
    yaxis=dict(showgrid=False, zeroline=False,),
    xaxis=dict(showgrid=False, zeroline=False),
    plot_bgcolor='#040404',  # dark-gray: '#1a1a1a' '#fcfcfc'
)

# CHANGE color of text
# option A: change color of all text the same
# Also: apply hover template
fig.update_traces(
    hovertemplate=sub_hovertemplate,
    marker_line_width=0,
    textposition='top center',
    textfont=dict(
        color='#fcfcfc',  # '#fcfcfc' '#1a1a1a'
        size=17,
        # bgcolor='#ababab',
        # opacity=0.2,
    ),
)

# option b: match color to the dot (point in the scatter)
# fig.for_each_trace(lambda t: t.update(textfont_color=t.marker.color, textposition='top center'))

# fig.show(renderer='png')
fig.show()

# from dash import Dash, dcc, html, Input, Output

# # try:
# #   # delete existing app to free up the port
# #   del app
# # except NameError:
# #   pass

# # Fails: '127.0.0.1', 'localhost'
# #   `localhost refused to connect`
# # '0.0.0.0' -> Doesn't fail explicitly, but won't display anything
# dash_host_name_ = '0.0.0.0'
# dash_port_ = 8081

# # build app
# app = JupyterDash(__name__)
# app.layout = html.Div([
#     html.H1('JupyterDash Test 1,2,3!!'),
#     html.H2('JupyterDash Test 1,2,3!!'),
#     dcc.Graph(id='graph')
# ])

# if __name__ == '__main__':
#     # "external' & jupterlab run, but we can't see anything
#     # app.run_server(mode='external', debug=True, port=dash_port_)
#     # app.run_server(mode='jupyterlab', debug=True, port=dash_port_)
#     app.run_server(mode="inline", host=dash_host_name_, port=dash_port_, debug=True)

# %%html
# <iframe src=f"http://{dash_host_name_}:{dash_port_}/" width="700" height="400"></iframe>

# %%html
# <iframe src=f"http://127.0.0.1:8888/" width="700" height="400"></iframe>

# df_city_and_loc['city_users_l7_scaled'] = pd.cut(
#     df_city_and_loc['city_users_l7'],
#     bins=[
#         -1, df_city_and_loc['city_users_l7'].quantile(0.20),
#         df_city_and_loc['city_users_l7'].quantile(0.40),
#         df_city_and_loc['city_users_l7'].quantile(0.55),
#         df_city_and_loc['city_users_l7'].quantile(0.65),
#         df_city_and_loc['city_users_l7'].quantile(0.75),
#         df_city_and_loc['city_users_l7'].quantile(0.85),
#         df_city_and_loc['city_users_l7'].quantile(0.97),
#         df_city_and_loc['city_users_l7'].quantile(0.990)
#         , np.inf
#     ],
#     labels=[0.7, 0.9, 1.1, 1.2, 1.6, 2.4, 3.8, 9, 20]
# )

# # print(df_city_and_loc['city_users_l7_scaled'].describe())
# # value_counts_and_pcts(df_city_and_loc['city_users_l7_scaled'], sort_index=True, sort_index_ascending=True)
# # df_city_and_loc['city_users_l7_scaled'].value_counts()

# df_city_and_loc.head()

# df_top_city = datasets['01 Top Subreddits by CITY rank'].copy()
# print(df_top_city.shape)
# # df_top_city.head()

# # get initial list
# df_top_city_agg = (
#     df_top_city[df_top_city['rank_city'] <= 10]
#     .groupby(['country_code', 'region', 'city'])
#     .agg(
#         **{
#             'top_subreddits': ('subreddit_name', list)
#         }
#     )
# )
# # split list with <br> to make display easier/better
# df_top_city_agg['top_subreddits'] = (
#     df_top_city_agg['top_subreddits']
#     .apply(lambda x: '<br> r/'.join(x))
#     .astype(str)
#     .str.replace("'", "")
# )

# df_top_city_agg.head()

# df_city_map = df_top_city_agg.copy().merge(
#     df_city_and_loc.rename(columns={'geo_country_code': 'country_code', 'geo_region': 'region', 'geo_city': 'city'}),
#     how='left',
#     on=['country_code', 'region', 'city'],
# )
# df_city_map.shape

# df_city_map.head()

# l_top_country_codes = [
#   "DE", "MX", "AU"
#   , "FR", "NL", "IT", "ES", "BR"
#   , "US", "GB", "IN", "CA"
# ]

# df_top_city_labels =  df_city_and_loc[(
#     # top from target countries
#     (
#         (df_city_and_loc['geo_country_code'].isin(l_top_country_codes)) &
#         (df_city_and_loc['city_rank_country'] == 1)
#     ) |
#     # top from the US
#     (
#         (df_city_and_loc['geo_country_code'] == 'US') &
#         (df_city_and_loc['city_rank_country'] <= 5)
#     )
# )].copy()

# df_top_city_labels.shape

# # add TOP city annotations separately. This way text should always be on top.
# # NVM, scatter geo doesn't support annotations, so we just create a new plot & add data
# #  https://community.plotly.com/t/how-can-i-combine-choropleth-and-scatter-layer-with-animation-frame-in-a-plotly-map/41330
# d_rename_c_top_city_annot = {
#     'latitude': 'x',
#     'longitude': 'y',
#     'geo_city': 'text',
# }

# d_top_city_annotations = (
#     df_city_and_loc[(
#         (df_city_and_loc['geo_country_code'].isin(l_top_country_codes)) &
#         (df_city_and_loc['city_rank_country'] == 1)
#     )]
#     [[k_ for k_ in d_rename_c_top_city_annot.keys()]]
#     .assign(showarrow=False, xshift=0, yshift=14)
#     .rename(columns=d_rename_c_top_city_annot)
#     .to_dict(orient='records')
# )

# fig_top_cities = px.scatter_geo(
#     df_top_city_labels,
#     lat="latitude",
#     lon='longitude',
#     # size=[0.000001] * len(df_top_city_labels),
#     opacity=0,  # set opacity to 0 so we don't see an extra marker
#     hover_name='geo_city',
#     hover_data={'latitude': False, 'longitude': False, 'geo_city': False,},
#     text='geo_city',
# )
# # Set hovermode=False so we only see hover info from map with actual city<>subreddit info
# fig_top_cities.update_layout(hovermode=False)
# fig_top_cities.show()

# # Create base plot with only names of top cities. No markers & no hover
# #  TODO(djb): Plot top countries even if they're not in the data(?)
# fig_top_cities = px.scatter_geo(
#     df_top_city_labels,
#     lat="latitude",
#     lon='longitude',
#     # size=[0.0001] * len(df_top_city_labels),
#     opacity=0,  # set opacity to 0 so we don't see an extra marker
#     hover_name='geo_city',
#     hover_data={'latitude': False, 'longitude': False, 'geo_city': False,},
#     text='geo_city',
# )
# # Set hovermode=False so we only see hover info from map with actual city<>subreddit info
# fig_top_cities.update_layout(hovermode=False)


# # Create custom data for hover with city & subredit info
# l_custom_text_ = [
#     'city',
#     # 'city_rank_country',
#     'top_subreddits',
    
#     # 'city_rank_world',  # world rank doesn't help much for now
    
# ]
# sub_hovertemplate = "<br>".join(
#     [
#         "<b>%{customdata[0]}</b>",
#         # "<b>Country Rank</b>: %{customdata[1]}"
#         # "<b>Popular Subreddits</b>:<br> r/%{customdata[1]}",
#         " r/%{customdata[1]}",
#     ]
# )
# fig = px.scatter_geo(
#     df_city_map.dropna(how='any'),
#     lat="latitude",
#     lon='longitude',
#     color="country_name",
#     hover_name="city", 
#     size="city_users_l7_scaled",
#     category_orders={'country_name': sorted(df_city_map['country_name'].dropna().unique())},
#     custom_data=l_custom_text_,
#     # top projection picks: natural earth
#     projection="natural earth"
# )


# fig.update_layout(
#     # annotations=d_top_city_annotations,  # annotations don't work with geoscatter()
#     width=980,
#     height=640,
#     autosize=True,
#     # yaxis=dict(showgrid=False, zeroline=False,),
#     # xaxis=dict(showgrid=False, zeroline=False),
#     # plot_bgcolor='#040404',  # dark-gray: '#1a1a1a' '#fcfcfc'
# )
# # Update hover info
# fig.update_traces(
#     hovertemplate=sub_hovertemplate,
# )
# fig.add_trace(fig_top_cities.data[0])

# fig.show()



