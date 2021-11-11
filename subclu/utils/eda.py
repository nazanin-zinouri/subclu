"""
Generic utils .
Mostly around:
- logging
- object introspection
- displaying pandas dataframes in notebooks
"""
import copy
from datetime import datetime, timedelta
# import gc
import importlib
# import io
from itertools import product
import logging
from logging import info
from pathlib import Path
from typing import Union, List, Any, Optional, Tuple, Dict
import sys
from pkg_resources import get_distribution

import numpy as np
import pandas as pd
from pandas.io.formats.style import Styler
# from tqdm.auto import tqdm

Array = Union[np.array, pd.Series, pd.DataFrame, List]


# ===============
# Logging & Misc
# ===
def reorder_array(items_to_front: list,
                  array):
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


def get_venn_sets2(
        iter_a: iter,
        iter_b: iter,
        a_name: str = 'a',
        b_name: str = 'b',
        return_dict: bool = True,
) -> Dict[str, set]:
    """Input 2 iterables and return a dictionary with
    the items in one
    """
    if not isinstance(iter_a, set):
        set_a = set(iter_a)
    else:
        set_a = iter_a
    if not isinstance(iter_b, set):
        set_b = set(iter_b)
    else:
        set_b = iter_b

    print(f"{len(set_a):6,.0f} <- {a_name}")
    print(f"{len(set_b):6,.0f} <- {b_name}")
    print(f"{len(set_a | set_b):6,.0f} <- {a_name} + {b_name}")

    d_ = dict()
    d_[f"{a_name}_only"] = set(iter_a) - set(iter_b)

    d_[f"{a_name}_and_{b_name}"] = set(iter_a) & set(iter_b)

    d_[f"{b_name}_only"] = set(iter_b) - set(iter_a)

    return d_


def setup_logging(
        log_format: str = 'basic_with_time',
        console_level=logging.INFO,
        file_level=logging.INFO,
        path_logs: str = 'logs',
        file_root_name: str = None,
        verbose: bool = False
) -> None:
    """Util that's helpful for saving logs or customizing display of console logs
    Especially useful when logging inside of ipython.
    By default, ipython initializes a log handler that's good for ipython, but makes
    customization non-trivial.
    Also includes some logic for stdout logs when running inside of ipython
    """
    dtm_start_log = datetime.utcnow()
    str_dtm_start = dtm_start_log.strftime('%Y-%m-%d_%H-%M-%S')
    if log_format == 'basic_with_time':
        ch_datetime_format = '%H:%M:%S'
    else:
        ch_datetime_format = '%Y-%m-%d %H:%M:%S'
    if log_format == 'verbose':
        log_format = ('%(asctime)s | %(levelname)s'
                      ' | %(filename)-10s:%(lineno)d \t | %(funcName)s'
                      '\t\t | "%(message)s"'
                      )  # if needed in multi-threading %(processName)-s |
    elif log_format in ['basic_with_date', 'basic_with_time']:
        log_format = '%(asctime)s | %(levelname)s | "%(message)s"'
    elif log_format == 'basic':
        log_format = '%(levelname)s | "%(message)s"'

    # Note: logging.basicConfig won't work inside ipython unless we reload `logging`
    importlib.reload(logging)
    # if file_root_name is not None:
    #     path_logs = Path(path_logs)
    #     Path.mkdir(path_logs, parents=False, exist_ok=True)
    #     # for some reason file logging isn't working with basicConfig...
    #     logging.basicConfig(
    #         level=file_level,
    #         format=log_format,
    #         handlers=[
    #             logging.FileHandler(str(path_logs / f'{str_dtm_start}_{file_root_name}.log')),
    #         ],
    #         datefmt='%Y-%m-%d %H:%M:%S'
    #     )

    # Explicitly getLogger in case running inside ipython/jupyter
    logger = logging.getLogger()
    logger.setLevel(console_level)

    if verbose:
        print("Initial handlers")
        for handler in logger.handlers:
            print(handler)

    # remove other console handlers (when kicked off by ipython)
    try:
        logger.handlers = [h for h in logger.handlers if not isinstance(h, logging.StreamHandler)]
    except AttributeError:
        pass

    # Set new stream/console handler
    ch = logging.StreamHandler()
    ch.setLevel(console_level)
    formatter = logging.Formatter(log_format, ch_datetime_format)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # For some reason, sqlalchemy can sometimes be set to the wrong level, so se it back
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)

    # set & add logger for file
    if file_root_name is not None:
        path_logs = Path(path_logs)
        Path.mkdir(path_logs, parents=False, exist_ok=True)

        fileHandler = logging.FileHandler(str(path_logs / f'{str_dtm_start}_{file_root_name}.log'))
        fileHandler.setLevel(file_level)
        fileHandler.setFormatter(formatter)
        logger.addHandler(fileHandler)

    if verbose:
        print("Final handlers")
        for handler in logger.handlers:
            print(handler)


def print_lib_versions(
        lib_list,
) -> None:
    """
    Show the library versions of the input list.
    Useful in jupyter to make sure we're using the expected versions
    in the current kernel.
    Args:
        lib_list: List of libraries to check for version
    Returns:
        None, it prints the data
    """
    print(f"python\t\tv {sys.version.split(' ')[0]}\n===")

    for lib_ in lib_list:
        sep_ = '\t' if len(lib_.__name__) > 7 else '\t\t'
        try:
            print(f"{lib_.__name__}{sep_}v: {lib_.__version__}")
        except AttributeError:
            print(f"{lib_.__name__}{sep_}v: {get_distribution(f'{lib_.__name__}').version}")


def elapsed_time(
        start_time,
        log_label: str = ' ',
        measure=None,
        verbose: bool = False,
) -> float:
    """
    Given a datetime object as a start time, calculate how many days/hours/minutes/seconds
    since the start time.
    """
    time_now = datetime.utcnow()
    time_elapsed = (time_now - start_time)
    if measure is None:
        pass  # keep as datetime.timedelta object
    elif measure == 'seconds':
        time_elapsed = time_elapsed / timedelta(seconds=1)
    elif measure == 'minutes':
        time_elapsed = time_elapsed / timedelta(minutes=1)
    elif measure == 'hours':
        time_elapsed = time_elapsed / timedelta(hours=1)
    elif measure == 'days':
        time_elapsed = time_elapsed / timedelta(days=1)
    else:
        raise NotImplementedError(f"Measure unknown: {measure}")

    if verbose:
        if measure is not None:
            logging.info(f"  {time_elapsed:,.3f} {measure} <- {log_label} time elapsed")
        else:
            logging.info(f"  {time_elapsed} <- {log_label} time elapsed")

    return time_elapsed


def notebook_display_config(
        figsize: tuple = (9, 6),
        axes_labelsize: float = 18,
        ytick_labelsize: float = 16,
        xtick_labelsize: float = 16,
        font_size: float = 16,
        pd_max_columns: float = 60,
        pd_max_rows: float = 30,
        pd_max_colwidth: float = 240,
        pd_display_width: float = 300,
) -> None:
    """Base imports & display settings for notebooks"""

    # plotting & visualization settings
    # don't use ggplot
    # import matplotlib.pyplot as plt
    # import matplotlib.ticker as mtick
    # plt.style.use('ggplot')

    # Set text sizes to be more legible for sns figures
    from pylab import rcParams
    rcParams['figure.figsize'] = figsize
    rcParams['axes.labelsize'] = axes_labelsize
    rcParams['ytick.labelsize'] = ytick_labelsize
    rcParams['xtick.labelsize'] = xtick_labelsize
    rcParams['font.size'] = font_size

    # pandas display options
    pd.set_option('display.max_columns', pd_max_columns)
    pd.set_option('display.max_rows', pd_max_rows)
    pd.set_option('display.max_colwidth', pd_max_colwidth)
    pd.set_option('display.width', pd_display_width)


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

    if sort_index & (index_group_cols is None):
        df_out = df_out.sort_index()
    elif sort_index & (index_group_cols is not None):
        df_out = sort_by_grouped_cols(
            df=df_out.reset_index(),
            sort_cols=[count_label],
            index_group_cols=index_group_cols,
            ascending=sort_index_ascending,
        )

    if reset_index & (col is not None):
        df_out = df_out.reset_index().rename(columns={'index': col})
    elif reset_index:
        df_out = df_out.reset_index()

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


def display_formatted_ints_and_pcts(
        df: pd.DataFrame,
        int_labels: List[str] = None,
        pct_labels: List[str] = None,
        pct_digits: int = 2,
) -> Styler:
    """
    Format integer & percent columns in a dataframe & return a Styler object
    for displaying it in jupyter.
    :param df:
    :param int_labels:
    :param pct_labels:
    :param pct_digits:
    :return: Styler
    """
    if int_labels is None:
        int_labels = ['count']
    if pct_labels is None:
        pct_labels = ['percent']

    num_cols = df.select_dtypes('number').columns
    d_format = {c: "{:,.0f}" for c in num_cols if any(lbl in c for lbl in int_labels)}
    d_format.update({c: ''.join(["{", f":.{pct_digits}%", "}"
                                 ]) for c in num_cols if any(lbl in c for lbl in pct_labels)})
    return df.style.format(d_format)


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
        fillna: Any = None,
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
        fillna:
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

    if fillna is not None:
        df[all_num_cols] = df[all_num_cols].fillna(fillna)

    if rename_cols_for_display:
        if rename_cols_pairs is None:
            # add a space after a dash so google sheets can display columns better
            rename_cols_pairs = [('_', ' '), ('-', '- ')]

        def rename_col_fxn(col: str, rename_cols_pairs: list = rename_cols_pairs):
            """Given a list of tuples, apply them to replace string patterns in col name"""
            col_new = col
            for tpl in rename_cols_pairs:
                col_new = col_new.replace(*tpl)
            return col_new

        d_format = {rename_col_fxn(k): v for k, v in d_format.items()}
        df = df.rename(columns={c: rename_col_fxn(c) for c in df.columns})

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
    or ranges, which could leak PHI.
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


def create_col_with_sparse_names(
        df,
        col_post_id: str = 'post_id',
        col_subreddit_name: str = 'subreddit_name',
        col_upvotes: str = 'upvotes',
        subreddits_to_ignore: iter = None,
) -> Union[np.ndarray, pd.Series]:
    """Only name the subs with the most upvotes for each sub"""
    if subreddits_to_ignore is None:
        subreddits_to_ignore = list()

    post_ids_to_keep = (
        df[~df[col_subreddit_name].isin(subreddits_to_ignore)]
        .sort_values(by=[col_upvotes], ascending=False)
        .drop_duplicates(subset=[col_subreddit_name], keep='first')
        [col_post_id]
    )
    return np.where(
        df[col_post_id].isin(post_ids_to_keep),
        df[col_subreddit_name],
        ''
    )


def hide_aa_text_in_plotly_legend() -> None:
    """
    There's no good way to remove `Aa` from plotly legends besides updating the
    HTML, so run this before creating a plotly fig in a notebook
    https://stackoverflow.com/questions/62554007/how-to-remove-aa-from-the-legend-in-plotly-py

    Returns: None
    """
    from IPython.core.display import HTML
    HTML("""
    <style>
    g.pointtext {display: none;}
    </style>
    """)


def display_items_for_cluster_id(
        df_subs_meta_plot,
        id_,
        cols_to_display: list = None,
        col_manual_labels: str = 'manual_topic_and_rating',
        col_clustering: str = 'cluster_id_agg_ward_cosine_35',
        n_subs_to_show: int = 15,
) -> None:
    """
    """
    from IPython.core.display import display

    if cols_to_display is None:
        cols_to_display = [
            'subreddit_name',
            'manual_topic_and_rating',
            'subreddit_title',

            # 'subreddit_name_title_and_clean_descriptions_word_count',
            'users_l28',
            'posts_l28',
            'comments_l28',

            'post_median_word_count',

            'German_posts_percent',
            'English_posts_percent',
            # 'other_language_posts_percent',

            'image_post_type_percent',
            'text_post_type_percent',
            # 'link_post_type_percent',
            # 'other_post_type_percent',

            'rating',
            'rating_version',
            'over_18',
        ]
        cols_to_display = [c for c in cols_to_display if c in df_subs_meta_plot.columns]

    mask_ = df_subs_meta_plot[col_clustering] == id_
    print(f"\nCluster ID: {id_}\n  {mask_.sum()} Subreddit count in group")

    # noinspection PyTypeChecker
    display(
        value_counts_and_pcts(
            df_subs_meta_plot[mask_][col_manual_labels],
            add_col_prefix=False,
            reset_index=True,
            cumsum=False,
        ).hide_index()
    )

    # noinspection PyTypeChecker
    display(
        style_df_numeric(
            df_subs_meta_plot[mask_][cols_to_display]
            .sort_values(by=['users_l28'], ascending=False)
            .head(n_subs_to_show)
            ,
            rename_cols_for_display=True,
            l_bar_simple=[
                'German_posts_percent', 'English_posts_percent',
                'image_post_type_percent', 'text_post_type_percent',
                'users_l28',
            ]
        ).set_properties(subset=['subreddit title'], **{'width': '300px'}).hide_index()
    )

#
# ~ fin
#
