# Python Notebook - i18n Topic Model Explorer

datasets

# none of these worked to update pandas... sigh

# !/usr/bin/python3 -m pip install --upgrade pip -t "/tmp"

# fails silently
# !pip install pandas -t "/tmp" > /dev/null 2>&1
# !pip install pandas --upgrade -t "/tmp"

import sys
from typing import Union, List, Any, Optional, Tuple, Dict

import numpy as np
import pandas as pd
from pandas.io.formats.style import Styler



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


def notebook_display_config(
        figsize: tuple = (9, 6),
        axes_labelsize: float = 18,
        ytick_labelsize: float = 16,
        xtick_labelsize: float = 16,
        font_size: float = 16,
        pd_max_columns: float = 60,
        pd_max_rows: float = 30,
        pd_max_colwidth: float = 440,
        pd_display_width: float = 400,
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



# Style to get sticky header for a dataframe
# https://github.com/pandas-dev/pandas/issues/29072

from IPython.display import HTML

# CSS styling 
#   Set max-height to ~900 so at most it covers about 1 screen
style_freeze_header = """
<style scoped>
    .dataframe-div {
      max-height: 900px;
      overflow: auto;
      position: relative;
    }

    .dataframe thead th {
      position: -webkit-sticky; /* for Safari */
      position: sticky;
      top: 0;
      background: white;
      /*
      color: white;
      */
    }

    .dataframe thead th:first-child {
      left: 0;
      z-index: 1;
    }

    .dataframe tbody tr th:only-of-type {
            vertical-align: middle;
        }

    .dataframe tbody tr th {
      position: -webkit-sticky; /* for Safari */
      position: sticky;
      left: 0;
      /* 
      background: black;
      color: white;
      */
      vertical-align: top;
    }
</style>
"""
# Getting default html as string
# df_html = df_c_summary.to_html() 
# # Concatenating to single string
# df_html = style_freeze_header+'<div class="dataframe-div">'+df_html+"\n</div>"

# Displaying df with sticky header and index
# HTML(df_html)

import pandas as pd
import plotly
import plotly.express as px

notebook_display_config()
print_lib_versions([pd, np, plotly])

df_c_summary = datasets['7d0200a9e0cf'].copy()
print(df_c_summary.shape)
df_c_summary.index.name = None

# df_c_summary.iloc[:5, :9]

style_df_numeric(
  df_c_summary.iloc[:3, :5],
  rename_cols_for_display=True,
).hide_index()

# df_cluster_summary.columns.tolist()

l_port_clusters = ['Porn & Celebrity']

mask_porn_cluster = df_c_summary['cluster_name'].isin(l_port_clusters)
mask_porn_cluster.sum()

l_cols_for_pct = [
  'users_l7_cluster_sum_us',
  'users_l7_cluster_sum_geo',
  
  'seo_users_l7_cluster_sum_us',
  'seo_users_l7_cluster_sum_geo',
  
  'subreddits_in_cluster_count_us',
  'subreddits_in_cluster_count_geo',
  
  'posts_l7_cluster_sum_us',
  'posts_l7_cluster_sum_geo',
]

# for pct:
# algo: replace "cluster_sum" or "cluster_count" with "pct_of"

for c_ in l_cols_for_pct:
  col_new_pct_ = (
    c_.replace('_in_cluster_count', '_pct_of')
    .replace('_cluster_sum', '_pct_of')
  )
  # "clean" col (excluding porn)
  df_c_summary.loc[~mask_porn_cluster, col_new_pct_] = (
    df_c_summary.loc[~mask_porn_cluster, c_] / 
    df_c_summary[~mask_porn_cluster][c_].sum()
  )
  # raw col (including all)
  df_c_summary[f"{col_new_pct_}_raw"] = (
    df_c_summary[c_] / 
    df_c_summary[c_].sum()
  )
del col_new_pct_, c_


c_users7_diff_ = 'users_l7_pct_diff_geo_v_us'
c_seo_users7_diff_ = 'seo_users_l7_pct_diff_geo_v_us'
c_subs_diff_ = 'subreddits_pct_diff_geo_v_us'
c_posts7_diff_ = 'posts_l7_pct_diff_geo_v_us'


df_c_summary[c_users7_diff_] = (
  df_c_summary['users_l7_pct_of_geo'] - 
  df_c_summary['users_l7_pct_of_us']
)
df_c_summary[c_seo_users7_diff_] = (
  df_c_summary['seo_users_l7_pct_of_geo'] - 
  df_c_summary['seo_users_l7_pct_of_us']
)
df_c_summary[c_subs_diff_] = (
  df_c_summary['subreddits_pct_of_geo'] - 
  df_c_summary['subreddits_pct_of_us']
)
df_c_summary[c_posts7_diff_] = (
  df_c_summary['posts_l7_pct_of_geo'] - 
  df_c_summary['posts_l7_pct_of_us']
)

# df_c_summary.head()

# df_c_summary.columns.tolist()

l_cols_summary_1 = [
  'subreddits_in_cluster_count_us',
  'subreddits_in_cluster_count_geo', 
  'subreddits_pct_diff_geo_v_us',
  'cluster_name',
  'top_subreddits_geo',
  'top_subreddits_us',
  # 'subreddits_pct_of_us',
  # 'subreddits_pct_of_geo',
]
d_rename_cols_for_display = {
  'subreddits_in_cluster_count_us': 'subreddits<br>in cluster<br>count us',
  'subreddits_in_cluster_count_geo': 'subreddits<br>in cluster<br>count geo',
  'subreddits_pct_diff_geo_v_us': 'subreddits<br>pct diff<br>geo v us',
}


df_html = (
    style_df_numeric(
      (
        df_c_summary[~mask_porn_cluster]
        # .sort_values(by='subreddits_pct_of_geo', ascending=False)
        .sort_values(by='cluster_name', ascending=True)
        [l_cols_summary_1]
        .rename(columns=d_rename_cols_for_display)
        # .head(20)
      ),
      rename_cols_for_display=True,
      pct_digits=1,
      pct_labels=['pct '],
      l_bar_simple=[d_rename_cols_for_display['subreddits_pct_diff_geo_v_us']],
    )
    .hide_index()
    .set_properties(subset=['top subreddits geo', 'top subreddits us'], **{'text-align': 'left'})
    .set_table_styles([dict(selector='th', props=[('text-align', 'center')])])
    # .set_caption(f"<h4>Clusters with most subreddits in country</h3>")
    # We need to do a CSS hack to freeze the header b/c we can't update 
    #. to a new version of pandas 
    # .set_sticky(axis="columns")
    # .to_html()  # doesn't work either, need to use .render()
    .render()
)

# Concatenating to single string
# Displaying df with sticky header and index
# we need to assign a class so that the style definition above can pick the header and make it sticky
HTML(
  style_freeze_header +
  '<div class="dataframe-div">' +
  df_html.replace("<table ", '<table class="dataframe" ') +
  "\n</div>"
)



