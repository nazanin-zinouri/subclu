# Python Notebook - i18n language detection for subreddits in v0.4.0 topic model

import numpy as np
import pandas as pd

import plotly
import plotly.express as px

import matplotlib.pyplot as plt

print(plotly.__version__)

datasets

from typing import Union, List, Any, Optional, Tuple, Dict

from pandas.io.formats.style import Styler


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

fillna_geo_country = 'US/Other/None'
fillna_language = 'Other_language'

df_lang_wide = (
  datasets['language_snapshot']
  .fillna({'geo_relevant_countries': fillna_geo_country, 'primary_post_language': fillna_language})
)

print(df_lang_wide.shape)

value_counts_and_pcts(
  df_lang_wide['geo_relevant_countries'].fillna(fillna_geo_country),
  add_col_prefix=False,
  count_type='subreddits in v0.4.0 model',
  cumsum=True,
  reset_index=True,
)  # .hide_index()  #.set_caption(f"Subreddit counts by geo-relevance")

# select subs for focus of analysis
df_top_geo_sub_count = value_counts_and_pcts(
  df_lang_wide['geo_relevant_countries'].fillna(fillna_geo_country),
  add_col_prefix=False,
  count_type='subreddits in v0.4.0 model',
  cumsum=True,
  reset_index=True,
  return_df=True,
)

s_selected_geo_relevant_countries = df_top_geo_sub_count.head(12)['geo_relevant_countries']
# s_selected_geo_relevant_countries

value_counts_and_pcts(
  df_lang_wide['primary_post_language'].fillna(fillna_language),
  add_col_prefix=False,
  count_type='subreddits in v0.4.0 model',
  cumsum=True,
  reset_index=True,
)  # .hide_index()  #.set_caption(f"Subreddit counts by geo-relevance")

# select subs for focus of analysis
df_top_lang_sub_count = value_counts_and_pcts(
  df_lang_wide['primary_post_language'].fillna(fillna_language),
  add_col_prefix=False,
  count_type='subreddits in v0.4.0 model',
  cumsum=True,
  reset_index=True,
  return_df=True,
)

s_selected_langs = df_top_lang_sub_count.head(10)['primary_post_language']
# s_selected_langs

value_counts_and_pcts(
  df_lang_wide[['geo_relevant_countries', 'primary_post_language']]
  .fillna({'geo_relevant_countries': fillna_geo_country, 'primary_post_language': fillna_language}),
  ['geo_relevant_countries', 'primary_post_language'],
  add_col_prefix=False,
  count_type='subreddits in v0.4.0 model',
  cumsum=True,
  reset_index=True,
)  # .hide_index()  #.set_caption(f"Subreddit counts by geo-relevance")

df_lang_wide_selected_geos = df_lang_wide[df_lang_wide['geo_relevant_countries'].isin(s_selected_geo_relevant_countries)]
print(df_lang_wide_selected_geos.shape)
df_lang_wide_selected_languages = df_lang_wide[df_lang_wide['primary_post_language'].isin(s_selected_langs)]
print(df_lang_wide_selected_languages.shape)

style_df_numeric(
  df_lang_wide[['post_median_text_len', 'post_median_word_count']]
  .describe().T
  .reset_index()
  .rename(columns={'index': 'column'}),
  l_bar_simple=['mean', '50%']
).hide_index()

df_text_len_country = (
  df_lang_wide_selected_geos
  .groupby(['geo_relevant_countries'])
  ['post_median_text_len']
  .describe()
  .reset_index()
  .sort_values(by=['50%'], ascending=False)
)
# df_text_len_country.shape
style_df_numeric(
  df_text_len_country,
  l_bar_simple=['mean', '50%'],
).hide_index()

fig = px.box(
  df_lang_wide_selected_geos,
  x="post_median_text_len",
  y="geo_relevant_countries",
  # color="geo_relevant_countries",
  log_x=True,
  points=False,
  category_orders={'geo_relevant_countries': df_text_len_country['geo_relevant_countries'].to_list()},
  title="Post Text Length"
)
fig.update_traces(
  orientation='h'
)
fig.update_layout(
  autosize=False,
  width=600,
  height=450,
)

fig.show()

df_word_count_country = (
  df_lang_wide_selected_geos
  .groupby(['geo_relevant_countries'])
  ['post_median_word_count']
  .describe()
  .reset_index()
  .sort_values(by=['50%'], ascending=False)
)
# df_word_count_country.shape
style_df_numeric(
  df_word_count_country,
  l_bar_simple=['mean', '50%'],
).hide_index()

fig = px.box(
  df_lang_wide_selected_geos,
  x="post_median_word_count", 
  y="geo_relevant_countries",
  # color="geo_relevant_countries",
  log_x=True,
  points=False,
  category_orders={'geo_relevant_countries': df_text_len_country['geo_relevant_countries'].to_list()},
  title="Post Word Count by Country"
)
fig.update_traces(
  orientation='h'
)
fig.update_layout(
  autosize=False,
  width=550,
  height=450,
)

fig.show()

df_text_len_lang = (
  df_lang_wide_selected_languages
  .groupby(['primary_post_language'])
  ['post_median_text_len']
  .describe()
  .reset_index()
  .sort_values(by=['50%'], ascending=False)
)
# df_text_len_country.shape
style_df_numeric(
  df_text_len_lang,
  l_bar_simple=['mean', '50%'],
).hide_index()

fig = px.box(
  df_lang_wide_selected_languages,
  x="post_median_text_len", 
  y="primary_post_language",
  # color="primary_post_language",
  log_x=True,
  points=False,
  category_orders={'primary_post_language': df_text_len_lang['primary_post_language'].to_list()},
  title="Post Text Length by Language"
)
fig.update_traces(
  orientation='h'
)
fig.update_layout(
  autosize=False,
  width=600,
  height=450,
)

fig.show()

df_prim_lang_pct_by_geo = (
  df_lang_wide_selected_geos
  .groupby(['geo_relevant_countries'])
  ['primary_post_language_percent']
  .describe()
  .reset_index()
  .sort_values(by=['50%'], ascending=False)
)
# df_text_len_country.shape
style_df_numeric(
  df_prim_lang_pct_by_geo,
  l_bar_simple=['mean', '50%'],
).hide_index()

fig = px.box(
  df_lang_wide_selected_geos,
  x="primary_post_language_percent", 
  y="geo_relevant_countries",
  # color="primary_post_language",
  # log_x=True,
  points=False,
  category_orders={'geo_relevant_countries': df_prim_lang_pct_by_geo['geo_relevant_countries'].to_list()},
  # title="Primary Language"
)
fig.update_traces(
  orientation='h'
)
fig.update_layout(
  autosize=False,
  width=600,
  height=400,
  xaxis_tickformat=".0%",
)

fig.show()

df_prim_lang_pct_by_lang = (
  df_lang_wide_selected_languages
  .groupby(['primary_post_language'])
  ['primary_post_language_percent']
  .describe()
  .reset_index()
  .sort_values(by=['50%'], ascending=False)
)

style_df_numeric(
  df_prim_lang_pct_by_lang,
  l_bar_simple=['mean', '50%'],
).hide_index()

fig = px.box(
  df_lang_wide_selected_languages,
  x="primary_post_language_percent", 
  y="primary_post_language",
  # color="primary_post_language",
  # log_x=True,
  points=False,
  category_orders={'primary_post_language': df_prim_lang_pct_by_lang['primary_post_language'].to_list()},
  title="Primary-Post Language %, by Language"
)
fig.update_traces(
  orientation='h'
)
fig.update_layout(
  autosize=False,
  width=600,
  height=400,
  xaxis_tickformat=".0%",
)

fig.show()

l_cols_long_init = [
  'subreddit_name', 'geo_relevant_countries', 'primary_topic',
  'post_median_text_len',
  'post_median_word_count',
  'posts_for_modeling_count',
]
col_language = 'detected_language'
col_lang_level = 'language_level'
l_cols_long_ix = l_cols_long_init + [col_lang_level]

l_language_cols = [c_ for c_ in df_lang_wide.columns if c_.endswith('_posts_percent')]

# Get the % of posts for primary v. secondary in long-form
df_lang_long = (
  df_lang_wide
  .set_index(l_cols_long_init)
  [l_language_cols]
  .stack()
  .reset_index()
  .rename(columns={
    f"level_{len(l_cols_long_init)}": col_language, 
    0: 'posts_percent'
  })
  .assign(
    **{col_language: lambda x: x[col_language].str.replace('_posts_percent', '')}
  )
)
print(f"{df_lang_long.shape} <- shape with ALL languages")

# Exclude languages for each sub where lang is too low
#. it'll be too noisy to see all these languages stacked
df_lang_long = (
  df_lang_long[df_lang_long['posts_percent'] >= 0.04]
  .sort_values(
    by=[
      'posts_for_modeling_count', 'subreddit_name',
      'posts_percent'
    ], 
    ascending=[
      False, True,
      False,
    ]
  )
)
# fill primary topic b/c plotly will throw errors for null values
df_lang_long['primary_topic'] = df_lang_long['primary_topic'].fillna('None')
print(f"{df_lang_long.shape} <- shape after filtering out low-detection languages")

# df_lang_long['primary_topic'].value_counts(dropna=False).head(10)

# df_lang_wide['primary_topic'].value_counts(dropna=False).head(10)

# country_ = 'Germany'
# mask_country_wide = df_lang_wide['geo_relevant_countries'].str.contains(country_)

# value_counts_and_pcts(
#   df_lang_wide[mask_country_wide],
#   ['primary_post_language', 'geo_relevant_countries',],
#   count_type='subreddits',
#   sort_index=True,
#   cumsum=False,
# )

country_ = 'Germany'
mask_country_wide = df_lang_wide['geo_relevant_countries'].str.contains(country_)

value_counts_and_pcts(
  df_lang_wide[mask_country_wide],
  ['primary_post_language', ],
  count_type='subreddits',
  sort_index=False,
  cumsum=False,
  reset_index=True,
).hide_index()

country_ = 'Germany'
mask_country_wide = df_lang_wide['geo_relevant_countries'].str.contains(country_)

fig = px.box(
  df_lang_wide[mask_country_wide],
  x="primary_post_language_percent", 
  y="primary_post_language",
  # color="primary_post_language",
  # log_x=True,
  points=False,
  category_orders={'primary_post_language': df_lang_wide[mask_country_wide]['primary_post_language'].value_counts().index.to_list()},
  title="Germany: Primary-Post Language %, by Language"
)
fig.update_traces(
  orientation='h'
)
fig.update_layout(
  autosize=False,
  width=600,
  height=400,
  xaxis_tickformat=".0%",
)

fig.show()

# Check subreddits that are not in German or English. anything stand out?

country_ = 'Germany'
mask_country_wide = df_lang_wide['geo_relevant_countries'].str.contains(country_)
mask_mature_and_sex_topic = (
  (df_lang_wide['primary_topic'].str.contains('Mature')) |
  (df_lang_wide['primary_topic'].str.contains('Sex'))
)

l_cols_display_sub_info = [
  'primary_topic',
  'rating_short',
  'subreddit_name',
  'post_median_text_len',
  'posts_for_modeling_count',
  'primary_post_language',
  'primary_post_language_percent',
  'secondary_post_language',
  'secondary_post_language_percent',
]

style_df_numeric(
  df_lang_wide[
    mask_country_wide &
    mask_mature_and_sex_topic & 
    ~(df_lang_wide['primary_post_language'].isin(['German', 'English']))
  ]
  [l_cols_display_sub_info]
  .fillna({'primary_topic': '', 'rating_short': ''})
  .sort_values(by=['posts_for_modeling_count', 'post_median_text_len'], ascending=[False, True]),
  rename_cols_for_display=True,
  l_bar_simple=['post_median_text_len', 'primary_post_language_percent',]
).hide_index()

country_ = 'Germany'
mask_country_wide = df_lang_wide['geo_relevant_countries'].str.contains(country_)

style_df_numeric(
  df_lang_wide[
    mask_country_wide &
    ~mask_mature_and_sex_topic & 
    ~(df_lang_wide['primary_post_language'].isin(['German', 'English']))
  ]
  [l_cols_display_sub_info]
  .fillna({'primary_topic': '', 'rating_short': ''})
  .sort_values(by=['posts_for_modeling_count', 'post_median_text_len'], ascending=[False, True])
  .head(12),
  rename_cols_for_display=True,
  l_bar_simple=['post_median_text_len', 'primary_post_language_percent',]
)

country_ = 'Germany'
mask_country_wide = df_lang_wide['geo_relevant_countries'].str.contains(country_)

fig = px.bar(
  df_lang_wide[mask_country_wide],
  x='posts_percent',
  y='subreddit_name',
  color=col_language,
  orientation='h',
  title=f"Detected language for <b>{country_}</b>-geo-relevant subs <br>primary_topic=<b>{primary_topic_}</b>",
  # facet_row='primary_topic',
  # facet_col_wrap=3,
)
fig.update_layout(
  autosize=False,
  width=800,
  height=450,
)

fig.show()

country_ = 'Germany'
mask_country = df_lang_long['geo_relevant_countries'].str.contains(country_)

for primary_topic_ in ['None', 'Place', 'Gaming']:
  # primary_topic_ = 'Place'
  mask_primary_topic = df_lang_long['primary_topic'] == primary_topic_

  fig = px.bar(
    df_lang_long[mask_primary_topic & mask_country].head(25),
    x='posts_percent',
    y='subreddit_name',
    color=col_language,
    orientation='h',
    title=f"Detected language for <b>{country_}</b>-geo-relevant subs <br>primary_topic=<b>{primary_topic_}</b>",
    # facet_row='primary_topic',
    # facet_col_wrap=3,
  )
  fig.update_layout(
    autosize=False,
    width=800,
    height=450,
  )

  fig.show()

country_ = 'Germany'
mask_country = df_lang_long['geo_relevant_countries'].str.contains(country_)

for primary_topic_ in ['None', 'Place', 'Gaming']:
  # primary_topic_ = 'Place'
  mask_primary_topic = df_lang_long['primary_topic'] == primary_topic_

  fig = px.bar(
    df_lang_long[mask_primary_topic & mask_country].head(25),
    x='posts_percent',
    y='subreddit_name',
    color=col_language,
    orientation='h',
    title=f"Detected language for <b>{country_}</b>-geo-relevant subs <br>primary_topic=<b>{primary_topic_}</b>",
    # facet_row='primary_topic',
    # facet_col_wrap=3,

  )
  fig.update_layout(
    autosize=False,
    width=800,
    height=450,
  )

  fig.show()

country_ = 'Mexico'
mask_country = df_lang_long['geo_relevant_countries'].str.contains(country_)

for primary_topic_ in ['None', 'Place', 'Gaming']:
  # primary_topic_ = 'Place'
  mask_primary_topic = df_lang_long['primary_topic'] == primary_topic_

  fig = px.bar(
    df_lang_long[mask_primary_topic & mask_country].head(35),
    x='posts_percent',
    y='subreddit_name',
    color=col_language,
    orientation='h',
    title=f"Detected language for <b>{country_}</b>-geo-relevant subs <br>primary_topic=<b>{primary_topic_}</b>",
    # facet_row='primary_topic',
    # facet_col_wrap=3,

  )
  fig.update_layout(
    autosize=False,
    width=800,
    height=500,
  )

  fig.show()

country_ = 'Mexico'
primary_topic_ = 'Place'
mask_primary_topic = df_lang_long['primary_topic'] == primary_topic_
mask_country = df_lang_long['geo_relevant_countries'].str.contains(country_)

fig = px.bar(
  df_lang_long[mask_primary_topic & mask_country].head(50),
  x='posts_percent',
  y='subreddit_name',
  color=col_language,
  orientation='h',
  title=f"Detected language for <b>{country_}</b>-geo-relevant subs <br>primary_topic=<b>{primary_topic_}</b>",
  # facet_row='primary_topic',
  # facet_col_wrap=3,
  
)
fig.update_layout(
  autosize=False,
  width=800,
  height=500,
)

fig.show()

country_ = 'Mexico'
primary_topic_ = 'Culture, Race, and Ethnicity'
mask_primary_topic = df_lang_long['primary_topic'] == primary_topic_
mask_country = df_lang_long['geo_relevant_countries'].str.contains(country_)

fig = px.bar(
  df_lang_long[mask_primary_topic & mask_country].head(50),
  x='posts_percent',
  y='subreddit_name',
  color=col_language,
  orientation='h',
  title=f"Detected language for <b>{country_}</b>-geo-relevant subs <br>primary_topic=<b>{primary_topic_}</b>",
)
fig.update_layout(
  autosize=False,
  width=800,
  height=400,
)

fig.show()







mask_country = df_lang_wide['geo_relevant_countries'].str.contains('Mexico')

l_cols_long_init = [
  'subreddit_name', 'geo_relevant_countries', 'primary_topic',
  'post_median_text_len',
  'post_median_word_count',
  'posts_for_modeling_count',
]
col_lang_level = 'language_level'
l_cols_long_ix = l_cols_long_init + [col_lang_level]

df_lang_long = (
# Get the language-name primary v. secondary in long-form
  (
    df_lang_wide[mask_country]
    .set_index(l_cols_long_init)
    [['primary_post_language', 'secondary_post_language']]
    .stack()
    .reset_index()
    .rename(columns={
        f"level_{len(l_cols_long_init)}": col_lang_level, 
        0: 'language_name'
    })
    .set_index(l_cols_long_ix)
  )
  .merge(
    # Get the % of posts for primary v. secondary in long-form
    (
      df_lang_wide[mask_country]
      .set_index(l_cols_long_init)
      [['primary_post_language_percent', 'secondary_post_language_percent']]
      .stack()
      .reset_index()
      .rename(columns={
        f"level_{len(l_cols_long_init)}": col_lang_level, 
        0: 'posts_percent'
      })
      .assign(
        **{col_lang_level: lambda x: x[col_lang_level].str.replace('_percent', '')}
      )
      .set_index(l_cols_long_ix)
    ),
    how='outer',
    left_index=True,
    right_index=True,
  )
  .reset_index()
  .sort_values(
    by=['posts_for_modeling_count', 'subreddit_name'], 
    ascending=[False, True]
  )
)
df_lang_long.head(5)


fig = px.bar(
  df_lang_long.head(15),
  x='posts_percent',
  y='subreddit_name',
  color='language_name',
  orientation='h',
  
)
fig.show()

# # Get the % of posts for primary v. secondary in long-form
# (
#   df_lang_wide[mask_country]
#   .set_index(
#     ['subreddit_name', 'geo_relevant_countries',
#     ]
#   )
#   [['primary_post_language_percent', 'secondary_post_language_percent']]
#   .stack()
#   .reset_index()
#   .rename(columns={'level_2': 'language_level', 0: 'posts_percent'})
#   .assign(
#     **{'language_level': lambda x: x['language_level'].str.replace('_percent', '')}
#   )
# )

mask_country = df_lang['geo_relevant_countries'].str.contains('Mexico')

fig = px.bar(
  df_lang[mask_country].head(6),
  y='primary_post_language_percent',
  x='subreddit_name',
  color='primary_post_language',
  
)
fig.show()

for c_ in l_countries_to_check:
  px.

