"""
Script to run & automate plots to visualize progress/process.

Try using this instead of a notebook because of the problems I had with
notebooks failing to save and losing a day's worth of work ;_;

# Provenance of data
The jobs used to create these embeddings came from these jobs:
- Subreddit descriptions: `84dd5a3878534f72b6442bcc0e4c8b95`
- Comments & posts: `3bf280ee76fc4595afc5e8cbaaf79a7d`

They were merged in notebook: `djb_09.00....ipynb`
"""
# %load_ext autoreload
# %autoreload 2
from datetime import datetime
import gc
import os
import logging
from logging import info

import fse
from fse.models import uSIF
import gensim
from gensim.models.fasttext import FastText, load_facebook_vectors
import joblib

import numpy as np
import pandas as pd
import plotly
import plotly.express as px
import seaborn as sns

import mlflow

from sklearn.manifold import TSNE
from sklearn.decomposition import TruncatedSVD

from subclu.data.fasttext_utils import (
    download_ft_pretrained_model,
    get_df_for_most_similar,
    get_project_subfolder,
)
from subclu.utils import set_working_directory
from subclu.utils.eda import (
    setup_logging, counts_describe, value_counts_and_pcts,
    notebook_display_config, print_lib_versions,
    style_df_numeric
)
from subclu.utils.mlflow_logger import MlflowLogger
from subclu.eda.aggregates import (
    compare_raw_v_weighted_language,
    get_language_by_sub_wide,
    get_language_by_sub_long,
)
from subclu.utils.data_irl_style import (
    get_colormap, theme_dirl,
    get_color_dict, base_colors_for_manual_labels,
    check_colors_used,
)
from subclu.data.data_loaders import LoadPosts, LoadSubreddits, create_sub_level_aggregates


print_lib_versions([fse, gensim, joblib, np, pd, plotly,])


# plotting
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.dates as mdates
plt.style.use('default')

setup_logging()
notebook_display_config()

# Set paths
path_this_ft_model = get_project_subfolder(
    f"data/models/fse/manual_merge_2021-06-07_17"
)

info(f"Loading data from: {path_this_ft_model}")
df_v_posts_merged_tsne2 = pd.read_parquet(
    path_this_ft_model /
    'df_vectorized_posts_svd_tsne-init_pca-perplexity_30-rand_state_42-ids_index-111669_by_2.parquet'
)
info(df_v_posts_merged_tsne2.shape)


# Load compressed vectors (tsne)
df_v_posts_merged_tsne2 = pd.read_parquet(
    path_this_ft_model /
    'df_vectorized_posts_svd_tsne-init_pca-perplexity_30-rand_state_42-ids_index-111669_by_2.parquet'
)
info(f"{df_v_posts_merged_tsne2.shape} - df_v posts")

df_v_subs_merged_tsne2 = pd.read_parquet(
    path_this_ft_model /
    'df_vectorized_subs_svd_tsne-agg_to_subreddit-ids_index-167_by_2.parquet'
)
info(f"{df_v_subs_merged_tsne2.shape} - df_v SUBS")


# Use list of subs to filter out in some plots
# l_large_nsfw_subs = [
#     'wixbros', 'katjakrasavicenudes',
#     'deutschetributes', 'germannudes',
#     'annitheduck', 'germanonlyfans',
#     'loredana', 'nicoledobrikovof',
#     'germansgonewild', 'elisaalinenudes',
#     'marialoeffler', 'germanwomenandcouples',
#     'germancelebritiesfap2', 'germancelebs',
#     'nicoledobrikov', 'elisaaline1',
#     'nicoledobrikov1', 'nicoledobrikovofs', 'germanpornstuff',
# ]

#
# Load preprocessed posts
#
l_cols_load = [
    # IDs
    'subreddit_name',
    'subreddit_id',
    'post_id',
    #     'user_id',
    #     'thing_type',

    # Meta
    'submit_date',
    #     'removed',
    'upvotes',
    #     'successful',
    #     'app_name',
    'combined_topic_and_rating',
    'post_type',
    'post_nsfw',
    'geolocation_country_code',

    # Language & text content
    'post_url',
    'language',
    'probability',
    'weighted_language',
    'weighted_language_probability',
    'text_len',
    'text_word_count',
    'post_url_for_embeddings',
    'text'
]

col_manual_labels = 'manual_topic_and_rating'

df_posts = LoadPosts(
    bucket_name='i18n-subreddit-clustering',
    folder_path='posts/2021-05-19',
    columns=l_cols_load,
    col_new_manual_topic=col_manual_labels,
).read_and_apply_transformations()
info(f"{df_posts.shape} posts with metadata")

df_subs = LoadSubreddits(
    bucket_name='i18n-subreddit-clustering',
    folder_path='subreddits/2021-06-01',
    columns=None,
    col_new_manual_topic=col_manual_labels,
).read_and_apply_transformations()
info(f"{df_subs.shape} SUBS with metadata")

#
# Aggregates for language detected + post type
# merge left because it doesn't make sense to get data on subreddits for which we don't have posts
info(f"Add post-level aggregates to df_subs")
df_subs = (
    create_sub_level_aggregates(df_posts)
    .merge(
        df_subs,
        how='left',
        left_index=True,
        right_on=['subreddit_name'],
    )
)
# print(value_counts_and_pcts(df_subs[col_manual_labels], top_n=None, return_df=True))

posts_hover_data = "<br>".join([
    "subreddit name: %{customdata[0]}",
    "subreddit manual label: %{customdata[1]}",
   "post text: %{customdata[2]}",
   "  %{customdata[3]}"
])

l_custom_data_posts = ['subreddit_name', col_manual_labels, 'text_1', 'text_2']

# %%time
#
# fig = px.scatter(
#     df_v_posts_tsne_meta,
#     y='tsne_0', x='tsne_1',
#     color=col_manual_labels,
#     custom_data=l_custom_data_posts,
#     color_discrete_map=d_manual_label_colors,
#     opacity=0.7,
# )
# fig.update_traces(hovertemplate=posts_hover_data)
# fig.update_layout(
#     title_text=(
#         f"{n_posts:,.0f} Posts from {n_subs} German-relevant subreddits"
#         f"<br>Using posts from {first_date} to {last_date}"
#     ),
#     title_x=0.5,
#     width=900,
#     height=700,
# #     uniformtext_minsize=8, uniformtext_mode='hide'
# )
# fig.show()

