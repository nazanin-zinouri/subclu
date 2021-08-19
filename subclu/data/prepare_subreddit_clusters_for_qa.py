# !/usr/bin/env python
# coding: utf-8

"""
Use these functions to prepare the cluster data for QA in a spreadsheet.

In the short term a spreadsheet is the easiest/best thing we can use to collaborate
for QA.

In the long term, maybe we can add some automation or tooling around it?
- e.g., similar to the tagging system or mechanical Turk to verify clusters.
"""


# # Purpose
#
# Reshape the cluster output data so that we can have a CSV file that is easy to share and use for QA with multiple people.

# # Imports & Setup

# In[1]:

# get_ipython().run_line_magic('load_ext', 'autoreload')
# get_ipython().run_line_magic('autoreload', '2')

# In[2]:


from datetime import datetime
import logging
from pprint import pprint

import numpy as np
import pandas as pd
import plotly
import plotly.express as px
import seaborn as sns

import mlflow

import omegaconf
from omegaconf import OmegaConf
from subclu.utils.hydra_config_loader import LoadHydraConfig

import subclu
from subclu.data.data_loaders import LoadSubreddits, LoadPosts
from subclu.utils import set_working_directory
from subclu.utils.eda import (
    setup_logging, counts_describe, value_counts_and_pcts,
    notebook_display_config, print_lib_versions,
    style_df_numeric, reorder_array,
)
from subclu.utils.language_code_mapping import (
    L_CLD3_CODES_FOR_LANGUAGES_IN_USE_MULTILINGUAL,
    D_CLD3_CODE_TO_LANGUAGE_NAME,
)
from subclu.utils.mlflow_logger import MlflowLogger
from subclu.eda.aggregates import compare_raw_v_weighted_language
from subclu.utils.data_irl_style import (
    get_colormap, theme_dirl
)

# ---
from tqdm.auto import tqdm
from subclu.data.counterpart_loaders import (
    LoadCounterpartSeeds,
    combine_reshaped_seeds
)

print_lib_versions([np, pd, plotly, sns, subclu])

# In[3]:


# plotting
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.dates as mdates

plt.style.use('default')

setup_logging()
notebook_display_config()

# # Load config
# Use Hydra configs to make it easier to replicate the run.
#
# Copy the config to the model folder so it's easier to trace it back

# In[4]:


config_clustering_v032a = LoadHydraConfig(
    config_name='cluster_output',
    config_path="../config",
    overrides=[
        f"data_text_and_metadata=top_subreddits_2021_07_16",
        f"data_embeddings_to_cluster=top_subs-2021_07_16-use_multi_lower_case_false_00",
        f"data_cluster_outputs=top_subreddits_v0.3.2a",
    ],
)
config_seeds = LoadHydraConfig(
    config_name='seed_counterparts_germany_2021-08-17',
    config_path="../config/data_subreddit_counterparts",
)

# pprint(config_clustering_v032a.config_dict, indent=2)
pprint(config_seeds.config_dict)

# In[5]:


# Extract CLUSTERING keys into easier to call
d_embeddings = config_clustering_v032a.config_dict['data_embeddings_to_cluster']
d_conf_meta = config_clustering_v032a.config_dict['data_text_and_metadata']
d_clusters = config_clustering_v032a.config_dict['data_cluster_outputs']

# embedding data
run_uuid = d_embeddings['run_uuid']

f_embeddings_sub_level = d_embeddings['df_sub_level_agg_c_post_comments_and_sub_desc']
f_embeddings_post_level = d_embeddings['df_post_level_agg_c_post_comments_sub_desc']

f_sub_distance_c = d_embeddings['df_sub_level_agg_c_post_comments_and_sub_desc_similarity']
f_sub_dist_pair_c = d_embeddings['df_sub_level_agg_c_post_comments_and_sub_desc_similarity_pair']

l_ix_sub = d_embeddings['l_ix_sub']  # 'subreddit_id',  b/c of dask's multi-index, I'm only using name
l_ix_post = d_embeddings['l_ix_post']

# cluster output data
bucket_and_folder_prefix = f"gs://{d_clusters['bucket_name']}/{d_clusters['folder_model_outputs']}"
f_subs_c_cluster_labels = d_clusters['f_subs_agg_c_cluster_labels']
f_subs_c_similarity_pair = d_clusters['f_subs_agg_c_similarity_pair']
col_best_clusters_a = d_clusters['col_best_clusters_a']

# # Load German geo-relevant & Ambassador subreddits
#
# Need to export them from BigQuery first (sigh)

# In[6]:


config_geo_relevant = LoadHydraConfig(
    config_name='geo_relevant_subs_2021-08-18',
    config_path="../config/data_geo_relevant_subreddits",
)
config_ambassador = LoadHydraConfig(
    config_name='de_ambassador_subs_2021-08-18',
    config_path="../config/data_ambassador_subreddits",
)

# In[7]:


get_ipython().run_cell_magic('time', '',
                             '\ndf_geo = pd.read_parquet(\n    f"gs://{config_geo_relevant.config_dict[\'bucket_name\']}/"\n    f"{config_geo_relevant.config_dict[\'folder_data_prefix\']}/"\n)\nprint(df_geo.shape)')

# In[8]:


get_ipython().run_cell_magic('time', '',
                             '\ndf_ambassador = pd.read_parquet(\n    f"gs://{config_ambassador.config_dict[\'bucket_name\']}/"\n    f"{config_ambassador.config_dict[\'folder_data_prefix\']}/"\n)\nprint(df_ambassador.shape)')

# # Load Distances & Clusters (cluster IDs)
#
# These should be the same files that feed the bigquery tables:
# - `reddit-employee-datasets.david_bermejo.subclu_subreddit_distance_v0032_c_posts_and_comments_and_meta`
# - `reddit-employee-datasets.david_bermejo.subclu_subreddit_cluster_labels_v032_a`
#

# In[9]:


print(bucket_and_folder_prefix)
print(f_subs_c_cluster_labels)
print(f_subs_c_similarity_pair)
print(col_best_clusters_a)

# In[13]:


# !gsutil ls -r $bucket_clusters
# !gsutil ls $bucket_and_folder_prefix


# In[14]:


get_ipython().run_cell_magic('time', '',
                             'df_subs_cluster = pd.read_parquet(f"{bucket_and_folder_prefix}/{f_subs_c_cluster_labels}")\n\nprint(df_subs_cluster.shape)')

# In[15]:


get_ipython().run_cell_magic('time', '',
                             '# this one will take a while (over 15 seconds) because it\'s over 14 million rows...\ndf_subs_distance = pd.read_parquet(f"{bucket_and_folder_prefix}/{f_subs_c_similarity_pair}")\n\nprint(df_subs_distance.shape)')

# In[17]:


df_subs_cluster[[c for c in df_subs_cluster.columns if 'description' not in c]].iloc[:6, :14]

# In[16]:


df_subs_distance.iloc[:6, :14]

# # Export cluster data to CSV so that it's easy to edit and share
#
# Need to have a central place to do QA.
#
# Columns to make data easier to work with/useful:
# - add column with geo-relevant column
# - add column to show whether a subreddit is an ambassador sub
#
# - add column to subreddit URL
# - add column to subreddit URL with google translate
# - add a flag for clusters that have at least 1 German (or Ambassador) subreddit
#     - so we can filter down to just DE clusters (DE <> DE, DE <> EN)
#
# - new col: related clusters
#     - for all clusters, find distance between the two biggest subreddits in each cluster & show closest groups
#     - need to think through what this actually means in practice. But the idea is: i should be able to see related clusters close together so it's easy to re-assign clusters if they fit better in an adjacent subreddit.
#
# (in spreadsheets):
# - translate the subreddit name
# - translate the subreddit description
#

# In[18]:


# columns used for new-cols & aggregations
col_manual_topic = 'manual_topic_and_rating'
col_ger_subs_count = 'german_subs_in_cluster'
col_ger_or_ambassador = 'german_or_ambassador_sub'
col_cluster_users_l28_sum = 'users_l28_for_cluster'
col_cluster_primary_topics = 'primary_topics_in_cluster'

l_cluster_cols_for_qa = [
    'cluster_id_agg_ward_cosine_200',
    'subreddit_name',
    'subreddit_id',
    'subreddit_title',
    'subreddit_public_description',

    'over_18',
    'rating',
    'topic',
    'rating_version',
    'topic_version',
    'subreddit_language',
    'primary_post_language',
    'primary_post_language_percent',
    'English_posts_percent',
    'German_posts_percent',

    'primary_post_type',
    'primary_post_type_percent',
    'posts_for_modeling_count',
    'post_median_word_count',

    'subscribers',
    'users_l28',
    'posts_l28',
    'comments_l28',

    'image_post_type_percent',
    'text_post_type_percent',
    'link_post_type_percent',

    # hide these in spreadsheet
    'mlflow_aggregation_run_uuid',
    col_manual_topic,
    'pt',

]

l_cluster_cols_for_qa_front = [
    'cluster_id_agg_ward_cosine_200',
    col_ger_subs_count,
    col_cluster_primary_topics,
    'subreddit_name',
    'subreddit_id',
    'subreddit_title',
    'subreddit_public_description',

    'subreddit_url',
    'subreddit_url_with_google_translate',
    col_ger_or_ambassador,
    'geo_country_code',
    'ambassador_sub',
]
df_subs_cluster_qa = (
    df_subs_cluster[l_cluster_cols_for_qa].copy()
        .merge(
        df_geo[['subreddit_name', 'geo_country_code']],
        how='left',
        on='subreddit_name'
    )

)

# Add columns to flag DE & ambassador subs
df_subs_cluster_qa['ambassador_sub'] = np.where(
    df_subs_cluster_qa['subreddit_name'].isin(df_ambassador['subreddit_name']),
    'yes',
    'no',
)
df_subs_cluster_qa[col_ger_or_ambassador] = np.where(
    (
            (df_subs_cluster_qa['ambassador_sub'] == 'yes') |
            (df_subs_cluster_qa['geo_country_code'] == 'DE')
    ),
    'yes',
    'no',
)

# Add sub URLs
df_subs_cluster_qa['subreddit_url'] = 'https://www.reddit.com/r/' + df_subs_cluster_qa['subreddit_name']
df_subs_cluster_qa['subreddit_url_with_google_translate'] = (
        'https://translate.google.com/translate?hl=&sl=auto&tl=en&u=' +
        df_subs_cluster_qa['subreddit_url']
)

# count of German subs per cluster
df_german_subs_per_cluster = (
    df_subs_cluster_qa[df_subs_cluster_qa[col_ger_or_ambassador] == 'yes'].groupby([col_best_clusters_a])
        .agg(
        **{col_ger_subs_count: ('subreddit_name', 'nunique'), }
    )
)
df_subs_cluster_qa = df_subs_cluster_qa.merge(
    df_german_subs_per_cluster.reset_index(),
    how='left',
    on=col_best_clusters_a,
)
df_subs_cluster_qa[col_ger_subs_count] = df_subs_cluster_qa[col_ger_subs_count].fillna(0).astype(int)

# Sum users_28 in cluster to help prioritize big clusters first
# This could back-fire if the first subreddits are too broad... might drop it if it doesn't help
df_cluster_aggs = (
    df_subs_cluster_qa.groupby([col_best_clusters_a], as_index=False)
        .agg(
        **{col_cluster_users_l28_sum: ('users_l28', 'sum'), }
    )
)
df_subs_cluster_qa = df_subs_cluster_qa.merge(
    df_cluster_aggs,
    how='left',
    on=col_best_clusters_a,
)

# ========================
# get primary topics per cluster
# ===
df_clusters_primary_topics = (
    df_subs_cluster_qa.groupby([col_best_clusters_a, col_manual_topic], as_index=False)
        .agg(
        subreddit_count=('subreddit_name', 'nunique')
    )
)
# Set the `uncategorized` labels to negative and sort by subreddit_count so it always shows up last
df_clusters_primary_topics['subreddit_count'] = np.where(
    df_clusters_primary_topics[col_manual_topic] == 'uncategorized',
    -df_clusters_primary_topics['subreddit_count'],
    df_clusters_primary_topics['subreddit_count']
)
df_clusters_primary_topics = (
    df_clusters_primary_topics
        .sort_values(by=[col_best_clusters_a, 'subreddit_count'], ascending=False)
)
# df_clusters_primary_topics.head()

# convert from rows to a list of topics
df_clusters_primary_topics = (
    df_clusters_primary_topics
        .groupby(col_best_clusters_a, as_index=True)[col_manual_topic]
        .apply(list)
        .to_frame()
        .reset_index()
)
# Remove python's list-related characters from output
df_clusters_primary_topics[col_manual_topic] = (
    df_clusters_primary_topics[col_manual_topic].astype(str)
        .str.replace('[', '', regex=False)
        .str.replace(']', '', regex=False)
        .str.replace("'", '', regex=False)
        .str.replace('"', '', regex=False)
)
df_clusters_primary_topics = df_clusters_primary_topics.rename(columns={col_manual_topic: col_cluster_primary_topics})
# Merge new column back to QA df
df_subs_cluster_qa = df_subs_cluster_qa.merge(
    df_clusters_primary_topics,
    how='left',
    on=col_best_clusters_a,
)

# sort rows to make QA easier
df_subs_cluster_qa = (
    df_subs_cluster_qa
        .sort_values(
        by=[col_cluster_primary_topics, col_ger_subs_count, col_cluster_users_l28_sum, col_best_clusters_a,
            col_ger_or_ambassador, 'users_l28'],
        ascending=[False, False, False, True, False, False],
    )
        .reset_index(drop=True)
)

# Reorganize column order
df_subs_cluster_qa = df_subs_cluster_qa[reorder_array(l_cluster_cols_for_qa_front, df_subs_cluster_qa.columns)]
print(df_subs_cluster_qa.shape)

# In[19]:


for clust_ in tqdm(list(df_subs_cluster_qa[col_best_clusters_a].unique())[:3]):
    mask_cluster = df_subs_cluster_qa[col_best_clusters_a] == clust_
    if df_subs_cluster_qa[mask_cluster][col_ger_subs_count].values[0] > 0:
        display(
            style_df_numeric(
                df_subs_cluster_qa[mask_cluster].drop(['subreddit_public_description'], axis=1).iloc[:10, :16]
            )
        )

# # Create new aggregate/summary df to use as the key for cluster-level labels

# In[ ]:


TODO

# In[104]:


col_total_subs_count = 'subreddits_in_cluster_count'
col_ger_subs_count_redo = 'german_subreddits_in_cluster_count'
col_ger_subs_pct = 'german_subreddits_in_cluster_percent'

# Create base query with counts
df_cluster_level_qa = (
    df_subs_cluster_qa.groupby([col_best_clusters_a], as_index=False)
        .agg(
        **{col_total_subs_count: ('subreddit_name', 'nunique'), }
    )
).merge(
    df_german_subs_per_cluster,
    how='left',
    on=col_best_clusters_a,
).rename(columns={col_ger_subs_count: col_ger_subs_count_redo})

df_cluster_level_qa[col_ger_subs_count_redo] = (
    df_cluster_level_qa[col_ger_subs_count_redo].fillna(0)
).astype(int)

# get % of German subs in cluster
df_cluster_level_qa[col_ger_subs_pct] = (
        df_cluster_level_qa[col_ger_subs_count_redo] /
        df_cluster_level_qa[col_total_subs_count]
)

# Add primary topics
df_cluster_level_qa = df_cluster_level_qa.merge(
    df_clusters_primary_topics,
    how='left',
    on=col_best_clusters_a,
)

# TODO(djb): instead of only the top subreddit, maybe pick the top 3
# . it's hard to see a pattern when we only look at one subreddit
# append top German sub for each cluster
#  it assumes that subs-qa has already been sorted
mask_ger_or_amb_subs = df_subs_cluster_qa[col_ger_or_ambassador] == 'yes'

df_cluster_level_qa = df_cluster_level_qa.merge(
    df_subs_cluster_qa[mask_ger_or_amb_subs]
        .drop_duplicates(subset=[col_best_clusters_a, ], keep='first')
    [[col_best_clusters_a, 'subreddit_name', ]],
    how='left',
    on=col_best_clusters_a,
).rename(columns={'subreddit_name': 'top_german_subreddit'})

df_cluster_level_qa = df_cluster_level_qa.merge(
    df_subs_cluster_qa[~mask_ger_or_amb_subs]
        .drop_duplicates(subset=[col_best_clusters_a, ], keep='first')
    [[col_best_clusters_a, 'subreddit_name', ]],
    how='left',
    on=col_best_clusters_a,
).rename(columns={'subreddit_name': 'top_non_german_subreddit'})

df_cluster_level_qa.shape

# In[105]:


# does it make sense to remove uncategorized??
# df_cluster_level_qa[col_cluster_primary_topics] = (
#     df_cluster_level_qa[col_cluster_primary_topics].str.replace(", uncategorized", '')
# )


# In[106]:


# df_cluster_top_subs = (
#     pd.concat([
#         df_subs_cluster_qa[mask_ger_or_amb_subs]
#         .drop_duplicates(subset=[col_best_clusters_a,], keep='first')
#         [[col_best_clusters_a, 'subreddit_name', col_ger_or_ambassador]],
#         df_subs_cluster_qa[~mask_ger_or_amb_subs]
#         .drop_duplicates(subset=[col_best_clusters_a,], keep='first')
#         [[col_best_clusters_a, 'subreddit_name', col_ger_or_ambassador]]
#     ])
#     .sort_values(by=col_best_clusters_a)
#     #.set_index([col_best_clusters_a, ])
#     # .unstack()
#     # .to_frame()
# )  #.head(20)


# In[107]:


# df_cluster_top_subs.head()


# In[108]:


# style_df_numeric(df_cluster_top_subs.head(), rename_cols_for_display=True)


# In[109]:


style_df_numeric(df_cluster_level_qa.head(), rename_cols_for_display=True)

# In[110]:


style_df_numeric(df_cluster_level_qa.tail(), rename_cols_for_display=True)

# In[ ]:


# In[25]:


df_clusters_primary_topics.head()

# In[ ]:


# # Save df to GCS so we can convert it into a spreadsheet

# In[ ]:


BREAK

# In[183]:


print(bucket_and_folder_prefix)
print(f_subs_c_cluster_labels)
print(f_subs_c_similarity_pair)
print(col_best_clusters_a)

# ## Save cluster-level df

# In[ ]:


# ## Save subreddit-level df

# In[ ]:


BREAK

# In[185]:


shape_ = df_subs_cluster_qa.shape
df_subs_cluster_qa.to_csv(
    (
        f"{bucket_and_folder_prefix}/df_subs_only-meta_and_clustering_for_qa"
        f"-{datetime.utcnow().strftime('%Y-%m-%d_%H%M')}"
        f"-{shape_[0]}_by_{shape_[1]}.csv"
    ),
    index=False
)
