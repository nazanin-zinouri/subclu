"""
Utils to load data & apply common ETL/cleanup/aggregations.

Ideally, call these functions so that new columns have the same definitions
across different notebooks/experiments.
"""
from logging import info
from typing import Dict, Union

import numpy as np
import pandas as pd

from ..eda.aggregates import get_language_by_sub_wide
# from subclu.eda.aggregates import get_language_by_sub_wide


class LoadPosts:
    """
    Class to load posts data and apply some standard transformations.
    Currently defaults to loading form GCS and that the files are parquet.

    We could extend it to query from BQ if needed.
    """

    def __init__(
            self,
            bucket_name: str = 'i18n-subreddit-clustering',
            folder_path: str = 'posts/2021-05-19',
            columns: iter = None,
            col_new_manual_topic: str = 'manual_topic_and_rating',
    ):
        self.bucket_name = bucket_name
        self.folder_path = folder_path
        self.columns = columns
        self.col_new_manual_topic = col_new_manual_topic

    def read_raw(self) -> pd.DataFrame:
        """Read raw files w/o any transformations"""
        return pd.read_parquet(
            path=f"gs://{self.bucket_name}/{self.folder_path}",
            columns=self.columns
        )

    def read_and_apply_transformations(self) -> pd.DataFrame:
        """Read & apply all transformations in a single call"""
        info(f"Reading raw data...")
        df = self.read_raw()

        info(f"  Applying transformations...")
        # plotly throws out errors if we try use a col with nulls in a plot
        if 'post_nsfw' in df.columns:
            df['post_nsfw'] = df['post_nsfw'].fillna('unlabeled')

        if 'weighted_language' in df.columns:
            df['weighted_language_top'] = np.where(
                df['weighted_language'].isin(['en', 'de', ]),
                df['weighted_language'],
                'other'
            )
        if 'post_type' in df.columns:
            df['post_type_agg3'] = np.where(
                df['post_type'].isin(['text', 'image', 'link']),
                df['post_type'],
                'other'
            )
            df['post_type_agg2'] = np.where(
                df['post_type'].isin(['text', 'image']),
                df['post_type'],
                'other'
            )

        if self.col_new_manual_topic not in df.columns:
            df[self.col_new_manual_topic] = create_new_manual_topic_column(df)

        return df


class LoadSubreddits(LoadPosts):
    """Build on top of Load Posts to standardize loading subreddit metadata

    No need to over-ride LoadPost function until/unless we want to append
    post-level aggregates, but that's better handled as a separate function.
    """

    def __init__(
            self,
            bucket_name: str = 'i18n-subreddit-clustering',
            folder_path: str = 'subreddits/2021-06-01',
            folder_posts: str = 'posts/2021-05-19',
            columns: iter = None,
            col_new_manual_topic: str = 'manual_topic_and_rating'
    ) -> None:
        super().__init__(bucket_name, folder_path, columns, col_new_manual_topic)
        self.folder_posts = folder_posts

    def read_apply_transformations_and_merge_post_aggs(
            self,
            df_posts: pd.DataFrame = None,
    ) -> pd.DataFrame:
        """Besides loading the sub-data, load post-level data & merge aggregates from post-level"""
        if df_posts is None:
            info(f"Loading df_posts from: {self.folder_posts}")
            # limit to only cols absolutely needed to save time & RAM
            l_cols_post_aggs_only = [
                'subreddit_name',
                'subreddit_id',
                'weighted_language',    # For language aggs
                'post_type',            # For post aggs
                'combined_topic_and_rating',    # Needed for new manual label
                'text_word_count',      # To get median post word count
            ]
            df_posts = LoadPosts(
                bucket_name=self.bucket_name,
                folder_path=self.folder_posts,
                columns=l_cols_post_aggs_only,
            ).read_and_apply_transformations()

        info(f"  reading sub-level data & merging with aggregates...")
        df_subs = (
            self.read_and_apply_transformations()
            .merge(
                create_sub_level_aggregates(
                    df_posts,
                    col_manual_label=self.col_new_manual_topic,
                    col_subreddit_id='subreddit_id',
                ),
                how='outer',
                left_on=['subreddit_name'],
                right_index=True,
                suffixes=('', '_post')
            )
        )
        # fill missing data into single col & drop duplicate cols
        df_subs['subreddit_id'] = np.where(
            df_subs['subreddit_id'].isnull(),
            df_subs['subreddit_id_post'],
            df_subs['subreddit_id']
        )
        df_subs[self.col_new_manual_topic] = np.where(
            df_subs[self.col_new_manual_topic].isnull(),
            df_subs[f"{self.col_new_manual_topic}_post"],
            df_subs[self.col_new_manual_topic]
        )
        return df_subs.drop(['subreddit_id_post',
                             f"{self.col_new_manual_topic}_post"],
                            axis=1)


def create_sub_level_aggregates(
        df_posts: pd.DataFrame,
        col_sub_key: str = 'subreddit_name',
        col_language: str = 'weighted_language_top',
        col_post_type: str = 'post_type_agg3',
        col_word_count: str = 'text_word_count',
        col_total_posts: str = 'total_posts_count',
        col_manual_label: str = None,
        col_subreddit_id: str = None,
) -> pd.DataFrame:
    """Take a posts df and create some aggregate columns in a wide format
    so that we can merge this with a df_subs (each row = 1 sub).

    By default only returns percentages.
    """
    # use col manual label & sub ID to append in case we want to merge
    #  with sub metadata that's missing this info
    l_add_extra_cols = list()
    if col_manual_label is not None:
        l_add_extra_cols.append(col_manual_label)
    if col_subreddit_id is not None:
        l_add_extra_cols.append(col_subreddit_id)

    # create roll ups for "other languages"
    df_lang_sub = get_language_by_sub_wide(
        df_posts,
        col_sub_name=col_sub_key,
        col_lang_weighted=col_language,
        col_total_posts=col_total_posts,
    ).rename(
        columns={'de_percent': 'German_posts_percent',
                 'en_percent': 'English_posts_percent',
                 'other_percent': 'other_language_posts_percent',
                 }
    )
    df_lang_sub = df_lang_sub[[c for c in df_lang_sub.columns if c.endswith('_posts_percent')]]

    df_post_type_sub = get_language_by_sub_wide(
        df_posts,
        col_sub_name=col_sub_key,
        col_lang_weighted=col_post_type,
        col_total_posts=col_total_posts,
    ).rename(
        columns={'image_percent': 'image_post_type_percent',
                 'text_percent': 'text_post_type_percent',
                 'link_percent': 'link_post_type_percent',
                 'other_percent': 'other_post_type_percent',
                 }
    )
    df_post_type_sub = df_post_type_sub[[c for c in df_post_type_sub.columns if c.endswith('_post_type_percent')]]

    df_merged = (
        df_lang_sub
        .merge(
            df_post_type_sub,
            how='outer',
            left_index=True,
            right_index=True,
        )
        .merge(
            (
                df_posts
                .groupby(col_sub_key)
                .agg(
                    **{'post_median_word_count': (col_word_count, 'median')
                       }
                )
            ),
            left_index=True,
            right_index=True,
        )
    )
    if 0 == len(l_add_extra_cols):
        return df_merged
    else:
        return df_merged.merge(
            (
                df_posts[[col_sub_key] + l_add_extra_cols]
                .drop_duplicates()
                .set_index(col_sub_key)
            ),
            how='left',
            left_index=True,
            right_index=True,
        )


def create_new_manual_topic_column(
        df: pd.DataFrame,
        col_sub_name: str = 'subreddit_name',
        col_prev_label: str = 'combined_topic_and_rating',
        sub_names_to_new_label: Dict[str, str] = None,
        old_labels_to_new_label: Dict[str, str] = None,
) -> Union[pd.Series, np.ndarray]:
    """Create new column that starts with a previous label and tweaks it.

    First it applies label based on a subreddit name
    Then it tries to map previous names to a new name
    Finally it applies the old name if no overrides found.
    """
    new_place_culture = 'place/culture'
    new_cult_ent_music = 'culture, entertainment, music'

    nsfw_category = 'over18_nsfw'
    # looks like most of these might've been classified as NSFW AFTER I PULLED
    # THE TRAINING DATA
    l_nsfw_subs = [
        'wixbros', 'germannudes', 'loredana', 'katjakrasavicenudes',
        'deutschetributes', 'nicoledobrikovof', 'germanonlyfans',
        'elisaalinenudes', 'julesboringlifehot', 'marialoeffler',
        'annitheduck', 'emmyruss', 'bibisbeautypalacensfw',
        'juliabeautx_xxx',
    ]

    if old_labels_to_new_label is None:
        old_labels_to_new_label = {
            'food': 'food and drink',
            'culture + entertainment': new_cult_ent_music,
            'place': new_place_culture,
        }
    if sub_names_to_new_label is None:
        sub_names_to_new_label = {
            'de_iama': 'reddit institutions',
            'askswitzerland': 'reddit institutions',
            'fragreddit': 'reddit institutions',
            'askagerman': 'reddit institutions',

            'wasletztepreis': 'internet culture and memes',
            'einfach_posten': 'internet culture and memes',

            'de': new_place_culture,
            'switzerland': new_place_culture,
            'wien': new_place_culture,
            'zurich': new_place_culture,

            'germanrap': new_cult_ent_music,

            'fahrrad': 'sports',
        }
        for s_ in l_nsfw_subs:
            sub_names_to_new_label[s_] = nsfw_category

    return np.where(
        df[col_sub_name].isin(sub_names_to_new_label.keys()),
        df[col_sub_name].replace(sub_names_to_new_label),
        df[col_prev_label].replace(old_labels_to_new_label)
    )

#
# ~ fin
#
