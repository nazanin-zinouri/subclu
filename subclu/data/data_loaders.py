"""
Utils to load data & apply common ETL/cleanup/aggregations.

Ideally, call these functions so that new columns have the same definitions
across different notebooks/experiments.
"""
from logging import info
from typing import Dict, Union

import numpy as np
import pandas as pd

from subclu.eda.aggregates import (
    compare_raw_v_weighted_language,
    get_language_by_sub_wide,
    get_language_by_sub_long,
)


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
            folder_path: str = 'posts/2021-05-19',
            columns: iter = None,
            col_new_manual_topic: str = 'manual_topic_and_rating'
    ) -> None:
        super().__init__(bucket_name, folder_path, columns, col_new_manual_topic)


def create_sub_level_aggregates(
        df_posts: pd.DataFrame,
        col_sub_key: str = 'subreddit_name',
        col_language: str = 'weighted_language_top',
        col_post_type: str = 'post_type_agg3',
        col_total_posts: str = 'total_posts_count',
) -> pd.DataFrame:
    """Take a posts df and create some aggregate columns in a wide format
    so that we can merge this with a df_subs (each row = 1 sub).

    By default only returns percentages.
    """
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
    return df_lang_sub.merge(
        df_post_type_sub,
        how='outer',
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

    return np.where(
        df[col_sub_name].isin(sub_names_to_new_label.keys()),
        df[col_sub_name].replace(sub_names_to_new_label),
        df[col_prev_label].replace(old_labels_to_new_label)
    )

#
# ~ fin
#
