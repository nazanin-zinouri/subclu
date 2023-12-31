"""
Utils to load data & apply common ETL/cleanup/aggregations.

Ideally, call these functions so that new columns have the same definitions
across different notebooks/experiments.
"""
import logging
from logging import info
from pathlib import Path
from typing import Dict, Union

from dask import dataframe as dd
import dask.array as da
from google.cloud import storage
import numpy as np
import pandas as pd
from tqdm import tqdm

from ..eda.aggregates import get_language_by_sub_wide
from ..utils.language_code_mapping import (
    L_CLD3_CODES_FOR_TOP_LANGUAGES_AND_USE_MULTILINGUAL,
    L_CLD3_CODES_FOR_LANGUAGES_IN_USE_MULTILINGUAL,
    L_USE_MULTILINGUAL_LANGUAGE_NAMES,
    D_CLD3_CODE_TO_LANGUAGE_NAME,
)
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
            folder_path: str = 'posts/de/2021-06-16',
            columns: iter = None,
            col_new_manual_topic: str = 'manual_topic_and_rating',
            col_unique_check: str = 'post_id',
            local_path_root: str = f"/home/jupyter/subreddit_clustering_i18n/data/local_cache/",
            df_format: str = 'pandas',
            read_fxn: Union[callable, str] = 'dask',
            unique_check: bool = True,
            verbose: bool = False,
    ):
        """
        Args:
            bucket_name:
            folder_path:
            columns:
            col_new_manual_topic:
            col_unique_check:
            local_path_root:
            df_format:
        """
        self.bucket_name = bucket_name
        self.folder_path = folder_path
        self.col_new_manual_topic = col_new_manual_topic
        self.col_unique_check = col_unique_check

        self.local_path_root = local_path_root
        self.df_format = df_format

        if read_fxn == 'dask':
            self.read_fxn = dd.read_parquet
        elif read_fxn == 'pandas':
            self.read_fxn = pd.read_parquet
        else:
            self.read_fxn = read_fxn

        if columns == 'aggregate_embeddings_':
            self.columns = [
                # IDs
                'subreddit_name',
                'subreddit_id',
                'post_id',

                # Meta
                'submit_date',
                # 'removed',
                'upvotes',
                # 'successful',
                # 'app_name',

                # 'combined_topic_and_rating',  # Deprecated since v0.4.1
                'post_type',  # For post aggs
                # 'post_nsfw',
                # 'geolocation_country_code',

                # Language & text content
                # 'post_url',
                # 'language',
                # 'probability',
                'weighted_language', # For language aggs
                # 'weighted_language_probability',
                'text_len',
                'text_word_count',
                # 'post_url_for_embeddings',
                # 'text'
            ]
        else:
            self.columns = columns

        # unique check only gets computed if the df_format is pandas
        #  otherwise it's super expensive on 10+ million rows
        self.unique_check = unique_check
        self.verbose = verbose

    def read_raw(self) -> pd.DataFrame:
        """Read raw files w/o any transformations"""
        info(f"Reading raw data...")
        self._local_cache()

        if self.verbose:
            info(f"  Read fxn: {self.read_fxn}")
            info(f"  df format: {self.df_format}")

        if (self.read_fxn == dd.read_parquet) & (self.df_format == 'pandas'):
            if self.verbose:
                info(f"  Reading with dask and converting to pandas...")
            df = self.read_fxn(
                # path=f"gs://{self.bucket_name}/{self.folder_path}",
                path=self.path_local_folder,
                columns=self.columns,
            ).compute()
        else:
            if self.verbose:
                info(f"  Reading without .compute(): {self.read_fxn}")
            df = self.read_fxn(
                # path=f"gs://{self.bucket_name}/{self.folder_path}",
                path=self.path_local_folder,
                columns=self.columns
            )

        if all([self.df_format == 'pandas', self.unique_check]):
            if self.verbose:
                info(f"  Checking ID uniqueness...")
            # Make this uniqueness check optional b/c it can take
            #   +10 seconds on 8 million posts
            assert len(df) == df[self.col_unique_check].nunique()

        return df

    def _local_cache(self) -> None:
        """Download the files locally to speed up load times & reduce bandwidth costs"""
        storage_client = storage.Client()

        # Extract bucket name & prefix from artifact URI
        # parsed_uri = artifact_uri.replace('gs://', '').split('/')
        # artifact_prefix = '/'.join(parsed_uri[1:])
        # full_artifact_folder = f"{artifact_prefix}/{artifact_folder}"

        self.path_local_folder = Path(f"{self.local_path_root}/{self.folder_path}")
        # need to check the parent folder only:
        artifact_folder = self.folder_path.split('/')[-1]
        info(f"  Local folder to download artifact(s):\n  {self.path_local_folder}")
        Path.mkdir(self.path_local_folder, exist_ok=True, parents=True)

        bucket = storage_client.get_bucket(self.bucket_name)
        l_files_to_download = list(bucket.list_blobs(prefix=self.folder_path))
        if self.verbose:
            info(f"  Bucket name: {self.bucket_name}")
            info(f"  Prefix to download: {self.folder_path}")
            info(f"  # of files to download: {len(l_files_to_download)}")
        for blob_ in tqdm(l_files_to_download, ascii=True, ncols=70, position=0, leave=True):
            # Skip files that aren't in the same folder as the expected (input) folder
            parent_folder = blob_.name.split('/')[-2]
            if artifact_folder != parent_folder:
                continue

            f_name = (
                    self.path_local_folder /
                    f"{blob_.name.split('/')[-1].strip()}"
            )
            if f_name.exists():
                pass
                # info(f"  {f_name.name} <- File already exists, not downloading")
            else:
                blob_.download_to_filename(f_name)

    def read_and_apply_transformations(self) -> pd.DataFrame:
        """Read & apply all transformations in a single call"""
        df = self.read_raw()

        info(f"  Applying transformations...")
        # plotly throws out errors if we try use a col with nulls in a plot
        if 'post_nsfw' in df.columns:
            df['post_nsfw'] = df['post_nsfw'].fillna('unlabeled')

        # Dask is hot garbage when assigning/creating new columns, so
        # convert dask to pandas before applying transformations
        l_cols_for_transforming = [
            'weighted_language',
            'post_type',
        ]
        if isinstance(df, dd.DataFrame):
            if any([c_ in df.columns for c_ in l_cols_for_transforming]):
                logging.warning(f"  CONVERTING dask df to pandas")
                df = df.compute()

        if self.col_new_manual_topic not in df.columns:
            # TODO(djb): this column isn't used anymore, maybe remove it in the future?
            try:
                df[self.col_new_manual_topic] = create_new_manual_topic_column(df)
            except (KeyError, TypeError) as e:
                logging.error(f"Error creating manual topic... {e}")

        # Start using the list of Languages in USE-multilingual to get a better idea of
        #  other languages, not just German
        # Need to convert language code to Language Name to prevent duplicates for
        #  languages w/ multiple codes e.g., (Chinese, Russian)
        if 'weighted_language' in df.columns:
            df['weighted_language_top'] = np.where(
                df['weighted_language'].isin(['UNKNOWN'] + L_CLD3_CODES_FOR_TOP_LANGUAGES_AND_USE_MULTILINGUAL),
                df['weighted_language'].replace(D_CLD3_CODE_TO_LANGUAGE_NAME),
                'Other_language'
            )
            df['weighted_language_in_use_multilingual'] = np.where(
                df['weighted_language'].isin(L_CLD3_CODES_FOR_LANGUAGES_IN_USE_MULTILINGUAL),
                True,
                False
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

        return df


class LoadSubreddits(LoadPosts):
    """Build on top of Load Posts to standardize loading subreddit metadata

    No need to over-ride LoadPost function until/unless we want to append
    post-level aggregates, but that's better handled as a separate function.
    """

    def __init__(
            self,
            bucket_name: str = 'i18n-subreddit-clustering',
            folder_path: str = 'subreddits/de/2021-06-16',
            folder_posts: str = 'posts/de/2021-06-16',
            columns: iter = None,
            col_new_manual_topic: str = 'manual_topic_and_rating',
            col_unique_check: str = 'subreddit_name',
            local_path_root: str = f"/home/jupyter/subreddit_clustering_i18n/data/local_cache/",
            df_format: str = 'pandas',
            read_fxn: callable = dd.read_parquet,
            unique_check: bool = True,
            verbose: bool = False,
    ) -> None:
        super().__init__(
            bucket_name=bucket_name,
            folder_path=folder_path,
            columns=columns,
            col_new_manual_topic=col_new_manual_topic,
            col_unique_check=col_unique_check,
            local_path_root=local_path_root,
            df_format=df_format,
            read_fxn=read_fxn,
            unique_check=unique_check,
            verbose=verbose,
        )
        self.folder_posts = folder_posts
        # TODO(djb)
        #  over-ride cols, subs are usually small enough that we
        #  don't have to worry about loading only some cols, but keep in mind for later
        if columns == 'aggregate_embeddings_':
            self.columns = [c for c in columns if c != 'combined_topic_and_rating']
        else:
            self.columns = columns

    def read_apply_transformations_and_merge_post_aggs(
            self,
            df_posts: pd.DataFrame = None,
            cols_post: Union[iter, str] = 'all_aggs_',
            df_format: str = 'dask',
            read_fxn: str = 'dask',
            unique_check: bool = False,
            **posts_kwargs,
    ) -> pd.DataFrame:
        """Besides loading the sub-data, load post-level data & merge aggregates from post-level"""
        if df_posts is None:
            info(f"Loading df_posts from: {self.folder_posts}")
            if cols_post == 'all_aggs_':
                # limit to only cols absolutely needed to save time & RAM
                cols_post_ = [
                    'subreddit_name',
                    'subreddit_id',
                    'post_id',
                    'weighted_language',    # For language aggs
                    'post_type',            # For post aggs
                    # 'combined_topic_and_rating',    # Needed for new manual label
                    'text_word_count',      # To get median post word count
                    'text_len',             # Get median text len
                ]
            elif cols_post == 'post_count_only_':
                cols_post_ = [
                    'subreddit_name',
                    'subreddit_id',
                    'post_id',
                ]
            else:
                cols_post_ = cols_post

            # The fastes method seems to be:
            #  do everything in dask until the last moment
            if all([df_format == 'dask', cols_post == 'post_count_only_']):
                df_posts = LoadPosts(
                    bucket_name=self.bucket_name,
                    folder_path=self.folder_posts,
                    columns=cols_post_,
                    df_format=df_format,
                    read_fxn=read_fxn,
                    unique_check=unique_check,
                    **posts_kwargs,
                ).read_and_apply_transformations().compute()
            else:
                # in case the output is already a pandas df
                df_posts = LoadPosts(
                    bucket_name=self.bucket_name,
                    folder_path=self.folder_posts,
                    columns=cols_post_,
                    df_format=df_format,
                    read_fxn=read_fxn,
                    unique_check=unique_check,
                    **posts_kwargs,
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

        # Remove duplicate cols & co_new_manual_topic
        if self.col_new_manual_topic in df_subs.columns:
            try:
                df_subs[self.col_new_manual_topic] = np.where(
                    df_subs[self.col_new_manual_topic].isnull(),
                    df_subs[f"{self.col_new_manual_topic}_post"],
                    df_subs[self.col_new_manual_topic]
                )
            except KeyError:
                pass
        l_dupe_cols = [
            'subreddit_id_post',
            f"{self.col_new_manual_topic}_post"
        ]
        for col_ in l_dupe_cols:
            try:
                df_subs = df_subs.drop(col_, axis=1)
            except KeyError:
                pass

        return df_subs


class LoadComments(LoadPosts):
    """Build on top of Load Posts to standardize loading subreddit metadata

    No need to over-ride LoadPost function until/unless we want to append
    post-level aggregates, but that's better handled as a separate function.
    """
    def __init__(
            self,
            bucket_name: str = 'i18n-subreddit-clustering',
            folder_path: str = 'comments/de/2021-06-16',
            folder_posts: str = 'posts/de/2021-06-16',
            columns: iter = None,
            col_new_manual_topic: str = 'manual_topic_and_rating',
            col_unique_check: str = 'comment_id',
            local_path_root: str = f"/home/jupyter/subreddit_clustering_i18n/data/local_cache/",
            df_format: str = 'pandas',
            read_fxn: callable = dd.read_parquet,
            unique_check: bool = True,
            verbose: bool = False,
    ) -> None:
        super().__init__(
            bucket_name=bucket_name,
            folder_path=folder_path,
            columns=columns,
            col_new_manual_topic=col_new_manual_topic,
            col_unique_check=col_unique_check,
            local_path_root=local_path_root,
            df_format=df_format,
            read_fxn=read_fxn,
            unique_check=unique_check,
            verbose=verbose,
        )
        self.folder_posts = folder_posts

        #  over-ride cols b/c post cols will be different than comments or subs
        if columns == 'aggregate_embeddings_':
            self.columns = [
                # IDs
                'subreddit_name',
                'subreddit_id',
                'post_id',
                'comment_id',
                # 'user_id',

                # Comment & user meta
                # 'thing_type',
                'submit_date',
                # 'removed',
                'upvotes',
                # 'successful',
                # 'app_name',
                # 'post_type',
                # 'post_nsfw',
                # 'geolocation_country_code',
                # 'subreddit_geo_country_code',
                # 'combined_topic',
                # 'combined_topic_and_rating',
                # 'rating',
                # 'rating_version',

                # Text & language meta
                # 'language',
                # 'probability',
                # 'weighted_language',
                # 'weighted_language_probability',
                # TODO(djb) add it back to SQL query... not sure why it's missing from top/US query
                'comment_text_len',
                'comment_text_word_count',
                # 'comment_body_text',
            ]
        else:
            self.columns = columns

    # TODO(djb) is there a way to quickly calculate and store text len?
    #  I don't want to have to calculate it each time it's needed
    # def read_raw(self) -> pd.DataFrame:
    #     """Over-ride default read and calculate text len column if it's missing"""
    #     df = super().read_raw()
    #     if 'comment_text_len' not in df.columns:


def create_sub_level_aggregates(
        df_posts: pd.DataFrame,
        col_sub_key: str = 'subreddit_name',
        col_language: str = 'weighted_language_top',
        col_post_type: str = 'post_type',
        col_word_count: str = 'text_word_count',
        col_total_posts: str = 'total_posts_count',
        col_text_len: str = 'text_len',
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
    if col_manual_label in df_posts.columns:
        l_add_extra_cols.append(col_manual_label)
    if col_subreddit_id in df_posts.columns:
        l_add_extra_cols.append(col_subreddit_id)

    # =====================
    # Posts: Detected language percentages + Primary post-language
    # ===
    # We assume that language codes have been converted to language names already
    if col_language in df_posts.columns:
        df_lang_sub = get_language_by_sub_wide(
            df_posts,
            col_sub_name=col_sub_key,
            col_lang_weighted=col_language,
            col_total_posts=col_total_posts,
        )
        # Only keep the percent cols, not the counts
        df_lang_sub = df_lang_sub.rename(
            columns={c: c.replace('_percent', '_posts_percent') for c in df_lang_sub.columns}
        )
        l_cols_language_percent = [c for c in df_lang_sub.columns if c.endswith('_posts_percent')]
        df_lang_sub = df_lang_sub[l_cols_language_percent]

        # Create new column for predominant language name
        df_lang_sub['primary_post_language'] = (
            df_lang_sub[l_cols_language_percent].idxmax(axis=1).str.replace('_posts_percent', '')
        )
        # Create new col for predominant language percent
        df_lang_sub['primary_post_language_percent'] = (
            df_lang_sub[l_cols_language_percent].max(axis=1)
        )
        # Append column with whether predominant language is covered by use-multilingual
        df_lang_sub['primary_post_language_in_use_multilingual'] = np.where(
            df_lang_sub['primary_post_language'].isin(L_USE_MULTILINGUAL_LANGUAGE_NAMES),
            True,
            False
        )

        df_lang_sub = df_lang_sub.merge(
            get_subreddit_secondary_language(
                df_subs=df_lang_sub.reset_index(),
                l_language_pct_cols=l_cols_language_percent,
                col_index='subreddit_name',
                col_2nd_language='secondary_post_language',
                col_2nd_lang_pct='secondary_post_language_percent',
                col_language_rank='language_rank',
                min_pct_limit=0.008,
            ),
            how='left',
            left_index=True,
            right_index=True,
        )

    # =====================
    # Posts: percentages of post-type + Primary post-type
    # ===
    if col_post_type in df_posts.columns:
        df_post_type_sub = get_language_by_sub_wide(
            df_posts,
            col_sub_name=col_sub_key,
            col_lang_weighted=col_post_type,
            col_total_posts=col_total_posts,
        )
        # Only keep the percent cols, not the counts
        suffix_post_type_pct = '_post_type_percent'
        df_post_type_sub = df_post_type_sub.rename(
            columns={c: c.replace('_percent', suffix_post_type_pct) for c in df_post_type_sub.columns}
        )
        l_cols_post_type_percent = [c for c in df_post_type_sub.columns if c.endswith(suffix_post_type_pct)]
        df_post_type_sub = df_post_type_sub[l_cols_post_type_percent]

        # Create new column for predominant post_type
        #  this way it'll be easier to compare subreddits by post type w/o
        #  having to check 3 or more columns!
        df_post_type_sub['primary_post_type'] = (
            df_post_type_sub[l_cols_post_type_percent].idxmax(axis=1).str.replace(suffix_post_type_pct, '')
        )
        # Create new col for predominant language percent
        df_post_type_sub['primary_post_type_percent'] = (
            df_post_type_sub[l_cols_post_type_percent].max(axis=1)
        )

    d_aggregates = {
        'posts_for_modeling_count': ('post_id', 'nunique'),
    }
    if col_word_count in df_posts.columns:
        d_aggregates['post_median_word_count'] = (col_word_count, 'median')
    if col_text_len in df_posts.columns:
        d_aggregates['post_median_text_len'] = (col_text_len, 'median')

    # Merge the aggregated posts
    if all([
        col_language in df_posts.columns,
        col_post_type in df_posts.columns,
    ]):
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
                    .agg(**d_aggregates)
                ),
                how='outer',
                left_index=True,
                right_index=True,
            )
        )
    elif (col_language in df_posts.columns) & (col_post_type not in df_posts.columns):
        df_merged = (
            df_lang_sub
            .merge(
                (
                    df_posts
                    .groupby(col_sub_key)
                    .agg(**d_aggregates)
                ),
                how='outer',
                left_index=True,
                right_index=True,
            )
        )
    elif (col_language not in df_posts.columns) & (col_post_type in df_posts.columns):
        df_merged = (
            df_post_type_sub
            .merge(
                (
                    df_posts
                    .groupby(col_sub_key)
                    .agg(**d_aggregates)
                ),
                how='outer',
                left_index=True,
                right_index=True,
            )
        )
    else:
        df_merged = (
            df_posts
            .groupby(col_sub_key)
            .agg(**d_aggregates)
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


def get_subreddit_secondary_language(
        df_subs: pd.DataFrame,
        l_language_pct_cols: iter,
        col_index: str = 'subreddit_name',
        col_2nd_language: str = 'secondary_post_language',
        col_2nd_lang_pct: str = 'secondary_post_language_percent',
        col_language_rank: str = 'language_rank',
        min_pct_limit: float = 0.008,
) -> pd.DataFrame:
    """Function to get secondary language - this will be helpful for i18n subs that
    are multilingual
    """
    if isinstance(col_index, str):
        col_index = [col_index]

    return (
        df_subs
        [col_index + l_language_pct_cols]
        .set_index(col_index)

        # When we stack, we take all the language percent columns (wide) and make it long (one row per language)
        .stack()
        .reset_index()
        # We then need to rename the new columns after stacking
        .rename(columns={'level_1': col_2nd_language,
                         0: col_2nd_lang_pct,
                         })
        # We need to clean the column names back into language names
        .assign(
            **{col_2nd_language: lambda x: x[col_2nd_language].str.replace('_posts_percent', '')}
        )

        # Filter out languages that are not meaningful because they're not common or b/c
        #  the subreddit has no posts of that language
        .query(
            f"{col_2nd_language} != 'Other_language' & {col_2nd_language} != 'UNKNOWN' & {col_2nd_lang_pct} > 0.0001"
        )
        # Rank the remaining languages
        .assign(
            **{col_language_rank: lambda x: x.groupby(col_index)[col_2nd_lang_pct].rank(method='first',
                                                                                        ascending=False),
               },
        )
        # Keep only languages that are rank=2 and meet the minimum percent
        .query(f"{col_language_rank} == 2 & {col_2nd_lang_pct} > {min_pct_limit}")
        .sort_values(by=['subreddit_name', 'language_rank'], ascending=[True, True])
        .set_index(col_index)
        .drop('language_rank', axis=1)
    )


#
# ~ fin
#
