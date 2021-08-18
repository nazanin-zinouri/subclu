"""
Class to load & clean up counterpart spreadsheet so we can programmatically find
counterparts using the subreddit distance and/or clusters.
"""
from logging import info
from pathlib import Path
# from typing import Dict, Union

from dask import dataframe as dd
from google.cloud import storage
# import numpy as np
import pandas as pd
from tqdm.auto import tqdm


class LoadCounterpartSeeds:
    """
    Class to load posts data and apply some standard transformations.
    Currently defaults to loading from GCS buckets and that the files are CSV.

    We could extend it to query from Google sheets if needed when we get a service account
    that gets access to it.
    """

    def __init__(
            self,
            bucket_name: str = 'i18n-subreddit-clustering',
            folder_path: str = 'data/subreddit_counterparts',
            file_name: str = 'Criteria for default subs - [OLD]default subs-2021-08-13_15-17.csv',
            columns: iter = None,
            local_path_root: str = f"/home/jupyter/subreddit_clustering_i18n/data/local_cache/",
            df_format: str = 'pandas_csv',
            skiprows: int = 3,
    ):
        self.bucket_name = bucket_name
        self.folder_path = folder_path
        self.file_name = file_name
        self.skiprows = skiprows

        self.local_path_root = local_path_root
        self.df_format = df_format
        if df_format == 'pandas_csv':
            self.read_fxn = pd.read_csv
        elif df_format == 'pandas_parquet':
            self.read_fxn = pd.read_parquet
        elif df_format == 'dask_parquet':
            self.read_fxn = dd.read_parquet
        else:
            raise NotImplementedError(f"Format not implemented:  {df_format}")

        if columns == 'core_columns':
            self.columns = [
                'subreddit_name',
                'counterpart_priority',
                'topic_seed',
                'pod',
                'german',
                'ambassador_sub',
                'type_of_content_seed',

                # manual counterparts
                'counterpart_1',
                'counterpart_2',
            ]
        else:
            self.columns = columns

    def read_raw(self) -> pd.DataFrame:
        """Read raw files w/o any transformations"""
        self._local_cache()

        if self.file_name is None:
            f_to_load = self.path_local_folder
        else:
            f_to_load = f"{self.path_local_folder}/{self.file_name}"

        try:
            # first try parquet format
            df = self.read_fxn(
                # path=f"gs://{self.bucket_name}/{self.folder_path}",
                path=f_to_load,
                columns=self.columns
            )
        except TypeError:
            try:
                # then try CSV format
                df = self.read_fxn(
                    f_to_load,
                    usecols=self.columns,
                    skiprows=self.skiprows,
                )
            except ValueError:
                # Still CSV format, but ignore columns if values are missing
                df = self.read_fxn(
                    f_to_load,
                    usecols=None,
                    skiprows=self.skiprows,
                )
        except KeyError:
            # Ignore columns if values are missing
            df = self.read_fxn(
                # path=f"gs://{self.bucket_name}/{self.folder_path}",
                f_to_load,
                columns=None
            )


        return df

    def _local_cache(self) -> None:
        """Download the files locally to speed up load times & reduce bandwidth costs"""
        storage_client = storage.Client()

        # Extract bucket name & prefix from artifact URI
        self.path_local_folder = Path(f"{self.local_path_root}/{self.folder_path}")
        # need to check the parent folder only:
        artifact_folder = self.folder_path.split('/')[-1]
        info(f"Local folder to download artifact(s):\n  {self.path_local_folder}")
        Path.mkdir(self.path_local_folder, exist_ok=True, parents=True)

        bucket = storage_client.get_bucket(self.bucket_name)
        if self.file_name is None:
            l_files_to_download = list(bucket.list_blobs(prefix=self.folder_path))
        else:
            l_files_to_download = list(bucket.list_blobs(prefix=f"{self.folder_path}/{self.file_name}"))

        for blob_ in tqdm(l_files_to_download):
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

    def read_and_transform(self) -> pd.DataFrame:
        """Read & apply all transformations in a single call"""
        info(f"Reading raw data...")
        df = self.read_raw()

        info(f"  Applying transformations...")
        df = (
            df
            .rename(columns={'sub names': 'subreddit_name',
                             'prioirity?': 'counterpart_priority',
                             'Type of content': 'type_of_content_seed',
                             'topic': 'topic_seed',
                             }
                    )
            .rename(columns={c: c.lower().replace(' ', '_').replace('?', '') for c in df.columns})
        )
        if self.columns is not None:
            df = df[self.columns]

        # clean up/standardize column values
        d_fix_subreddit_names = {
            'de_events (r/berlin?? is it too location? pokemongoDE?)': 'de_events',
            'Hundeschule - should this be aww-related?': 'hundeschule',
            'fotografie (is this the right name?)': 'fotografie'
        }
        df['subreddit_name'] = df['subreddit_name'].replace(d_fix_subreddit_names)

        for c_ in df.select_dtypes('object').columns:
            df[c_] = df[c_].str.lower().str.strip()

        d_rename_type_of_content = {
            'text_and_links': 'links and text',
            "text, links": 'links and text',
            'everything': 'all',
            'pics': 'photos',
            'test': 'text',
        }
        df['type_of_content_seed'] = df['type_of_content_seed'].replace(d_rename_type_of_content)

        return df

    def read_and_reshape_to_rows(self):
        """use .stack() to get the counterparts as rows instead of columns"""
        df = self.read_and_transform()
        l_ix_seeds_stack = ['subreddit_name', 'counterpart_1', 'counterpart_2']

        df_seeds_reshape = (
            df[l_ix_seeds_stack]
            .set_index('subreddit_name')
            .stack()
            .to_frame()
            .reset_index()
            .rename(columns={'level_1': 'manual_counterpart', 0: 'counterpart_name'})
        ).merge(
            df[['subreddit_name'] + list(df.drop(l_ix_seeds_stack, axis=1))],
            how='left',
            on='subreddit_name',
        )

        return df_seeds_reshape


#
# ~ fin
#
