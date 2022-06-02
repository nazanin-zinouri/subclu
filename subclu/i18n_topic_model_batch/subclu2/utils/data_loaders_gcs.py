"""
Utils to load text & metadata for post, comments, & subreddits from parquet as a dataframe
"""
from datetime import datetime
from concurrent import futures
import logging
from logging import info
from pathlib import Path
from typing import Union, Iterator, Tuple

from google.cloud import storage
# from google.cloud.storage.bucket import Bucket
from google.cloud.storage.blob import Blob

from dask import dataframe as dd
import pandas as pd


logger = logging.getLogger(__name__)


class LoadSubredditsGCS:
    """
    Class to load subreddits from GCS parquet files. Makes it easy to load all or only some
    parquet files into memory or as a generator (one file/df at a time).

    We use one file/df at a time to get embeddings (we're GPU/memory constrained so we can't
    run inference on all files at once).
    """
    def __init__(
            self,
            bucket_name: str,
            gcs_path: str,
            local_cache_path: str,
            columns: iter = None,
            col_unique_check: str = 'subreddit_id',
            df_format: str = 'pandas',
            n_sample_files: int = None,
            n_files_slice_start: int = None,
            n_files_slice_end: int = None,
            unique_check: bool = True,
            verbose: bool = False,
    ):
        """"""
        self.bucket_name = bucket_name
        self.gcs_path = gcs_path
        self.columns = [str(_) for _ in columns]
        self.col_unique_check = col_unique_check
        self.df_format = df_format

        if local_cache_path == 'local_vm':
            self.local_path_root = f"/home/jupyter/subreddit_clustering_i18n/data/local_cache/"
        else:
            self.local_path_root = local_cache_path

        # Here we'll cache files locally
        self.path_local_folder = None
        self.local_files_ = None
        self.local_parquet_files_ = None
        self.n_local_parquet_files_ = None

        # NOTE: unique check only gets computed if the df_format is `pandas`
        #  otherwise it's super expensive on 10+ million rows in `dask`
        self.unique_check = unique_check
        self.verbose = verbose

        self.n_sample_files = n_sample_files
        self.n_files_slice_start = n_files_slice_start
        self.n_files_slice_end = n_files_slice_end

    def read_as_one_df(self) -> Union[dd.DataFrame, pd.DataFrame]:
        self.local_cache()

        if self.verbose:
            info(f"  df format: {self.df_format}")

        if (1 == self.n_local_parquet_files_) & ('pandas' == self.df_format):
            df = pd.read_parquet(
                self.local_parquet_files_[0],
                columns=self.columns,
            )
        else:
            df = dd.read_parquet(
                self.local_parquet_files_,
                columns=self.columns,
            )

            if 'pandas' == self.df_format:
                df = df.compute()

        if all([self.df_format == 'pandas', self.unique_check]):
            if self.verbose:
                info(f"  Checking ID uniqueness...")
            #  Uniqueness check optional b/c it can take
            #   +10 seconds on 8 million posts
            unique_check_ = (len(df) == df[self.col_unique_check].nunique())
            assert unique_check_, f"Unique column check failed: {self.col_unique_check}"

        return df

    def yield_files_and_dfs(self) -> Iterator[Tuple[Path, pd.DataFrame]]:
        """
        For tqdm to be useful you'd need to use `total` argument:
          tqdm(generator, total=self.n_local_parquet_files_):
        """
        self.local_cache()

        for f_ in self.local_parquet_files_:
            yield f_, pd.read_parquet(
                f_,
                columns=self.columns
            )

    def local_cache(self) -> None:
        """Cache files to read from GCS to local"""
        if self.path_local_folder is not None:
            if self.verbose:
                info(f"  Files already downloaded.")
        else:
            d_local_cache = download_files_in_parallel(
                bucket_name=self.bucket_name,
                gcs_folder_path=self.gcs_path,
                local_path_root=self.local_path_root,
                n_sample_files=self.n_sample_files,
                n_files_slice_start=self.n_files_slice_start,
                n_files_slice_end=self.n_files_slice_end,
                verbose=self.verbose,
            )

            # get names for the individual parquet files to read
            self.path_local_folder = d_local_cache['path_local_folder']
            self.local_files_ = d_local_cache['files_downloaded']
            self.local_parquet_files_ = d_local_cache['parquet_files_downloaded']
            self.n_local_parquet_files_ = len(self.local_parquet_files_)


def download_blob(
        blob: Blob,
        local_file_name: str,
) -> None:
    """Use this helper function to download multiple files in parallel"""
    blob.download_to_filename(local_file_name)


def download_files_in_parallel(
        bucket_name: str,
        gcs_folder_path: str,
        local_path_root: str,
        n_sample_files: int = None,
        n_files_slice_start: int = None,
        n_files_slice_end: int = None,
        verbose: bool = False,
) -> dict:
    t_start_ = datetime.utcnow()
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # Set paths
    path_local_folder = Path(f"{local_path_root}/{bucket_name}/{gcs_folder_path}")
    artifact_folder = gcs_folder_path.split('/')[-1]
    info(f"  Local folder to download artifact(s):\n  {path_local_folder}")
    Path.mkdir(path_local_folder, exist_ok=True, parents=True)
    d_output = {
        'path_local_folder': path_local_folder
    }

    l_files_downloaded = []
    l_parquet_files_downloaded = []

    l_files_to_check = list(bucket.list_blobs(prefix=gcs_folder_path))[:n_sample_files]
    info(f"  {len(l_files_to_check)} <- Files matching prefix")
    if any([(_ is not None) for _ in [n_files_slice_start, n_files_slice_end]]):
        # make new copy of blobs to process so that when slicing the time estimates from tqdm
        #  are useful/accurate + we only download exactly what's needed
        l_files_to_check = (
            l_files_to_check[n_files_slice_start:n_files_slice_end]
        )

    info(f"  {len(l_files_to_check)} <- Files to check")
    n_cached_files = 0

    downloads = []
    with futures.ThreadPoolExecutor(max_workers=4) as executor:
        for blob_ in l_files_to_check:
            # Skip files that aren't in the same folder as the expected immediate folder
            parent_folder = blob_.name.split('/')[-2]
            if artifact_folder != parent_folder:
                continue

            f_name = (
                    path_local_folder /
                    f"{blob_.name.split('/')[-1].strip()}"
            )
            l_files_downloaded.append(f_name)
            if '.parquet' == f_name.suffix:
                l_parquet_files_downloaded.append(f_name)
            if f_name.exists():
                n_cached_files += 1
                if verbose:
                    info(f"    {f_name.name} <- File already exists, not downloading")
            else:
                downloads.append(
                    executor.submit(download_blob, blob_, f_name)
                )

    futures.wait(downloads)
    d_output['files_downloaded'] = l_files_downloaded
    d_output['parquet_files_downloaded'] = l_parquet_files_downloaded

    t_total_ = datetime.utcnow() - t_start_
    info(f"  Files already cached: {n_cached_files}")
    logger.info(f"{t_total_}  <- Downloading files elapsed time")

    return d_output
