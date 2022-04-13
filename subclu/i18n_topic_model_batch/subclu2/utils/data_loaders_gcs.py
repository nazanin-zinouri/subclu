"""
Utils to load text & metadata for post, comments, & subreddits from parquet as a dataframe
"""
from datetime import datetime
from concurrent import futures
import logging
from logging import info
from pathlib import Path

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
            read_fxn: str = 'dask',
            n_sample_files: int = None,
            n_files_slice_start: int = None,
            n_files_slice_end: int = None,
            unique_check: bool = True,
            verbose: bool = False,
    ):
        """"""
        self.bucket_name = bucket_name
        self.gcs_path = gcs_path
        self.columns = columns
        self.col_unique_check = col_unique_check

        if local_cache_path == 'local_vm':
            self.local_path_root = f"/home/jupyter/subreddit_clustering_i18n/data/local_cache/"
        else:
            self.local_path_root = local_cache_path
        self.df_format = df_format

        # NOTE: unique check only gets computed if the df_format is `pandas`
        #  otherwise it's super expensive on 10+ million rows in `dask`
        self.unique_check = unique_check
        self.verbose = verbose

        self.n_sample_files = n_sample_files
        self.n_files_slice_start = n_files_slice_start
        self.n_files_slice_end = n_files_slice_end

        if read_fxn == 'dask':
            self.read_fxn = dd.read_parquet
        elif read_fxn == 'pandas':
            self.read_fxn = pd.read_parquet
        else:
            self.read_fxn = read_fxn


def download_blob(
        blob: Blob,
        local_file_name: str,
) -> None:
    blob.download_to_filename(local_file_name)


def download_files_in_parallel(
        bucket_name: str,
        gcs_folder_path: str,
        local_path_root: str,
) -> dict:
    t_start_ = datetime.utcnow()
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # Set paths
    path_local_folder = Path(f"{local_path_root}/{bucket_name}/{gcs_folder_path}")
    artifact_folder = gcs_folder_path.split('/')[-1]
    info(f"  Local folder to download artifact(s):\n  {path_local_folder}")
    Path.mkdir(path_local_folder, exist_ok=True, parents=True)
    d_output: {
        'path_local_folder': path_local_folder
    }

    pool = futures.ThreadPoolExecutor()
    downloads = []

    l_files_to_check = list(bucket.list_blobs(prefix=gcs_folder_path))
    l_files_downloaded = []

    for blob_ in l_files_to_check:
        # Skip files that aren't in the same folder as the expected (input) folder
        parent_folder = blob_.name.split('/')[-2]
        if artifact_folder != parent_folder:
            continue

        f_name = (
                path_local_folder /
                f"{blob_.name.split('/')[-1].strip()}"
        )
        l_files_downloaded.append(f_name)
        if f_name.exists():
            pass
            # info(f"  {f_name.name} <- File already exists, not downloading")
        else:
            download = pool.submit(blob_, f_name)
            downloads.append(download)

    futures.wait(downloads)
    d_output['files_downloaded'] = l_files_downloaded

    t_total_ = datetime.utcnow() - t_start_
    logger.info(
        "Downloading files took %.2f minutes (%.2f seconds) ---", t_total_ / 60, t_total_
    )

    return d_output
