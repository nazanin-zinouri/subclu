"""
Utils to set up base mlflow setup & config
Currently everything is local, but at some point we might switch to a server

TODO(djb): add method in mlflowLogger class to upload sqlite file to central GCS
bucket so that I can merge all experiments/runs together even when they're run in any
arbitrary VM.

TODO(djb): create init experiments method to try to make sure that all VMs have the same
experiment names/IDs

TODO(djb): Merge sqlite DBs from multiple VMs:
SQLite Studio might be worth trying (as long as I don't have dozens of dbs to merge)
- https://stackoverflow.com/questions/80801/how-can-i-merge-many-sqlite-databases
- https://sqlitestudio.pl/
"""
import json
import logging
import os
from logging import info
import socket

import joblib
import pandas as pd
from pathlib import Path
import subprocess
from typing import List, Union

from google.cloud import storage
from dask import dataframe as dd
import mlflow
from mlflow.utils import mlflow_tags
from mlflow.exceptions import MlflowException
from tqdm import tqdm


class MlflowLogger:
    """
    This class is a workaround for using mlflow WITHOUT a server.

    When storing artifacts from multiple VMs, I thought about creating a subfolder
    for each VM (e.g., the vm name) so that we prevent naming conflicts. But
    I might go with central mlruns location + central experiments list to keep all
    VMs writing in same experiment names & IDs... might need to merge runs from
    some DBs later, though *SIGH*
    """
    def __init__(
            self,
            tracking_uri: str = 'sqlite',
            default_artifact_root: str = 'gs://i18n-subreddit-clustering/mlflow/mlruns',
    ):
        self.default_artifact_root = default_artifact_root
        self.host_name = socket.gethostname()

        if tracking_uri in [None, 'sqlite']:
            # TODO(djb): update path to config file?
            path_mlruns_db = Path(f"/home/jupyter/subreddit_clustering_i18n/mlflow_sync/{self.host_name}")
            Path.mkdir(path_mlruns_db, exist_ok=True, parents=True)
            tracking_uri = f"sqlite:///{path_mlruns_db}/mlruns.db"
            mlflow.set_tracking_uri(tracking_uri)
        else:
            mlflow.set_tracking_uri(tracking_uri)

        self.tracking_uri = tracking_uri

        # Reset logging to `warn` because sometimes in notebooks this
        #  gets changed to `info` and overwhelms all other output
        self.reset_sqlalchemy_logging()
        self.initialize_experiment_names()

    def initialize_experiment_names(self):
        """Set global experiment names to make it easy to merge
        runs from multiple SQLite files/databases (created by separate VMs).

        EXPERIMENT NAMES NEED TO BE UNIQUE.
        """
        l_experiments = [
            'Default',
            'fse_v1',
            'fse_vectorize_v1',
            'subreddit_description_v1',
            'fse_vectorize_v1.1',

            # For GPU VMs:
            'use_multilingual_v0.1_test',
            'use_multilingual_v1',
            'use_multilingual_v1_aggregates_test',
            'use_multilingual_v1_aggregates',

            # new experiments for v0.3.2 - use VM with GPU that isn't broken for inference!
            'v0.3.2_use_multi_inference_test',
            'v0.3.2_use_multi_inference',
            'v0.3.2_use_multi_aggregates_test',
            'v0.3.2_use_multi_aggregates',

            # new experiments for v0.4.0 - use VM with GPU that isn't broken for inference!
            'v0.4.0_use_multi_inference_test',
            'v0.4.0_use_multi_inference',
            'v0.4.0_use_multi_aggregates_test',
            'v0.4.0_use_multi_aggregates',

        ]
        for i, exp in enumerate(l_experiments):
            try:
                mlflow.create_experiment(
                    exp,
                    artifact_location=f"{self.default_artifact_root}/{i}",
                )
            except MlflowException:
                pass

    def create_experiment(
            self,
            name: str,
            artifact_location: str = None,
    ) -> str:
        """Wrapper around mlflow.create_experiment()
        This one uses the `default_artifact_root` set at the class-init to set
        the experiment location by auto-incrementing based on latest ACTIVE experiment.

        There could be weird results if we delete an experiment with runs & artifacts and
         then we create a new experiment. It's possible that both experiments might share the
         same artifact folder: mlruns/3, but the UUIDs for all runs should still be unique.
        """
        if artifact_location is not None:
            artifact_location = artifact_location
        else:
            artifact_location = (
                f"{self.default_artifact_root}/"
                f"{1 + self.get_max_experiment_id()}"
            )

        return mlflow.create_experiment(
            name,
            artifact_location=artifact_location
        )

    def set_experiment(
            self,
            name: str,
            artifact_location: str = None,
    ) -> None:
        """Wrapper around mlflow.create_experiment()/set_experiment
        This one uses the `default_artifact_root` set at the class-init to set
        the experiment location by auto-incrementing based on latest ACTIVE experiment.

        There could be weird results if we delete an experiment with runs & artifacts and
         then we create a new experiment. It's possible that both experiments might share the
         same artifact location, but the UUIDs for all runs should still be unique.
        """
        try:
            self.create_experiment(name=name, artifact_location=artifact_location)
        except MlflowException:
            pass

        return mlflow.set_experiment(name)

    @staticmethod
    def add_git_hash_to_active_run() -> None:
        """
        Check whether mlflow has set a tag for git commit,
        if it doesn't, set it.

        Returns: None
        """
        active_run = mlflow.active_run()
        git_commit = active_run.data.tags.get(mlflow_tags.MLFLOW_GIT_COMMIT)
        if git_commit is None:
            mlflow.set_tag('mlflow.source.git.commit', get_git_hash())

    def set_tag_hostname(self, key: str = 'host_name') -> str:
        """Add host_name as tag so it's easier to track which VM produced model"""
        if mlflow.active_run() is not None:
            mlflow.set_tag(key, self.host_name)
        return self.host_name

    def log_param_hostname(
            self,
            key: str = 'host_name',
            send_to_info: bool = True,
    ) -> str:
        """Add host_name as tag so it's easier to track which VM produced model"""
        if mlflow.active_run() is not None:
            mlflow.log_param(key, self.host_name)

        if send_to_info:
            info(f"{key}: {self.host_name}")
        return self.host_name

    @staticmethod
    def log_cpu_count(
            send_to_info: bool = True,
            param: bool = True,
            metric: bool = True,
    ) -> Union[int, None]:
        """Get the system's CPU & RAM, log it to mlflow and return dict"""
        try:
            cpu_count = os.cpu_count()
            metric_name = 'cpu_count'

            if send_to_info:
                info(f"{metric_name}: {cpu_count}")

            if mlflow.active_run() is not None:
                if param:
                    mlflow.log_param(metric_name, cpu_count)
                if metric:
                    mlflow.log_metric(metric_name, cpu_count)

            return cpu_count
        except Exception as e:
            logging.error(f"Error logging CPU info\n {e}")
            return None

    @staticmethod
    def log_ram_stats(
            send_to_info: bool = True,
            param: bool = True,
            metric: bool = True,
            only_memory_used: bool = False,
    ) -> Union[dict, None]:
        """Log total, active, & used RAM
        Could be helpful when debugging to identify jobs limited by RAM
        """
        try:
            memory_total, memory_used, memory_free = map(
                int, os.popen('free -t -m').readlines()[-1].split()[1:]
            )
            memory_used_percent = memory_used / memory_total
            d_ram = {
                'memory_total': memory_total,
                'memory_used_percent': memory_used_percent,
                'memory_used': memory_used,
                'memory_free': memory_free,
            }

            if only_memory_used:
                d_ram = {k: v for k, v in d_ram.items() if '_used' in k}

            if send_to_info:
                d_ram_pretty_format = {
                    **{k: f"{v:,.2%}" for k, v in d_ram.items() if '_percent' in k},
                    **{k: f"{v:,.0f}" for k, v in d_ram.items() if '_percent' not in k},
                }
                info(f"RAM stats:\n{d_ram_pretty_format}")

            if mlflow.active_run() is not None:
                if param:
                    mlflow.log_params(d_ram)
                if metric:
                    mlflow.log_metrics(d_ram)
            return d_ram

        except Exception as e:
            logging.error(f"Error logging CPU & RAM info\n {e}")
            return None



    @staticmethod
    def reset_sqlalchemy_logging() -> None:
        """
        For some reason my function to set logging info in notebooks can reset
        sqlalchemy and other libraries to "INFO", which can add a lot of noise.

        Returns: None
        """
        logging.getLogger('sqlalchemy').setLevel(logging.WARN)
        logging.getLogger('alembic').setLevel(logging.WARN)

    def list_experiment_meta(self,
                             output_format=None
                             ) -> Union[List[dict], pd.DataFrame]:
        """Get experiment meta as list of dictionaries"""
        mlflow_client = mlflow.tracking.MlflowClient()
        # The first time we call this function, we may see a lot of 'info' logs
        self.reset_sqlalchemy_logging()
        l_exp = list()

        for exp_ in mlflow_client.list_experiments():
            l_exp.append(
                json.loads(
                    mlflow.utils.proto_json_utils.message_to_json(exp_.to_proto())
                )
            )
        if output_format == 'pandas':
            return pd.DataFrame(l_exp)
        else:
            return l_exp

    def get_max_experiment_id(self) -> int:
        """Get the largest experiment ID for ACTIVE experiments
        Use it to set the artifact location as 1+ max
        """
        return max([int(e['experiment_id']) for e in self.list_experiment_meta()])

    def search_all_runs(
            self,
            experiment_ids: Union[str, int, List[int]] = None,
    ) -> pd.DataFrame:
        """
        Get all runs in pandas format
        Returns:
        """
        if experiment_ids is None:
            experiment_ids = [d['experiment_id'] for d in self.list_experiment_meta()]
        return mlflow.search_runs(experiment_ids, output_format='pandas')

    def read_run_artifact(
            self,
            run_id: str,
            artifact_folder: str,
            artifact_file: str = None,
            experiment_ids: Union[str, int, List[int]] = None,
            read_function: Union[callable, str] = 'pd_parquet',
            columns: iter = None,
            cache_locally: bool = True,
            local_path_root: str = f"/home/jupyter/subreddit_clustering_i18n/data/local_cache/",
            n_sample_files: int = None,
            verbose: bool = False,
    ):
        """
        Example:
        df_v_sub = (
            pd.read_parquet(f"{artifact_uri}/{folder_vect_subs}")
        )
        artifact_file:
            if you only want to read a single file in the artifact_folder, pass this value

        WARNING! if a folder name is a subset of another name, it's possible that
        GCS will return files that are in the other similar folders.
        TODO(djb) Create a check to make sure that the parent of each file matches
         the input folder WITHOUT FUZZY MATCHES
        Example input: df_sub_level__sub_desc_similarity
         output will include:
              - df_sub_level__sub_desc_similarity (expected)
              - df_sub_level__sub_desc_similarity_pair (DO NOT WANT!)
        """
        # set some defaults for common file types so we don't have to load
        if isinstance(read_function, str):
            if 'pd_parquet' == read_function:
                read_function = pd.read_parquet
            elif 'pd_csv' == read_function:
                read_function = pd.read_csv
            elif 'dask_parquet' == read_function:
                read_function = dd.read_parquet
            elif 'json' == read_function:
                read_function = json.loads

            else:
                raise NotImplementedError(f"{read_function} Not implemented...")

        # first get a df for all runs
        # logging.info(f"  Getting all runs...")
        df_all_runs = self.search_all_runs(experiment_ids=experiment_ids)

        # Then get the URI for the specific run we want
        artifact_uri = df_all_runs.loc[
            df_all_runs['run_id'] == run_id,
            'artifact_uri'
        ].values[0]

        if cache_locally:
            storage_client = storage.Client()

            # Extract bucket name & prefix from artifact URI
            parsed_uri = artifact_uri.replace('gs://', '').split('/')
            bucket_name = parsed_uri[0]
            artifact_prefix = '/'.join(parsed_uri[1:])
            full_artifact_folder = f"{artifact_prefix}/{artifact_folder}"

            path_local_folder = Path(f"{local_path_root}/{full_artifact_folder}")
            path_to_load = path_local_folder
            # TODO(djb): fix error: when artifact folder is something like:
            #  'd_ix_to_id/d_ix_to_id.csv',
            #  then the file won't be saved, instead we'll create a folder that has the file name
            #   ad we can't download/read the file! hmm:
            info(f"Local folder to download artifact(s):\n  {path_local_folder}")
            Path.mkdir(path_local_folder, exist_ok=True, parents=True)

            bucket = storage_client.get_bucket(bucket_name)
            l_files_to_download = list(bucket.list_blobs(prefix=full_artifact_folder))
            l_parquet_files_downloaded = list()
            # not all the files in a folder will be parquet files, so we may need to download all files first
            for blob_ in tqdm(l_files_to_download, ncols=80, ascii=True, position=0, leave=True):
                # Skip files that aren't in the same folder as the expected (input) folder
                parent_folder = blob_.name.split('/')[-2]
                if artifact_folder != parent_folder:
                    continue

                f_name = (
                    path_local_folder /
                    f"{blob_.name.split('/')[-1].strip()}"
                )
                if artifact_file is not None:
                    if f_name != artifact_file:
                        continue

                if str(f_name).endswith('parquet'):
                    l_parquet_files_downloaded.append(f_name)

                if f_name.exists():
                    pass
                    # info(f"  {f_name.name} <- File already exists, not downloading")
                else:
                    blob_.download_to_filename(f_name)
            info(f"  Parquet files found: {len(l_parquet_files_downloaded[:n_sample_files]):,.0f}")
            if verbose:
                input_files = ['/'.join(f_.name.split('/')[-2:]) for f_ in l_files_to_download]
                parquet_files = ['/'.join(str(f_).split('/')[-2:]) for f_ in l_parquet_files_downloaded]
                info(f"Input files: \n{input_files}")
                info(f"Parquet files: \n{parquet_files}")
        else:
            path_to_load = f"{artifact_uri}/{artifact_folder}"

        if read_function == dd.read_parquet:
            try:
                return read_function(l_parquet_files_downloaded[:n_sample_files], columns=columns)
            except OSError:
                return read_function(f"{path_to_load}/*.parquet", columns=columns)
        if read_function == pd.read_csv:
            print('path to load\n', path_to_load)
            print('path to TYPE\n', type(path_to_load))
            print('list of parquet\n', l_parquet_files_downloaded)
            logging.warning(f"THIS CALL MAY FALL WITH OR JSON CSV FILES!")
            # try:
            return read_function(path_to_load / artifact_file)
            # except OSError:
            #     return read_function(l_parquet_files_downloaded)
                # This error might happen if there are non-parquet files in the folder
                # so we'll append `*.parquet` to try to read parquet files
                # return read_function(f"{path_to_load}/*.parquet",
                #                      columns=columns)

        elif json.loads != read_function:  # meant for pd.read_parquet
            try:
                return read_function(path_to_load,
                                     columns=columns)
            except TypeError:
                return read_function(path_to_load)

            except OSError:
                # This error might happen if there are non-parquet files in the folder
                # so we'll append `*.parquet` to try to read parquet files
                return read_function(f"{path_to_load}/*.parquet",
                                     columns=columns)

        else:
            # load JSON file might be able to use it with other readers, but might
            #  take a long time depending on file size.
            # A single JSON file with ~11 rows can take 2+ seconds to load
            storage_client = storage.Client()
            parsed_uri = artifact_uri.replace('gs://', '').split('/')
            artifact_prefix = '/'.join(parsed_uri[1:])

            bucket = storage_client.get_bucket(parsed_uri[0])
            blob = bucket.blob(f"{artifact_prefix}/{artifact_folder}")

            # Download the contents of the blob as a string and parse it using json.loads() method
            return read_function(blob.download_as_string(client=None))


def get_git_hash() -> str:
    """
    Borrowed from s-overflow. Use it to get current git hash and add as a tag, IFF
    mlflow hasn't detected the current git tag.
    https://stackoverflow.com/questions/14989858/

    Returns: git-hash as a string
    """
    try:
        git_hash = (
        subprocess.check_output(['git', 'rev-parse', 'HEAD'])
        .strip()
        .decode('ascii')
    )
    except OSError:
        git_hash = None

    return git_hash


def save_and_log_config(
        config: dict,
        local_path: Union[Path, str],
        name_for_artifact_folder: str = 'config',
) -> None:
    """Take a dictionary config, save it locally, then log as parms & as artifact mlflow"""
    info(f"  Saving config to local path...")
    f_joblib = Path(local_path) / f'{name_for_artifact_folder}.gz'
    f_json = Path(local_path) / f'{name_for_artifact_folder}.json'

    joblib.dump(config, f_joblib)

    info(f"  Logging config to mlflow...")
    mlflow.log_artifact(str(f_joblib), name_for_artifact_folder)

    try:
        with open(str(f_json), 'w') as f:
            json.dump(config, f)
        mlflow.log_artifact(str(f_json), name_for_artifact_folder)

    except Exception as e:
        logging.warning(f"Could not save config to JSON. \n{e}")


def save_pd_df_to_parquet_in_chunks(
        df: Union[pd.DataFrame, dd.DataFrame],
        path: Union[str, Path],
        target_mb_size: int = None,
        write_index: bool = True,
) -> None:
    """
    TODO(djb)

    Dask doesn't support multi-index dataframes, so you may need to reset_index()
    before calling this function.
    Maybe it's ok to reset_index and I can set it again on read?

    TODO: Might need to create a data-loader class that can read & reset index for embeddings dfs

    For reference, BigQuery dumped 75 files for ~1 million comments, each file is ~4MB
    """
    if isinstance(df, pd.DataFrame):
        info(f"Converting pandas to dask...")
        mem_usage_mb = df.memory_usage(deep=True).sum() / 1048576

        info(f"  {mem_usage_mb:6,.1f} MB <- Memory usage")

        if target_mb_size is None:
            if mem_usage_mb < 100:
                target_mb_size = 30
            elif 100 <= mem_usage_mb < 1000:
                target_mb_size = 40
            elif 1000 <= mem_usage_mb < 3000:
                target_mb_size = 60
            else:
                target_mb_size = 75

        n_dask_partitions = 1 + int(mem_usage_mb // target_mb_size)

        info(f"  {n_dask_partitions:6,.0f}\t<- target Dask partitions"
             f"\t {target_mb_size:6,.1f} <- target MB partition size"
             )

    # info(f"Saving parquet files to:\n  {path}...")
        (
            dd.from_pandas(df, npartitions=n_dask_partitions)
            .to_parquet(path, write_index=write_index)
        )
    else:
        info(f"  Saving existing dask df as parquet...")
        # if it's a dask df, simply save as is
        # Don't log partition size, this might add overhead/time that's a waste
        # info(f"  {df.npartitions:6,.0f}\t<- EXISTING Dask partitions")
        df.to_parquet(path, write_index=write_index)



#
# ~ fin
#
