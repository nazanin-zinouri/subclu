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
from logging import info
import os
from pathlib import Path
import shutil
import socket
import subprocess
from typing import List, Union

import joblib
import numpy as np
import pandas as pd
import yaml

from google.cloud import storage
from dask import dataframe as dd
from omegaconf import DictConfig, OmegaConf
import mlflow
from mlflow.utils import mlflow_tags
from mlflow.exceptions import MlflowException
from sklearn.metrics import classification_report, confusion_matrix
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
            'v0.4.0_use_multi_clustering_test',
            'v0.4.0_use_multi_clustering',

            # new experiments for v0.4.1 - again using inference GPU VM
            'v0.4.1_mUSE_inference_test',
            'v0.4.1_mUSE_inference',
            'v0.4.1_mUSE_aggregates_test',
            'v0.4.1_mUSE_aggregates',
            'v0.4.1_mUSE_clustering_test',
            'v0.4.1_mUSE_clustering',

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
                if metric:
                    mlflow.log_metric(metric_name, cpu_count)
                if param:
                    mlflow.log_param(metric_name, cpu_count)

            return cpu_count
        except Exception as e:
            logging.error(f"Error logging CPU info\n {e}")
            return None

    @staticmethod
    def log_ram_stats(
            send_to_info: bool = True,
            param: bool = False,
            metric: bool = True,
            only_memory_used: bool = False,
    ) -> Union[dict, None]:
        """Log total, active, & used RAM
        Could be helpful when debugging to identify jobs limited by RAM

        Set param=False by default because we want to call this fxn multiple times
        and mlflow returns an error if we try to set the value of a param more
        than one time.
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
                if metric:
                    mlflow.log_metrics(d_ram)
                if param:
                    # Total memory is the only thing that is a param
                    #  the others are metrics
                    mlflow.log_param('memory_total', d_ram['memory_total'])
            return d_ram

        except Exception as e:
            logging.error(f"Error logging RAM info\n {e}")
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

    def list_run_artifacts(
            self,
            run_id: str,
            artifact_folder: str = None,
            experiment_ids: Union[str, int, List[int]] = None,
            only_top_level: bool = True,
            verbose: bool = False,
    ):
        """list artifacts for a run in GCS"""
        # first get a df for all runs
        # logging.info(f"  Getting all runs...")
        df_all_runs = self.search_all_runs(experiment_ids=experiment_ids)

        # Then get the URI for the specific run we want
        artifact_uri = df_all_runs.loc[
            df_all_runs['run_id'] == run_id,
            'artifact_uri'
        ].values[0]

        # initialize GCS client
        storage_client = storage.Client()

        # Extract bucket name & prefix from artifact URI
        parsed_uri = artifact_uri.replace('gs://', '').split('/')
        bucket_name = parsed_uri[0]
        artifact_prefix = '/'.join(parsed_uri[1:])
        # this root is expected to be: '<experiment_id>/<uuid>/artifacts'
        root_artifact_prefix = '/'.join(parsed_uri[-3:])

        if artifact_folder is not None:
            full_artifact_folder = f"{artifact_prefix}/{artifact_folder}"
        else:
            full_artifact_folder = f"{artifact_prefix}"

        bucket = storage_client.get_bucket(bucket_name)
        l_files_to_check = list(bucket.list_blobs(prefix=full_artifact_folder))
        if verbose:
            info(f"{len(l_files_to_check):6,.0f} <- Artifacts to check count")

        # Find only the top-level files & folders
        l_files_and_folders_top_level = list()
        l_files_and_folders_clean = list()
        for blob_ in l_files_to_check:
            b_name = blob_.name
            # parent_folders_1_2_3 = '/'.join(b_name.split('/')[-4:-1])

            if root_artifact_prefix in b_name:
                l_files_and_folders_clean.append(b_name)
            else:
                if verbose:
                    info(f"Skip files that aren't in the run's artifacts folder")
                continue

            # first get the directory/path AFTER the /artifacts
            keys_after_root = b_name.split(f"{root_artifact_prefix}/")[-1]
            # Then append ONLY the first path or file
            l_files_and_folders_top_level.append(keys_after_root.strip().split('/')[0])

        # Convert the list to a set b/c we'll have dupes when multiple files are in a subfolder
        l_files_and_folders_top_level = set(l_files_and_folders_top_level)
        l_files_and_folders_top_level = sorted(l_files_and_folders_top_level)

        info(f"{len(l_files_and_folders_clean):6,.0f} <- Artifacts clean count")
        info(f"{len(l_files_and_folders_top_level):6,.0f} <- Artifacts & folders at TOP LEVEL clean count")
        if only_top_level:
            return l_files_and_folders_top_level
        else:
            return l_files_and_folders_clean

    def read_run_artifact(
            self,
            run_id: str,
            artifact_folder: str = None,
            artifact_file: str = None,
            experiment_ids: Union[str, int, List[int]] = None,
            read_function: Union[callable, str] = 'pd_parquet',
            columns: iter = None,
            cache_locally: bool = True,
            local_path_root: str = f"/home/jupyter/subreddit_clustering_i18n/data/local_cache/",
            n_sample_files: int = None,
            verbose: bool = False,
            read_csv_kwargs: dict = None,
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
                read_function = json.load

            else:
                raise NotImplementedError(f"{read_function} Not implemented...")

        if artifact_file is not None:
            artifact_file_name_only = artifact_file.split('/')[-1]

        # first get a df for all runs
        # logging.info(f"  Getting all runs...")
        df_all_runs = self.search_all_runs(experiment_ids=experiment_ids)

        # Then get the URI for the specific run we want
        artifact_uri = df_all_runs.loc[
            df_all_runs['run_id'] == run_id,
            'artifact_uri'
        ].values[0]

        l_parquet_files_downloaded = list()
        l_json_files_downloaded = list()
        l_csv_files_downloaded = list()

        if cache_locally:
            storage_client = storage.Client()

            # Extract bucket name & prefix from artifact URI
            parsed_uri = artifact_uri.replace('gs://', '').split('/')
            bucket_name = parsed_uri[0]
            artifact_prefix = '/'.join(parsed_uri[1:])

            # If we get an artifact file, get the implicit artifact folder from it
            if (artifact_folder is None) & (artifact_file is not None):
                split_file = artifact_file.split('/')

                # Check whether file is in top level or in a subfolder
                if len(split_file) <= 1:
                    artifact_folder = artifact_uri.split('/')[-1]
                    full_artifact_folder = artifact_prefix
                else:
                    artifact_folder = '/'.join(artifact_file.split('/')[:-1])
                    full_artifact_folder = f"{artifact_prefix}/{artifact_folder}"
            else:
                full_artifact_folder = f"{artifact_prefix}/{artifact_folder}"

            path_local_folder = Path(f"{local_path_root}/{full_artifact_folder}")
            path_to_load = path_local_folder
            # TODO(djb): fix error: when artifact folder is something like:
            #  'd_ix_to_id/d_ix_to_id.csv',
            #  then the file won't be saved, instead we'll create a folder that has the file name
            #   and we can't download/read the file! hmm:
            info(f"Local folder to download artifact(s):\n  {path_local_folder}")
            Path.mkdir(path_local_folder, exist_ok=True, parents=True)

            bucket = storage_client.get_bucket(bucket_name)
            # if artifact_file is None:
            # If we get a file as input, only download that specific file
            #  the logic to only download the specific file lives inside the loop, not here
            l_files_to_download = list(bucket.list_blobs(prefix=full_artifact_folder))
            # else:
            #     new_prefix = f"{full_artifact_folder}/{artifact_file_name_only}"
            #     print(f"New prefix: {new_prefix}")
            #     l_files_to_download = list(bucket.list_blobs(prefix=f"{full_artifact_folder}/{artifact_file_name_only}"))

            # not all the files in a folder will be parquet files, so we may need to download all files first
            for blob_ in tqdm(l_files_to_download, ncols=80, ascii=True, position=0, leave=True):
                parent_folder = blob_.name.split('/')[-2]
                if artifact_folder != parent_folder:
                    if verbose:
                        info(f"Skip files that aren't in the same folder as the expected (input) folder")
                    continue

                local_absolute_f_name = (
                    path_local_folder /
                    f"{blob_.name.split('/')[-1].strip()}"
                )

                if artifact_file is not None:
                    if local_absolute_f_name.name != artifact_file.split('/')[-1].strip():
                        if verbose:
                            info(f"Skipping file because it doesn't match artifact_file")
                        continue

                if str(local_absolute_f_name).lower().endswith('parquet'):
                    l_parquet_files_downloaded.append(local_absolute_f_name)
                if str(local_absolute_f_name).lower().endswith('json'):
                    l_json_files_downloaded.append(local_absolute_f_name)
                if any([str(local_absolute_f_name).lower().endswith(ext_) for ext_ in ['csv', 'txt', 'log']]):
                    l_csv_files_downloaded.append(local_absolute_f_name)

                if local_absolute_f_name.exists():
                    if verbose:
                        info(f"  {local_absolute_f_name.name} <- File already exists, not downloading")

                else:
                    blob_.download_to_filename(local_absolute_f_name)

            if read_function in [dd.read_parquet, pd.read_parquet]:
                info(f"  Parquet files found: {len(l_parquet_files_downloaded):5,.0f}")
                info(f"  Parquet files to use: {len(l_parquet_files_downloaded[:n_sample_files]):5,.0f}")
            if verbose:
                input_file_names = ['/'.join(f_.name.split('/')[-2:]) for f_ in l_files_to_download]
                parquet_file_names = ['/'.join(str(f_).split('/')[-2:]) for f_ in l_parquet_files_downloaded]
                json_file_names = ['/'.join(str(f_).split('/')[-2:]) for f_ in l_json_files_downloaded]
                info(f"Parquet files: {len(parquet_file_names)} \n  {parquet_file_names}")
                info(f"JSON files: {len(json_file_names)} \n  {json_file_names}")
                info(f"Input files: {len(input_file_names)} \n  {input_file_names}")
        else:
            path_to_load = f"{artifact_uri}/{artifact_folder}"

        if read_function == dd.read_parquet:
            try:
                return read_function(l_parquet_files_downloaded[:n_sample_files], columns=columns)
            except OSError:
                return read_function(f"{path_to_load}/*.parquet", columns=columns)

        if read_function == pd.read_csv:
            if verbose:
                print('path to load\n', path_to_load)
                print('path to TYPE\n', type(path_to_load))
                print('list of CSV\n', l_csv_files_downloaded)
            return read_function(l_csv_files_downloaded[0], **read_csv_kwargs)

        elif pd.read_parquet == read_function:
            if n_sample_files is not None:
                logging.warning(f"Loading ALL files to pandas df. File sampleing NOT implemented.")
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

        elif json.load == read_function:
            with open(l_json_files_downloaded[0], 'r') as f_:
                dict_ = read_function(f_)
            return dict_

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
    f_yaml = Path(local_path) / f'{name_for_artifact_folder}.yaml'

    try:
        info(f"  Logging config to mlflow with joblib...")
        joblib.dump(config, f_joblib)
        mlflow.log_artifact(str(f_joblib), name_for_artifact_folder)
    except TypeError as er:
        logging.error(f"  Could not safe config with joblib.\n{er}")

    try:
        with open(str(f_json), 'w') as f:
            json.dump(config, f)
        mlflow.log_artifact(str(f_json), name_for_artifact_folder)

    except Exception as e:
        logging.warning(f"  Could not save config to JSON. \n{e}")

    try:
        info(f"  Logging config to mlflow with YAML...")
        with open(str(f_yaml), 'w') as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
        mlflow.log_artifact(str(f_yaml), name_for_artifact_folder)
    except AttributeError:
        """
        pyyaml doesn't support some data types, so we fall back to OmegaConf 
        An omegaconf DictConfig is not compatible with pyyaml, we need to convert it
        in order to serialize as yaml
        https://github.com/omry/omegaconf/issues/334#issuecomment-683121428
        """
        oc_config = OmegaConf.create(config)
        try:
            with open(str(f_yaml), 'w') as f:
                OmegaConf.save(config=oc_config, f=f)
            mlflow.log_artifact(str(f_yaml), name_for_artifact_folder)
        except Exception as e:
            logging.error(f"    Could not save config to YAML. \n{e}")


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
                target_mb_size = 50
            elif 100 <= mem_usage_mb < 1000:
                target_mb_size = 100
            elif 1000 <= mem_usage_mb < 3000:
                target_mb_size = 200
            else:
                # We have some dfs that can take up over 156GB of RAM. So we need to increase the
                #  target_mb size, otherwise we'll end up with thousands of tiny dfs. example:
                # INFO | "  156,726.2 MB <- Memory usage"
                # INFO | "   2,090  <- target Dask partitions      75.0 <- target MB partition size"
                target_mb_size = 330

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


def save_df_and_log_to_mlflow(
        df: pd.DataFrame,
        path: Union[str, Path],
        subfolder: str,
        index: bool = True,
        parquet_via_dask: bool = False,
        save_csv: bool = True,
):
    """Save df """
    folder_full_ = Path(path) / subfolder
    Path(folder_full_).mkdir(exist_ok=True, parents=True)

    if parquet_via_dask:
        save_pd_df_to_parquet_in_chunks(
            df=df,
            path=folder_full_,
            write_index=index,
        )
    else:
        df.to_parquet(
            folder_full_ / f"{subfolder}.parquet",
            index=index
        )

    if save_csv:
        df.to_csv(
            folder_full_ / f"{subfolder}.csv",
            index=index,
        )
    if mlflow.active_run() is not None:
        mlflow.log_artifacts(str(folder_full_), artifact_path=subfolder)
    else:
        logging.warning(f"  Did NOT find an active mlflow run")


def log_pipeline_params(
        pipeline,
        save_path: Union[Path, str] = None,
        subfolder: str = 'pipeline_params',
        verbose: bool = True,
) -> None:
    """
    the dunder `__` keys should be the keys for each step of the pipeline
    e.g.,:
        'vectorize__ngram_range': (1, 1),
        'vectorize__norm': 'l2',
        'vectorize__preprocessor': None,
        'clf__C': 1.0,
        'clf__class_weight': None,
        'clf__fit_intercept': True,

    Items to exclude from logging:
    - 'cv' (cross fold splits) because those are cv split indices that can be large,
        not needed for analysis, and might crash mlflow
    - 'vocabulary_' in case it includes sensitive words/content
    - 'stop_words' in case it includes sensitive words/tokens

    Instead of tracking vocab & stop_words here, include them as a config
      keywords and log as a separate parameter.
    If needed, we can recover stopwords and vocab from vectorizer, but let's reduce
     the surface area for exposing this data

    Args:
        pipeline: pipeline object to log
        verbose: Whether to log to logger (not mlflow) additional info
        save_path: Path or string-like to save the selected params to log.
        f_name: Name of the file to save
        
    Returns:
        None
    """
    info(f"Logging pipeline params...")

    d_select_params_raw = (
        {k: v for k, v in pipeline.get_params().items()
         if all(['__' in k,
                 '_cv' not in k,         # don't need to keep cross-validation folds
                 'stop_words' not in k,  # vectorize__stop_words
                 'vocabulary' not in k,  # vectorize__vocabulary
                 '_dtype' not in k,      # vectorize sometimes includes dtype, no need to set
                                         #  & dtype creates problems when serializing to JSON
                 ])}
    )
    d_select_params = d_select_params_raw.copy()  # rename_dict_for_mlflow(d_select_params_raw)

    if save_path is not None:
        path_subfolder = Path(save_path) / subfolder
        Path.mkdir(path_subfolder, exist_ok=True, parents=True)
        info(f"  Saving pipeline params to: {path_subfolder}")
        try:
            joblib.dump(d_select_params_raw,
                        str(path_subfolder / f"{subfolder}.gz")
                        )
        except Exception as er:
            logging.error(f"  Error saving pipeline params to dictionary \n  {er}")
        try:
            with open(path_subfolder / f"{subfolder}.json", 'w') as f:
                json.dump(d_select_params_raw, f)
        except Exception as er:
            logging.error(f"  Error saving pipeline params to json \n  {er}")

        try:
            oc_config = OmegaConf.create(d_select_params_raw)
            with open(path_subfolder / f"{subfolder}.yaml", 'w') as f:
                OmegaConf.save(config=oc_config, f=f)
        except Exception as e:
            logging.error(f"  Could not save config to YAML. \n{e}")

        logging.info(f"  Logging pipeline params to mlflow...")
        mlflow.log_artifacts(str(path_subfolder), subfolder)

    # TODO(djb): this step might need to change if we split:
    #   vectorizing & IDF into separate steps
    # get vectorizer & model/clf names
    #   log len of vocab & ngram_range, if available
    # d_vect_params_raw = dict()
    # vectorizer_key = 'vectorize'
    # try:
    #     d_vect_params_raw['vectorizer__name'] = type(pipeline[vectorizer_key]).__name__
    #     d_vect_params_raw['vocabulary_len'] = len(pipeline[vectorizer_key].vocabulary_)
    #     d_vect_params_raw['ngram_range'] = pipeline[vectorizer_key].ngram_range
    #     d_vect_params_raw['ngram_range_min'] = min(pipeline[vectorizer_key].ngram_range)
    #     d_vect_params_raw['ngram_range_max'] = max(pipeline[vectorizer_key].ngram_range)
    #     for k, v in d_vect_params_raw.items():
    #         try:
    #             mlflow.log_param(k, v)
    #         except Exception as e:
    #             logging.error(f"Error logging param:{k} with value:{v}\n  {e}")
    # except (KeyError, AttributeError) as e:
    #     logging.warning(f"Error logging vectorizer params:\n  {e}")

    # Log name for each step so it's easier to compare different configs
    for tup_ in pipeline.steps:
        try:
            step_name = tup_[0]
            transformer_name = type(tup_[1]).__name__
            mlflow.log_param(f"pipe-{step_name}_name", transformer_name)
        except (KeyError, AttributeError) as e:
            logging.warning(f"Error logging step:\n  {e}")

    for param, val in d_select_params.items():
        mlflow.log_param(f"_pipe-{param}", val)
        if verbose:
            info(f"  {param}: {val}")


def rename_for_mlflow(metric_val_or_name: Union[str, float, Path]
                      ) -> Union[str, float]:
    """Replace some values that may break mlflow's log naming

    Names usually break when using local storage for parameter values
    or key names. However, we might not needed now that we're using the server
    """
    if isinstance(metric_val_or_name, Path):
        return metric_val_or_name.name

    try:
        return (
            metric_val_or_name
            .replace(',', '')
            .replace('.', '-')
            .replace('/', '-')
            .replace(' ', '_')
            .replace('\n', '-')
            .replace('\t', '-')
        )
    except (SyntaxError, AttributeError):
        # if float or another similar type, return that value instead:
        return metric_val_or_name


def log_clf_report_and_conf_matrix(
        y_true: Union[pd.Series, np.array],
        y_pred: Union[pd.Series, np.array],
        data_fold_name: str,
        class_labels: iter = None,
        save_path: str = None,
        log_metrics_to_mlflow: bool = True,
        log_artifacts_to_mlflow: bool = False,
        log_to_console: bool = True,
        remove_files_from_local_path: bool = False,
        print_clf_df: bool = False,
        return_confusion_mx: bool = False,
) -> Union[None, pd.DataFrame]:
    """Take a clf report (in dictionary format) and log specific params to:
     - logging
     - mlflow

    Returns: None or pd.DataFrame
    """
    logging.info(f"Start processing {data_fold_name.upper()} metrics for logging")

    clf_report = classification_report(y_true=y_true, y_pred=y_pred,
                                       labels=class_labels,
                                       output_dict=True)
    log_metrics = dict()
    for class_lab, results in clf_report.items():
        # only save/report selected keys for dashboard, save clf report separately
        if class_lab == 'accuracy':
            log_metrics['accuracy'] = results
        elif class_lab in ['weighted avg', 'macro avg']:
            for score, val in results.items():
                log_metrics[(f"{score.lower().replace('-', '_')}_"
                             f"{class_lab.replace(' ', '_')}")] = val
        elif class_lab not in ['micro avg', 'macro avg']:
            metrics_filtered = ['f1-score', 'support']  # 'precision', 'recall'
            for metric, val in {met: results[met] for met in metrics_filtered if met in results}.items():
                log_metrics[f"{class_lab.replace(' ', '_')}-{metric.replace('-', '_')}"] = val

    for met, val in tqdm(log_metrics.items()):
        # this logging line is what Sagemaker will try to capture for metric performance
        # and could be used for hyperparm tuning job. Generally we may want to use log-loss, though
        if val is None:
            logging.warning(f" Metric: {met} value is `None`, setting to -99")
            val = -99
        if log_to_console:
            if isinstance(val, float):
                logging.info(f" {data_fold_name.capitalize()} {met}: {val:6f}")
            else:
                logging.info(f" {data_fold_name.capitalize()} {met}: {val}")

        if log_metrics_to_mlflow:
            mlflow.log_metric(f"{data_fold_name.lower()}-{rename_for_mlflow(met)}", val)

    clf_report_for_df = clf_report.copy()
    clf_report_for_df.pop('accuracy', None)
    df_rep = pd.DataFrame.from_dict(clf_report_for_df, orient='index')
    df_rep.index.name = 'class'

    df_rep.loc['accuracy', 'support'] = df_rep.loc['weighted avg', 'support']
    df_rep.loc['accuracy', 'f1-score'] = log_metrics['accuracy']
    df_rep['support'] = df_rep['support'].astype(int)
    if print_clf_df:
        with pd.option_context('display.float_format', '{:,.3f}'.format):
            print(df_rep.fillna(''))

    if save_path is not None:
        Path.mkdir(Path(save_path), parents=True, exist_ok=True)
        df_rep[['precision', 'recall', 'f1-score', 'support']
               ].to_csv(Path(save_path) / f"{data_fold_name}-classification_report.csv",
                        index=True)

    # create confusion matrix & save it too
    if y_true.ndim > 1:
        y_true = pd.Series(np.argmax(y_true, axis=-1)).map({i: val for i, val in enumerate(class_labels)})
    conf_mx = confusion_matrix(y_true, y_pred)

    # Add PPV & NPV calculation
    tn, fp, fn, tp = conf_mx.ravel()
    d_extra_metrics = {
        f"{data_fold_name.lower()}-ppv": (tp / (tp + fp)),
        f"{data_fold_name.lower()}-npv": (tn / (tn + fn)),

        f"{data_fold_name.lower()}-tn": tn,
        f"{data_fold_name.lower()}-fp": fp,
        f"{data_fold_name.lower()}-fn": fn,
        f"{data_fold_name.lower()}-tp": tp,
    }
    if log_to_console:
        for metric, val in d_extra_metrics.items():
            info(f"{data_fold_name.lower()}-{metric}: {val}")
    if log_metrics_to_mlflow:
        mlflow.log_metrics(d_extra_metrics)

    df_conf_mx = pd.DataFrame(conf_mx,
                              index=[rename_for_mlflow(lab) for lab in class_labels],
                              columns=[rename_for_mlflow(lab) for lab in class_labels])
    if save_path is not None:
        df_conf_mx.to_csv(Path(save_path) / f"{data_fold_name}-confusion_matrix.csv",
                          index=True)

    if log_artifacts_to_mlflow:
        mlflow.log_artifacts(save_path)
    if remove_files_from_local_path:
        shutil.rmtree(save_path, ignore_errors=True)

    if return_confusion_mx:
        return df_conf_mx


#
# ~ fin
#
