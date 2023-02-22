"""
Get ANN (approx nearest neighbors) with ANNOY.
Adapted from a notebook.
In the long term, this should be ported to gazette (kubeflow)
"""
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
from typing import Union, List

import hydra
from hydra.utils import get_original_cwd

import mlflow
import numpy as np
import pandas as pd

from ..utils.tqdm_logger import LogTQDM
from ..utils import mlflow_logger
from ..utils.mlflow_logger import (
    MlflowLogger,
    save_df_and_log_to_mlflow,
)

from ..utils import get_project_subfolder
from ..utils.eda import elapsed_time
from ..data.data_loaders import LoadSubreddits

from ..utils.big_query_utils import load_data_to_bq_table
from ..models.bq_embedding_schemas import embeddings_schema, similar_sub_schema


log = logging.getLogger(__name__)


class GetANN:
    """
    Class to orchestrate creating an ANN index and getting ANN in a format
    ready for BigQuery.

    Originally meant for subreddit-level embeddings, but it could be extended for
    post-level embeddings.

    ETA for ~250k subreddits & 100 ANN per item: 50 minutes.
    """
    def __init__(
            self,
            model_name: str,
            model_version: str,
            mlflow_experiment_name: str,
            embeddings_run_uuid: str,
            subreddit_embeddings_folder: str = 'df_subs_agg_c1',
            post_embeddings_folder: str = 'df_posts_agg_c1',
            n_min_post_per_sub: int = 4,
            index_cols: Union[str, List[str]] = 'subreddit_default',
            n_trees: int = 200,
            metric: str = 'angular',

            upload_to_bq: bool = False,
            bq_project: str = 'reddit-employee-datasets',
            bq_dataset: str = 'david_bermejo',
            bq_table_name: str = 'cau_similar_subreddits_by_text',

            n_sample_embedding_rows: int = None,
            mlflow_tracking_uri: str = 'sqlite',
            mlflow_run_name: str = 'ann_subreddits',
            logs_path: str = 'logs',
    ):
        """"""
        self.model_version = model_version
        self.model_name = model_name

        self.embeddings_run_uuid = embeddings_run_uuid
        self.subreddit_embeddings_folder = subreddit_embeddings_folder
        self.post_embeddings_folder = post_embeddings_folder

        self.n_sample_embedding_rows = n_sample_embedding_rows
        self.n_min_posts_per_sub = n_min_post_per_sub

        self.mlflow_experiment_name = mlflow_experiment_name
        self.mlflow_run_name = mlflow_run_name
        self.mlflow_tracking_uri = mlflow_tracking_uri

        # Create path to store local run
        self.path_local_model = None
        self.path_local_model_figures = None
        self.logs_path = logs_path

        if index_cols == 'subreddit_default':
            self.index_cols = ['subreddit_id', 'subreddit_name']
        else:
            self.index_cols = index_cols

        self.n_trees = n_trees
        self.metric = metric

        # attributes to save outputs
        self.upload_to_bq = upload_to_bq
        self.bq_project = bq_project
        self.bq_dataset = bq_dataset
        self.bq_table_name = bq_table_name

        # Set mlflowLogger instance for central tracker
        self.mlf = MlflowLogger(tracking_uri=self.mlflow_tracking_uri)

    def run_clustering(self):
        """"""
        log.info(f"== Start run_aggregation() method ==")
        t_start_run_ = datetime.utcnow()

        log.info(f"MLflow tracking URI: {mlflow.get_tracking_uri()}")
        self.mlf.set_experiment(self.mlflow_experiment_name)

        with mlflow.start_run(run_name=self.mlflow_run_name):
            log.info(
                f"=== START ANN job - Process ID {os.getpid()}"
            )
            self.mlf.add_git_hash_to_active_run()
            self.mlf.set_tag_hostname(key='host_name')
            self.mlf.log_param_hostname(key='host_name')
            self.mlf.log_cpu_count()
            self.mlf.log_ram_stats(param=True, only_memory_used=False)

            self._set_path_local_model()
            self._create_and_log_config()

            log.info(f"Loading subreddit embeddings...")
            df_embeddings = self._load_sub_embeddings()

            # TODO(djb): filter subreddits
            if self.filter_embeddings is not None:
                if self.filter_embeddings.get('filter_subreddits', False):
                    log.info(f"-- Loading data to filter SUBREDDITS")
                    df_subs = self._load_metadata_for_filtering()

                    df_embeddings = self._apply_filtering(
                        df_embeddings=df_embeddings,
                        df_subs=df_subs,
                    )

            # TODO(djb): build ANNOY index

            # TODO(djb): save ANNOY index to local & log to mlflow

            # TODO(djb): create df with all items with get_top_n_by_item_all_fast()

            # TODO(djb): Add pt & metadata columns

            # TODO(djb): log examples from a few expected subs
            #  e.g., finanzen, antivegan, de, ireland, mexico

            # TODO(djb): Save df to local & log to mlflow

            # TODO(djb): Reshape df to ndJSON & log to mlflow

            # TODO(djb): [OPTIONAL based on flag] Upload JSON data to BigQuery table




            log.info(f"-- Training clustering model --")
            t_start_model_fit = datetime.utcnow()
            self.pipeline.fit(
                df_embeddings[self.l_cols_embeddings]
            )
            total_model_fit_time = elapsed_time(
                start_time=t_start_model_fit,
                log_label='Model fit() time', verbose=True
            )
            mlflow.log_metric('model_fit_time_minutes',
                              total_model_fit_time / timedelta(minutes=1)
                              )
            self.mlf.log_ram_stats(param=False, only_memory_used=True)

    def _load_sub_embeddings(self):
        """Load embeddings for ANN"""
        t_start_load_embeddings_ = datetime.utcnow()
        df_embeddings = self.mlf.read_run_artifact(
            run_id=self.embeddings_run_uuid,
            artifact_folder=self.subreddit_embeddings_folder,
            read_function='pd_parquet',
            cache_locally=True,
        )
        self.l_cols_embeddings = [c for c in df_embeddings.columns if c.startswith('embeddings_')]

        r1_, c1_ = df_embeddings.shape
        log.info(f"{r1_:9,.0f} | {c1_:5,.0f} <- RAW df_embeddings SHAPE")
        if self.n_min_posts_per_sub is not None:
            log.info(f"  Keeping only subs with {self.n_min_posts_per_sub} >= posts")
            df_embeddings = df_embeddings[
                df_embeddings['posts_for_embeddings_count'] >= self.n_min_posts_per_sub
            ]
            r2_, c2_ = df_embeddings.shape
            log.info(f"{r2_:9,.0f} | {c2_:5,.0f} <- df_embeddings SHAPE, after min post filter")

        if self.n_sample_embedding_rows is not None:
            # pick the min, otherwise the sample function can raise error kill job
            n_sample = min([self.n_sample_embedding_rows, len(df_embeddings)])
            log.info(f"  SAMPLING n_rows: {n_sample:,.0f}")
            df_embeddings = df_embeddings.sample(n=n_sample, random_state=42)

        r_, c_ = df_embeddings.shape
        log.info(f"{r_:9,.0f} | {c_:5,.0f} <- df_embeddings SHAPE")

        total_emb_time = elapsed_time(
            start_time=t_start_load_embeddings_,
            log_label='Load embeddings time', verbose=True
        )
        if mlflow.active_run() is not None:
            mlflow.log_metrics(
                {'input_embeddings-n_rows': r_,
                 'input_embeddings-n_cols': c_}
            )
            mlflow.log_metric(
                'model_fit_time_minutes',
                total_emb_time / timedelta(minutes=1)
            )
            self.mlf.log_ram_stats(param=False, only_memory_used=True)
        return df_embeddings

    def _set_path_local_model(self):
        """Set where to save artifacts locally for this model"""
        try:
            get_original_cwd()
            hydra_initialized = True
        except ValueError:
            hydra_initialized = False

        if hydra_initialized:
            log.info(f"Using hydra's path")
            # log.info(f"  Current working directory : {os.getcwd()}")
            # log.info(f"  Orig working directory    : {get_original_cwd()}")
            self.path_local_model = Path(os.getcwd())
        else:
            # create local path to store artifacts before logging to mlflow
            self.path_local_model = get_project_subfolder(
                f"data/models/cluster_embeddings/{datetime.utcnow().strftime('%Y-%m-%d_%H%M%S')}-{self.mlflow_run_name}"
            )
            Path(self.path_local_model).mkdir(exist_ok=True, parents=True)
            log.info(f"  Local model saving directory: {self.path_local_model}")

        self._init_file_log()
        self.path_local_model_figures = self.path_local_model / 'figures'
        Path(self.path_local_model_figures).mkdir(exist_ok=True, parents=True)

    def _create_and_log_config(self):
        """Convert inputs into a dictionary we can save to replicate the run
        Don't log dfs with meta or raw embeddings! they could be dfs that take up gigs of storage
        """
        # TODO(djb): fix -- log configs for nested dictionaries
        #  examples:
        #   - pipeline input configs
        #   - clustering algo inputs
        #   - filter embeddings config
        self.config_to_log_and_store = dict()
        for k_, v_ in vars(self).items():
            try:
                if any([k_.startswith('df_')] +
                       [k_ == c for c in ['mlf', 'pipeline', 'f_log_file', 'optimal_ks']],
                       ):
                    # skip dataframes & some objects that aren't params
                    continue
                elif k_ == 'config_to_log_and_store':
                    # skip config file itself b/c it can lead to weird nested recursion
                    continue
                elif any([isinstance(v_, pd.DataFrame),
                          isinstance(v_, logging.FileHandler),
                          isinstance(v_, Path),
                          ]):
                    # Ignore some objects that won't be easy to pickle
                    # would it be better to only keep things that should be easy to pickle instead?
                    #  e.g., string, list, numeric, None ?
                    continue
                else:
                    self.config_to_log_and_store[k_] = v_
            except Exception as e:
                logging.warning(f"Error logging {k_}:\n  {e}")

        # log as params to mlflow
        for k, v in self.config_to_log_and_store.items():
            try:
                # exclude dicts/ConfDicts from mlflow params, but they should be saved
                #  in joblib &/or yaml
                if (v is None) | isinstance(v, (int, float, bool, str)):
                    mlflow.log_param(k, v)
            except Exception as e:
                log.error(f"Error logging {k}:\n  {e}")

        # log as artifact to GCS
        mlflow_logger.save_and_log_config(
            self.config_to_log_and_store,
            local_path=self.path_local_model,
            name_for_artifact_folder='config',
        )

    def _init_file_log(self) -> None:
        """Create a file & FileHandler to log data
        NOTE: make sure to remove fileHandler after job completes, otherwise a notebook
        or session could keep logging to this file.
        """
        if self.logs_path is not None:
            logger = logging.getLogger()

            path_logs = Path(self.path_local_model) / self.logs_path
            Path.mkdir(path_logs, parents=False, exist_ok=True)
            self.f_log_file = str(
                path_logs /
                f"{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_{self.mlflow_run_name}.log"
            )

            self.fileHandler = logging.FileHandler(self.f_log_file)
            self.fileHandler.setLevel(logging.INFO)

            formatter = logging.Formatter(
                '%(asctime)s | %(levelname)s | "%(message)s"',
                '%Y-%m-%d %H:%M:%S'
            )
            self.fileHandler.setFormatter(formatter)
            logger.addHandler(self.fileHandler)

    def _remove_file_logger(self) -> None:
        """After completing job, remove logging handler to prevent
        info from other jobs getting logged to the same log file
        """
        try:
            log.info(f"    Removing fileHandler...")
            if self.fileHandler is not None:
                logger = logging.getLogger()
                try:
                    logger.removeHandler(self.fileHandler)
                except Exception as e:
                    logging.error(f"Can't remove logger\n{e}")
            else:
                logging.info(f"There is NO fileHandler to remove")
        except Exception as er:
            logging.error(f"Can't remove file logger\n {er}")

#
# ~ fin
#
