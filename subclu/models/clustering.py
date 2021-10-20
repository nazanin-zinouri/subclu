"""
Module to cluster embeddings that have already been aggregated
"""
from datetime import datetime
import logging
from logging import info
import os
from pathlib import Path

import hydra
from hydra.utils import get_original_cwd
from omegaconf import DictConfig

import mlflow

# NOTE: when running from CLI, run script as:
#  python -m subclu.test.test_parallel_jobs
# Because otherwise you'll get relative import errors
from ..utils.tqdm_logger import LogTQDM
from ..utils.mlflow_logger import MlflowLogger, save_pd_df_to_parquet_in_chunks
from ..utils import get_project_subfolder
from ..utils import mlflow_logger

from .clustering_registry import D_MODELS_CLUSTERING


log = logging.getLogger(__name__)


@hydra.main(config_path='../config', config_name="clustering_v0.4.0_base")
def culster_embeddings(cfg: DictConfig) -> object:
    """
    The hydra runner will call the clustering class and apply all the needed
    hyperparameters
    """
    print(f"CFG keys: {cfg.keys()}")

    log.info(f"Define cluster class...")
    cluster = ClusterEmbeddings(
        dict_data_embeddings_to_cluster=cfg['data_embeddings_to_cluster'],
        dict_clustering_algo=cfg['clustering_algo'],
        mlflow_tracking_uri=cfg.get('mlflow_tracking_uri', 'sqlite'),
        mlflow_experiment_name=cfg.get('mlflow_experiment_name', 'v0.4.0_use_multi_clustering_test'),
        mlflow_run_name=cfg.get('mlflow_run_name', 'embedding_clustering'),
        pipeline_dict=cfg.get('pipeline_dict', None),
        logs_path=cfg.get('logs_path', 'logs/ClusterEmbeddings'),
    )

    cluster.run_clustering()
    return cluster
    # if cfg['mlflow_tracking_uri'].startswith('~'):
    #     expanded_path = Path(cfg['mlflow_tracking_uri']).expanduser()
    #     mlflow.set_tracking_uri(f"file://{expanded_path}")
    # else:
    #     mlflow.set_tracking_uri(cfg['mlflow_tracking_uri'])
    # mlflow.set_tracking_uri(cfg['mlflow_tracking_uri'])
    #
    # log.info(f"mlflow tracking URI: {mlflow.get_tracking_uri()}")
    # mlflow.set_experiment(cfg['mlflow_experiment_name'])
    #
    # with mlflow.start_run():
    #     log.info(f" START task {cfg.task} - Process ID {os.getpid()}")
    #     cwd = Path(os.getcwd())
    #
    #     # Log hydra config outputs
    #     path_hydra_config = cwd / '.hydra'
    #     if path_hydra_config.is_dir():
    #         mlflow.log_artifacts(str(path_hydra_config), 'hydra')
    #
    #     log.info(f"{datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')} | END task {cfg.task}")
    #     mlflow.end_run()


class ClusterEmbeddings:
    """
    Class to orchestrate different strategies to cluster embeddings
    - post-aggregates (e.g., post + comment) and
    - subreddit (e.g., post + comment + subreddit descriptions).
    """
    def __init__(
            self,
            dict_data_embeddings_to_cluster: dict,
            dict_clustering_algo: dict,
            mlflow_tracking_uri: str = 'sqlite',
            mlflow_experiment_name: str = 'v0.4.0_use_multi_clustering_test',
            mlflow_run_name: str = 'embedding_clustering',
            pipeline_dict: dict = None,
            logs_path: str = 'logs/ClusterEmbeddings',
            **kwargs
    ):
        """"""
        self.dict_data_embeddings_to_cluster = dict_data_embeddings_to_cluster
        self.dict_clustering_algo = dict_clustering_algo

        self.mlflow_experiment_name = mlflow_experiment_name
        self.mlflow_run_name = mlflow_run_name
        self.mlflow_tracking_uri = mlflow_tracking_uri

        self.pipeline_dict = pipeline_dict

        # Create path to store local run
        self.path_local_model = None
        self.logs_path = logs_path

        # use pre-loaded metadata if running in interactive mode
        # self.df_subs_meta = df_subs_meta
        # self.df_posts_meta = df_posts_meta
        # self.df_comments_meta = df_comments_meta

        # self.embeddings_read_fxn = embeddings_read_fxn
        # self.metadata_read_fxn = metadata_read_fxn

        # Set mlflowLogger instance for central tracker
        self.mlf = MlflowLogger(tracking_uri=self.mlflow_tracking_uri)

    def run_clustering(self):
        """"""
        with mlflow.start_run():
            log.info(
                f"=== START CLUSTERING - Process ID {os.getpid()}")

            # hydra takes care of creating a custom working directory
            print(f"Current working directory : {os.getcwd()}")
            print(f"Orig working directory    : {get_original_cwd()}")
            if os.getcwd() != get_original_cwd():
                log.info(f"Using hydra's path")
                self.path_local_model = Path(os.getcwd())
            else:
                # create local path to store artifacts before logging to mlflow
                self.path_local_model = get_project_subfolder(
                    f"data/models/aggregate_embeddings/{datetime.utcnow().strftime('%Y-%m-%d_%H%M%S')}-{self.mlflow_run_name}"
                )
                Path(self.path_local_model).mkdir(exist_ok=True, parents=True)
                info(f"  Local model saving directory: {self.path_local_model}")
                self._init_file_log()

            # TODO(djb): load clustering algo
            log.info(f"Loading clustering model...")
            cls = D_MODELS_CLUSTERING[self.dict_clustering_algo['model_key']](
                **self.dict_clustering_algo['model_kwargs']
            )

            log.info(f"Loading embeddings...")
            df_embeddings = self.mlf.read_run_artifact(
                run_id=self.dict_data_embeddings_to_cluster['run_uuid'],
                artifact_folder=self.dict_data_embeddings_to_cluster['df_sub_level_agg_c_post_comments_and_sub_desc'],
                read_function='pd_parquet',
                cache_locally=True,
            )
            log.info(f"{df_embeddings.shape} <- df_embeddings SHAPE")

            # TODO(djb): create pipeline with pre-processing steps
            #  e.g., normalize text &/or apply SVD

            # TODO(djb): Fit clustering algo

            # TODO(djb): Get predictions for each row

            # TODO(djb): Save predictions & log to mlflow

            # TODO(djb): Get metrics to compare clusters

            # TODO(djb): Save clustering algo & log to mlflow

            # TODO(djb):

            # Log hydra config outputs
            path_hydra_config = self.path_local_model / '.hydra'
            if path_hydra_config.is_dir():
                mlflow.log_artifacts(str(path_hydra_config), 'hydra')

            log.info(f"=== END clustering ===")
            mlflow.end_run()

        if os.getcwd() == get_original_cwd():
            info(f"    Removing fileHandler...")
            self._remove_file_logger()

    def _init_file_log(self) -> None:
        """Create a file & FileHandler to log data"""
        # TODO(djb): make sure to remove fileHandler after job is run_aggregation()
        if self.logs_path is not None:
            logger = logging.getLogger()

            path_logs = Path(self.logs_path)
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
        if self.fileHandler is not None:
            logger = logging.getLogger()
            try:
                logger.removeHandler(self.fileHandler)
            except Exception as e:
                logging.warning(f"Can't remove logger\n{e}")



if __name__ == "__main__":
    culster_embeddings()
