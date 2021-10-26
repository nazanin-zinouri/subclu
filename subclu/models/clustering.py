"""
Module to cluster embeddings that have already been aggregated
"""
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
# from typing import Union

import joblib
import mlflow
import mlflow.sklearn
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec

from tqdm import tqdm
import hydra
from hydra.utils import get_original_cwd
from omegaconf import DictConfig

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from scipy.cluster.hierarchy import fcluster
from sklearn.pipeline import Pipeline

# NOTE:
# To avoid relative import errors when running from CLI, run script with:
#  * -m flag
#  * no ".py" ending
# Example:
#  python -m subclu.test.test_parallel_jobs
# from ..utils.tqdm_logger import LogTQDM
from ..utils import mlflow_logger
from ..utils.mlflow_logger import MlflowLogger, save_pd_df_to_parquet_in_chunks
from ..utils import get_project_subfolder
from ..utils.eda import elapsed_time
from ..data.data_loaders import LoadSubreddits, LoadPosts

from .clustering_utils import (
    create_linkage_for_dendrogram, fancy_dendrogram,
    plot_elbow_and_get_k
)
from .clustering_registry import (
    D_CLUSTER_MODELS, D_CLUSTER_PIPELINE,
    D_CLUSTER_METRICS_WITH_KNOWN_LABELS
)


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
        dict_data_text_and_metadata=cfg['data_text_and_metadata'],
        embeddings_to_cluster=cfg['embeddings_to_cluster'],
        mlflow_tracking_uri=cfg.get('mlflow_tracking_uri', 'sqlite'),
        n_sample_embedding_rows=cfg.get('n_sample_embedding_rows', None),
        mlflow_experiment_name=cfg.get('mlflow_experiment_name', 'v0.4.0_use_multi_clustering_test'),
        mlflow_run_name=(
            f"{cfg.get('mlflow_run_name', 'embedding_clustering')}-{datetime.utcnow().strftime('%Y-%m-%d_%H%M%S')}"
        ),
        dict_filter_embeddings=cfg.get('filter_embeddings', None),
        pipeline_config=cfg.get('pipeline', None),
        logs_path=cfg.get('logs_path', 'logs/ClusterEmbeddings'),
    )

    cluster.run_clustering()
    return cluster


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
            dict_data_text_and_metadata: dict,
            embeddings_to_cluster: str = 'df_sub_level_agg_c_post_comments_and_sub_desc',
            n_sample_embedding_rows: int = None,
            mlflow_tracking_uri: str = 'sqlite',
            mlflow_experiment_name: str = 'v0.4.0_use_multi_clustering_test',
            mlflow_run_name: str = 'embedding_clustering',
            pipeline_config: dict = None,
            dict_filter_embeddings: dict = None,
            logs_path: str = 'logs/ClusterEmbeddings',
            # **kwargs
    ):
        """"""
        self.dict_data_embeddings_to_cluster = dict_data_embeddings_to_cluster
        self.dict_clustering_algo = dict_clustering_algo
        self.dict_data_text_and_metadata = dict_data_text_and_metadata

        self.embeddings_to_cluster = embeddings_to_cluster
        self.n_sample_embedding_rows = n_sample_embedding_rows

        self.mlflow_experiment_name = mlflow_experiment_name
        self.mlflow_run_name = mlflow_run_name
        self.mlflow_tracking_uri = mlflow_tracking_uri

        # Create path to store local run
        self.path_local_model = None
        self.path_local_model_figures = None
        self.logs_path = logs_path

        # pipeline to store model
        self.pipeline_config = pipeline_config
        self.pipeline = None
        self.dict_filter_embeddings = dict_filter_embeddings

        # attributes to save outputs
        self.df_accel = None
        self.optimal_ks = None

        # Set mlflowLogger instance for central tracker
        self.mlf = MlflowLogger(tracking_uri=self.mlflow_tracking_uri)

    def run_clustering(self):
        """"""
        log.info(f"== Start run_aggregation() method ==")
        t_start_run_clustering = datetime.utcnow()

        log.info(f"MLflow tracking URI: {mlflow.get_tracking_uri()}")
        self.mlf.set_experiment(self.mlflow_experiment_name)

        with mlflow.start_run(run_name=self.mlflow_run_name):
            log.info(
                f"=== START CLUSTERING - Process ID {os.getpid()}")
            self.mlf.add_git_hash_to_active_run()
            self.mlf.set_tag_hostname(key='host_name')
            self.mlf.log_param_hostname(key='host_name')
            self.mlf.log_cpu_count()
            self.mlf.log_ram_stats(param=True, only_memory_used=False)

            self._set_path_local_model()

            self._create_and_log_config()

            log.info(f"Creating pipeline...")
            self._create_pipeline()

            log.info(f"Loading embeddings...")
            df_embeddings = self._load_embeddings()

            if self.dict_filter_embeddings is not None:
                if self.dict_filter_embeddings.get('filter_subreddits', False):
                    log.info(f"-- Loading data to filter SUBREDDITS")
                    df_subs = self._load_metadata_for_filtering()

                    df_embeddings = self._apply_filtering(
                        df_embeddings=df_embeddings,
                        df_subs=df_subs,
                    )

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

            mlflow_logger.log_pipeline_params(
                self.pipeline,
                save_path=self.path_local_model,
            )

            self._log_pipeline_to_mlflow()

            # TODO(djb): Get metrics: elbow & "optimal" k's
            #  Only applies to agg-clutering models
            self._get_linkage_and_optimal_ks()

            # TODO(djb): Get predictions for each row (subreddit or post)
            self._get_cluster_ids_and_labels(
                df_embeddings=df_embeddings,
                df_subs=df_subs,
            )
            # TODO(djb): Save predictions & log to mlflow

            # TODO(djb): Create, save & log dendrograms



            # TODO(djb): Get metrics to compare clusters: Silhouette

            # TODO(djb): Get metrics to compare clusters:
            #  - classification report
            #  - adjusted metrics (rand index, mutual info)
            #  - homegeneity

            # TODO(djb):

            # Log hydra config outputs
            path_hydra_config = self.path_local_model / '.hydra'
            if path_hydra_config.is_dir():
                mlflow.log_artifacts(str(path_hydra_config), 'hydra')

            # Finish logging total time + end mlflow run
            total_fxn_time = elapsed_time(start_time=t_start_run_clustering,
                                          log_label='Total Clustering fxn time', verbose=True)
            mlflow.log_metric('vectorizing_time_minutes',
                              total_fxn_time / timedelta(minutes=1)
                              )
            log.info(f"=== END clustering ===")
            mlflow.end_run()

        if os.getcwd() == get_original_cwd():
            log.info(f"    Removing fileHandler...")
            self._remove_file_logger()

    def _create_pipeline(self):
        """Create pipeline with steps from pipeline config

        When adding steps at the beginning of pipeline, we need to add them from last to first.

        Full pipeline would look something like this:
        pipe_full = Pipeline([
            ('normalize', Normalizer(norm='l2')),
            ('reduce', TruncatedSVD(n_components=50)),
            ('cluster', AgglomerativeClustering(n_clusters=30, affinity='euclidean', connectivity=False)),
        ])
        """
        cls = D_CLUSTER_MODELS[self.dict_clustering_algo['model_name']](
            **self.dict_clustering_algo['model_kwargs']
        )
        # start with only the clustering algo
        self.pipeline = Pipeline([
            ('cluster', cls)
        ])

        # Then add other steps if set in the config
        #  Start with latest step first (reduce first, normalize last)
        l_pipe_steps_to_check = ['reduce', 'normalize']
        if self.pipeline_config is not None:
            log.info(f"Checking custom pipeline config...\n  {self.pipeline_config}")

            for step_ in l_pipe_steps_to_check:
                if self.pipeline_config.get(step_, dict()).get('add_step', False):
                    log.info(f"  Adding step: {step_}")
                    trf_name = self.pipeline_config[step_]['name']
                    trf_kwargs = self.pipeline_config[step_].get('kwargs_', None)

                    # Check if we have custom kwargs for this step:
                    if trf_kwargs is not None:
                        transformer_ = D_CLUSTER_PIPELINE[step_][trf_name](
                            **trf_kwargs
                        )
                    else:
                        transformer_ = D_CLUSTER_PIPELINE[step_][trf_name]()

                    self.pipeline.steps.insert(
                        0,
                        (step_, transformer_),
                    )
        log.info(f"  Pipeline to train:\n  {self.pipeline}")

    def _load_embeddings(self):
        """Load embeddings for clustering"""
        df_embeddings = self.mlf.read_run_artifact(
            run_id=self.dict_data_embeddings_to_cluster['run_uuid'],
            artifact_folder=self.dict_data_embeddings_to_cluster[self.embeddings_to_cluster],
            read_function='pd_parquet',
            cache_locally=True,
        )
        self.l_ix_subs = ['subreddit_name', 'subreddit_id']
        self.l_ix_post = ['subreddit_name', 'subreddit_id', 'post_id']
        self.l_cols_embeddings = [c for c in df_embeddings.columns if c.startswith('embeddings_')]

        if self.n_sample_embedding_rows is not None:
            log.info(f"  SAMPLING n_rows: {self.n_sample_embedding_rows}")
            df_embeddings = df_embeddings.sample(n=self.n_sample_embedding_rows)

        r_, c_ = df_embeddings.shape
        log.info(f"{r_:9,.0f} | {c_:5,.0f} <- df_embeddings SHAPE")
        mlflow.log_metrics(
            {'input_embeddings-n_rows': r_,
             'input_embeddings-n_cols': c_}
        )
        return df_embeddings

    def _load_metadata_for_filtering(self) -> pd.DataFrame:
        """Load metadata to filter embeddings"""
        pass
        log.warning(f"** Loading metadata NOT IMPLEMENTED! **")
        # TODO(djb)
        return LoadSubreddits(
            bucket_name=self.dict_data_text_and_metadata['bucket_name'],
            folder_path=self.dict_data_text_and_metadata['folder_subreddits_text_and_meta'],
            folder_posts=self.dict_data_text_and_metadata['folder_posts_text_and_meta'],
            columns=None,
            # col_new_manual_topic=col_manual_labels,
        ).read_apply_transformations_and_merge_post_aggs(
            cols_post='post_count_only_',
            df_format='dask',
            read_fxn='dask',
            unique_check=False,
        )

    def _apply_filtering(
            self,
            df_embeddings: pd.DataFrame,
            df_subs: pd.DataFrame,
    ) -> pd.DataFrame:
        """If we have kwargs for filtering, filter out embeddings
        example: if a sub only has 2 or 3 posts, then we're not going to trust
        those embeddings very much.
        """
        log.info(f"** Applying filters... **")
        col_filter_ = self.dict_filter_embeddings['filter_subreddits']['filter_column']
        min_value = self.dict_filter_embeddings['filter_subreddits']['minimum_column_value']
        log.info(f"  {col_filter_} >= {min_value}")

        df_embeddings = (
            df_subs[self.l_ix_subs + [col_filter_]]
            .merge(
                df_embeddings,
                how='right',
                on=self.l_ix_subs,
            )
        )
        mask_above_threshold = df_embeddings[col_filter_] >= min_value
        log.info(f"  Subreddits to drop: {(~mask_above_threshold).sum():,.0f}")

        r_, c_ = df_embeddings[mask_above_threshold].shape
        log.info(f"{r_:9,.0f} | {c_:5,.0f} <- df_embeddings SHAPE FILTERED")
        mlflow.log_metrics(
            {'filtered_embeddings-n_rows': r_,
             'filtered_embeddings-n_cols': c_}
        )
        return df_embeddings[mask_above_threshold]

    def _log_pipeline_to_mlflow(self):
        """Set schema so we can og the mlflow model"""
        log.info(f"Getting model signature...")
        # TODO(djb): add schema
        log.warning(f" Model-SCHEMA is currently null")
        # We can't infer the signature b/c clustering models don't usually have a .predict() method
        # signature = infer_signature(
        #     df_embeddings[self.l_cols_embeddings].iloc[:20, :],
        #     self.pipeline.predict(df_embeddings[self.l_cols_embeddings].iloc[:20, :])
        # )

        log.info(f"  Logging model to mlflow...")
        mlflow.sklearn.log_model(
            self.pipeline, "clustering_model",
            # signature=signature
        )

    def _get_linkage_and_optimal_ks(self):
        """Get model linkage, log it, & find optimal K-values"""
        try:
            log.info(f"-- Get linkage matrix for model --")
            self.X_linkage = create_linkage_for_dendrogram(
                self.pipeline.steps[-1][1]
            )

        except Exception as e:
            log.error(f"Model might not be Hierarchical:\n  {e}")

        try:
            log.info(f"-- Get optimal k-values --")
            fig = plt.figure(figsize=(14, 8))
            self.df_accel, self.optimal_ks = plot_elbow_and_get_k(
                self.X_linkage,
                n_clusters_to_check=600,
                return_optimal_ks=True,
            )
            plt.savefig(
                self.path_local_model_figures / (
                    f"elbow_diagram.png"
                ),
                dpi=200, bbox_inches='tight', pad_inches=0.2
            )
            plt.show()  # show AFTER saving, otherwise we'll save an empty plot
            plt.close(fig); del fig
            mlflow.log_artifacts(self.path_local_model_figures, 'figures')
            # Save figure
            folder_ = 'df_accel'
            folder_full_ = self.path_local_model / folder_
            save_pd_df_to_parquet_in_chunks(
                df=self.df_accel,
                path=folder_full_,
                write_index=True,
            )
            self.df_accel.to_csv(folder_full_ / 'df_accel.csv',
                                 index=False)
            mlflow.log_artifacts(folder_full_, artifact_path=folder_)

            # Save optimal k's, to mlflow & as artifacts
            for k_, d_ in self.optimal_ks.items():
                mlflow.log_metric(
                    f"optimal_k-{k_}",
                    d_['k']
                )

            folder_ = 'optimal_ks'
            log.info(f"  Saving optimal_ks: {folder_}")
            folder_full_ = self.path_local_model / folder_
            Path(folder_).mkdir(exist_ok=True, parents=True)
            pd.DataFrame(self.optimal_ks).T.to_parquet(
                path=folder_full_ / 'optimal_ks.parquet',
            )
            pd.DataFrame(self.optimal_ks).T.to_csv(
                folder_full_ / 'optimal_ks.csv',
                index=True,
            )
            mlflow.log_artifacts(folder_full_, artifact_path=folder_)

        except Exception as e:
            log.error(f"Elbow method failed:\n  {e}")

        try:
            log.info(f"  Creating dendrograms...")
            p_vals = {40}
            try:
                p_vals.add(
                    int(self.optimal_ks['050_to_100']['k'] + 1)
                )
                p_vals.add(
                    int(self.optimal_ks['100_to_200']['k'] + 1)
                )
            except Exception as e:
                log.error(f"  {e}")

            for p_ in tqdm(p_vals):
                log.info(f"  {p_}")
                fig = plt.figure(figsize=(14, 8))
                truncate_mode_ = 'lastp'
                fancy_dendrogram(
                    self.X_linkage,
                    plot_title='Clustering Algo',
                    annotate_above=self.X_linkage['distance'].quantile(q=0.985),
                    truncate_mode=truncate_mode_,
                    p=p_,
                    orientation='top',
                    show_leaf_counts=True, leaf_rotation=45,
                    show_contracted=False,
                )
                plt.savefig(
                    self.path_local_model_figures / (
                        f"dendrogram"
                        f"-truncate_mode_{truncate_mode_}"
                        f"-p_{p_}"
                        f".png"
                    ),
                    dpi=200, bbox_inches='tight', pad_inches=0.2
                )
                plt.show()
                plt.close(fig); del fig
            mlflow.log_artifacts(self.path_local_model_figures, 'figures')

        except Exception as e:
            log.error(f"Dendrogram failed:\n  {e}")


    def _get_cluster_ids_and_labels(
            self,
            df_embeddings: pd.DataFrame,
            df_subs: pd.DataFrame = None,
    ):
        """For a number of K-values, we need to get the labels"""
        if df_subs is not None:
            # try to add other columns from the df_subs meta
            l_cols_ground_truth = [
                # 'rating_name',  # ignore rating, it's useles/noisy for clustering
                'primary_topic',
            ]
            l_cols_to_add = l_cols_ground_truth + [
                'posts_for_modeling_count',
            ]
            l_cols_to_add = [c for c in l_cols_to_add if c in df_subs.columns]

            self.df_labels_ = (
                df_embeddings[self.l_ix_subs]
                .merge(
                    df_subs[self.l_ix_subs + l_cols_to_add],
                    how='left',
                    on=self.l_ix_subs,
                )
            ).copy()
        else:
            self.df_labels_ = df_embeddings[self.l_ix_subs].copy()

        s_k_to_evaluate = set(np.arange(10, 260, 10))
        # add the 'optimal' k_ values:
        col_optimal_k = 'optimal_k_for_interval'
        for interval_ in self.df_accel[col_optimal_k].dropna().unique():
            s_k_to_evaluate.add(self.df_accel[self.df_accel[col_optimal_k] == interval_]['k'].values[0])

        log.info(f"Get cluster IDs for each designated k...")
        for k_ in tqdm(sorted(s_k_to_evaluate)):
            self.df_labels_[f"{k_:03d}_k_labels"] = fcluster(self.X_linkage, k_, criterion='maxclust')

        log.info(self.df_labels_.shape)

        if df_subs is not None:
            # get named labels for each of the cols in l_cols_ground_truth
            d_df_crosstab_labels = dict()
            d_metrics = dict()
            l_cols_predicted = list()
            l_metrics_for_df = list()
            val_fill_pred_nulls = 'Meta/Reddit'

            log.info(f"-- Get true labels & metrics --")
            for col_cls_labels in tqdm([c for c in self.df_labels_.columns if '_k_labels' in c], mininterval=.8, ):
                k_int = int(col_cls_labels.split('_k_')[0])
                k_col_prefix = col_cls_labels.replace('_labels', '')
                log.info(f"  k: {k_col_prefix}")

                d_df_crosstab_labels[col_cls_labels] = dict()
                d_metrics[col_cls_labels] = dict()

                for c_tl in l_cols_ground_truth:
                    # to be on the safe side, sometimes nulls are filled as "null"
                    mask_not_null_gt = ~(
                            (self.df_labels_[c_tl].isnull()) |
                            (self.df_labels_[c_tl] == 'null')
                    )
                    d_df_crosstab_labels[col_cls_labels][c_tl] = pd.crosstab(
                        self.df_labels_[mask_not_null_gt][col_cls_labels],
                        self.df_labels_[mask_not_null_gt][c_tl]
                    )

                    # Create new predicted column
                    col_pred_ = f"{k_col_prefix}-predicted-{c_tl}"
                    l_cols_predicted.append(col_pred_)
                    self.df_labels_ = self.df_labels_.merge(
                        (
                            d_df_crosstab_labels[col_cls_labels][c_tl]
                            .idxmax(axis=1)
                            .to_frame()
                            .rename(columns={0: col_pred_})
                        ),
                        how='left',
                        left_on=col_cls_labels,
                        right_index=True,
                    )

                    # Should be rare, but fill just in case?
                    # self.df_labels_[col_pred_] = self.df_labels_[col_pred_].fillna(val_fill_pred_nulls)

                    # =====================
                    # Calculate metrics:
                    # ===
                    #         print(
                    #             classification_report(
                    #                 y_true=self.df_labels_[mask_not_null_gt][c_tl],
                    #                 y_pred=self.df_labels_[mask_not_null_gt][col_pred_],
                    #                 zero_division=0,
                    #             )
                    #         )

                    d_metrics_this_split = {
                        'predicted_col': col_cls_labels,
                        'truth_col': c_tl,
                        'k': k_int,
                    }
                    for m_name, metric_ in D_CLUSTER_METRICS_WITH_KNOWN_LABELS.items():
                        try:
                            d_metrics_this_split[m_name] = metric_(
                                y_true=self.df_labels_[mask_not_null_gt][c_tl],
                                y_pred=self.df_labels_[mask_not_null_gt][col_pred_],
                            )
                        except TypeError:
                            d_metrics_this_split[m_name] = metric_(
                                labels_true=self.df_labels_[mask_not_null_gt][c_tl],
                                labels_pred=self.df_labels_[mask_not_null_gt][col_pred_],
                            )
                    l_metrics_for_df.append(d_metrics_this_split)

            self.df_supervised_metrics_ = pd.DataFrame(l_metrics_for_df)
            log.info(f"{self.df_supervised_metrics_.shape} <- df_supervised metrics shape")

            folder_ = 'df_supervised_metrics'
            folder_full_ = self.path_local_model / folder_
            save_pd_df_to_parquet_in_chunks(
                df=self.df_supervised_metrics_,
                path=folder_full_,
                write_index=True,
            )
            self.df_supervised_metrics_.to_csv(
                folder_full_ / f'{folder_}.csv',
                index=False
            )
            joblib.dump(d_df_crosstab_labels,
                        folder_full_ / 'd_df_crosstab_labels.gzip'
                        )
            mlflow.log_artifacts(folder_full_, artifact_path=folder_)

        folder_ = 'df_labels'
        folder_full_ = self.path_local_model / folder_
        save_pd_df_to_parquet_in_chunks(
            df=self.df_labels_,
            path=folder_full_,
            write_index=True,
        )
        self.df_labels_.to_csv(folder_full_ / f'{folder_}.csv',
                               index=False)
        mlflow.log_artifacts(folder_full_, artifact_path=folder_)

    def _set_path_local_model(self):
        """Set where to save artifacts locally for this model"""
        if os.getcwd() != get_original_cwd():
            # hydra takes care of creating a custom working directory
            log.info(f"Using hydra's path")
            print(f"  Current working directory : {os.getcwd()}")
            print(f"  Orig working directory    : {get_original_cwd()}")
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

        # TODO(djb): instead of manually logging everything, use vars(self)
        #  to get all params & filter out:
        #  - things that start with `df_`
        #  - things named `mlf` (it's an mlflowLogger object)
        self.config_to_log_and_store = dict()
        for k_, v_ in vars(self).items():
            try:
                if any([k_.startswith('df_'), k_ == 'mlf', k_ == 'pipeline']):
                    # skip dataframes & some objects that aren't params
                    continue
                elif k_ == 'config_to_log_and_store':
                    # skip this config file b/c it can lead to weird nested recursion
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


#
# ~ fin
#
