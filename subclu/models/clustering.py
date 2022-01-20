"""
Module to cluster embeddings that have already been aggregated

NOTE when using hydra (CLI tool)
To avoid relative import errors when running from CLI, run script with:
 * -m flag
 * no ".py" ending

Example:
python -m subclu.test.test_parallel_jobs
python -m subclu.models.clustering mlflow_experiment_name="v0.4.0_use_multi_clustering_test"

"""
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
from typing import Union

import joblib
import mlflow
import mlflow.sklearn

import hydra
from hydra.utils import get_original_cwd
from omegaconf import DictConfig

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from scipy.cluster.hierarchy import fcluster, leaves_list
from sklearn.pipeline import Pipeline

from ..utils.tqdm_logger import LogTQDM
from ..utils import mlflow_logger
from ..utils.mlflow_logger import (
    MlflowLogger,
    save_df_and_log_to_mlflow,
)
from ..utils import get_project_subfolder
from ..utils.eda import elapsed_time
from ..data.data_loaders import LoadSubreddits  # , LoadPosts
from ..utils.ml_metrics import (
    log_precision_recall_fscore_support,
    log_classification_report_and_confusion_matrix,
)
from .clustering_utils import (
    create_linkage_for_dendrogram, fancy_dendrogram,
    plot_elbow_and_get_k
)
from .clustering_registry import (
    D_CLUSTER_MODELS, D_CLUSTER_PIPELINE,
    D_CLUSTER_METRICS_WITH_KNOWN_LABELS
)


log = logging.getLogger(__name__)


@hydra.main(config_path='../config', config_name="clustering_v0.4.1_subreddit_base")
def cluster_embeddings(
        cfg: DictConfig,
        return_object: bool = False
) -> Union[None, object]:
    """
    The hydra runner will call the clustering class and apply all the needed
    hyperparameters
    Note: by default we DO NOT return the cluster object because at some point around
       December something changed and we started getting a joblib/hydra error

    Args:
        cfg: hydra/omegaconf dictionary configuration

        return_object:
            whether to return the clustering object. By default, set to False
            because setting to True can result in errors when doing multi-run

    Returns:
        By default, set to False
            because setting to True can result in errors when doing multi-run
    """
    print(f"CFG keys: {cfg.keys()}")

    log.info(f"Define cluster class...")
    cluster = ClusterEmbeddings(
        data_embeddings_to_cluster=cfg['data_embeddings_to_cluster'],
        clustering_algo=cfg['clustering_algo'],
        data_text_and_metadata=cfg['data_text_and_metadata'],
        embeddings_to_cluster=cfg['embeddings_to_cluster'],
        mlflow_experiment_name=cfg['mlflow_experiment_name'],
        mlflow_tracking_uri=cfg.get('mlflow_tracking_uri', 'sqlite'),
        n_max_clusters_to_check_for_optimal_k=cfg.get('n_max_clusters_to_check_for_optimal_k', 2200),
        n_sample_embedding_rows=cfg.get('n_sample_embedding_rows', None),
        mlflow_run_name=(
            f"{cfg.get('mlflow_run_name', 'embedding_clustering')}-{datetime.utcnow().strftime('%Y-%m-%d_%H%M%S')}"
        ),
        filter_embeddings=cfg.get('filter_embeddings', None),
        pipeline_config=cfg.get('pipeline', cfg.get('pipeline_config', None)),
        logs_path=cfg.get('logs_path', 'logs'),
    )

    cluster.run_clustering()

    if return_object:
        return cluster

    """
    Error from hydra/joblib in multi-run details.
    Fix for now by returning None (nothing)
    ```
    Traceback (most recent call last):
      File "/opt/conda/lib/python3.7/site-packages/hydra/_internal/utils.py", line 211, in run_and_report
        return func()
      File "/opt/conda/lib/python3.7/site-packages/hydra/_internal/hydra.py", line 139, in multirun
        ret = sweeper.sweep(arguments=task_overrides)
      File "/opt/conda/lib/python3.7/site-packages/hydra/_internal/core_plugins/basic_sweeper.py", line 157, in sweep
        results = self.launcher.launch(batch, initial_job_idx=initial_job_idx)
      File "/opt/<same_as_above>/site-packages/hydra_plugins/hydra_joblib_launcher/joblib_launcher.py", line 46, in launch
      ...
      File "/opt/conda/lib/python3.7/concurrent/futures/_base.py", line 435, in result
        return self.__get_result()
      File "/opt/conda/lib/python3.7/concurrent/futures/_base.py", line 384, in __get_result
        raise self._exception
      TypeError: can't pickle _thread.RLock objects
    ```
    """


class ClusterEmbeddings:
    """
    Class to orchestrate different strategies to cluster embeddings
    - post-aggregates (e.g., post + comment) and
    - subreddit (e.g., post + comment + subreddit descriptions).
    """
    def __init__(
            self,
            data_embeddings_to_cluster: dict,
            clustering_algo: dict,
            data_text_and_metadata: dict,
            mlflow_experiment_name: str,
            embeddings_to_cluster: str = 'df_sub_level_agg_c_post_comments_and_sub_desc',
            n_sample_embedding_rows: int = None,
            n_max_clusters_to_check_for_optimal_k: int = 2200,
            mlflow_tracking_uri: str = 'sqlite',
            mlflow_run_name: str = 'embedding_clustering',
            pipeline_config: dict = None,
            filter_embeddings: dict = None,
            logs_path: str = 'logs',
            col_model_leaves_order: str = 'model_sort_order',
            # **kwargs
    ):
        """"""
        self.data_embeddings_to_cluster = data_embeddings_to_cluster
        self.clustering_algo = clustering_algo
        self.data_text_and_metadata = data_text_and_metadata

        self.embeddings_to_cluster = embeddings_to_cluster
        self.n_sample_embedding_rows = n_sample_embedding_rows
        self.n_max_clusters_to_check_for_optimal_k = n_max_clusters_to_check_for_optimal_k

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
        self.filter_embeddings = filter_embeddings

        # attributes to save outputs
        self.df_accel = None
        self.optimal_ks = None
        self.col_model_leaves_order = col_model_leaves_order

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

            if self.filter_embeddings is not None:
                if self.filter_embeddings.get('filter_subreddits', False):
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
            self.mlf.log_ram_stats(param=False, only_memory_used=True)

            mlflow_logger.log_pipeline_params(
                self.pipeline,
                save_path=self.path_local_model,
            )

            self._log_pipeline_to_mlflow()

            # Linkage and optimal Ks only applies to hierarchical clustering
            # TODO(djb): need a different method when using a different cluster type
            self._get_linkage_and_optimal_ks()
            self.mlf.log_ram_stats(param=False, only_memory_used=True)

            # Use the majority label for a cluster to compute supervised metrics
            #  We can use these to compare and find the "best" clusters:
            #  - adjusted metrics (rand index, mutual info)
            self._get_cluster_ids_and_labels_and_supervised_metrics(
                df_embeddings=df_embeddings,
                df_subs=df_subs,
            )

            # TODO(djb): Get unsupervised metrics to compare models & k: Silhouette

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
            log.info(f"--- END clustering metrics---")

            log.info(f"  Uploading logs to mlflow...")
            try:
                self.mlf.log_ram_stats(param=False, only_memory_used=True)
                l_logs = list(Path(os.getcwd()).glob('*.log'))
                for f_ in l_logs:
                    try:
                        mlflow.log_artifact(str(f_))
                    except Exception as e:
                        log.error(f"Couldn't log file: {f_}\n  {e}")
            except Exception as er:
                logging.error(f" Could not upload logs to mlflow {er}")

            # no need for mlflow.end_run() because we're running in a `with ...` block context

        # Remove fileHandler to prevent edge case: one file captures logs from multiple runs
        self._remove_file_logger()

        logging.info(f"=== END clustering full job ===")

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
        cls = D_CLUSTER_MODELS[self.clustering_algo['model_name']](
            **self.clustering_algo['model_kwargs']
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
            run_id=self.data_embeddings_to_cluster['run_uuid'],
            artifact_folder=self.data_embeddings_to_cluster[self.embeddings_to_cluster],
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
        self.mlf.log_ram_stats(param=False, only_memory_used=True)
        return df_embeddings

    def _load_metadata_for_filtering(self) -> pd.DataFrame:
        """Load metadata to filter embeddings"""
        log.warning(f"** Loading metadata **")
        return LoadSubreddits(
            bucket_name=self.data_text_and_metadata['bucket_name'],
            folder_path=self.data_text_and_metadata['folder_subreddits_text_and_meta'],
            folder_posts=self.data_text_and_metadata['folder_posts_text_and_meta'],
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
        col_filter_ = self.filter_embeddings['filter_subreddits']['filter_column']
        min_value = self.filter_embeddings['filter_subreddits']['minimum_column_value']
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
        self.mlf.log_ram_stats(param=False, only_memory_used=True)
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
            save_df_and_log_to_mlflow(
                df=self.X_linkage,
                path=self.path_local_model,
                subfolder='X_linkage',
                index=False,
            )
        except Exception as e:
            log.error(f"Model might not be Hierarchical:\n  {e}")

        try:
            log.info(f"-- Get optimal k-values --")
            fig = plt.figure(figsize=(14, 8))
            self.df_accel, self.optimal_ks = plot_elbow_and_get_k(
                self.X_linkage,
                n_clusters_to_check=self.n_max_clusters_to_check_for_optimal_k,
                return_optimal_ks=True,
            )
            plt.savefig(
                self.path_local_model_figures / (
                    f"elbow_diagram.png"
                ),
                dpi=200, bbox_inches='tight', pad_inches=0.2
            )
            # plt.show()  # show AFTER saving, otherwise we'll save an empty plot
            plt.close(fig)
            del fig
            mlflow.log_artifacts(self.path_local_model_figures, 'figures')

            save_df_and_log_to_mlflow(
                df=self.df_accel,
                path=self.path_local_model,
                subfolder='df_accel',
                index=False,
            )

            # Save optimal k's, to mlflow & as artifacts
            for k_, d_ in self.optimal_ks.items():
                mlflow.log_metric(
                    f"optimal_k-{k_}",
                    d_['k']
                )

            save_df_and_log_to_mlflow(
                df=pd.DataFrame(self.optimal_ks).T,
                path=self.path_local_model,
                subfolder='optimal_ks',
                index=True,
            )

        except Exception as e:
            log.error(f"Elbow method failed:\n  {e}")

        try:
            log.info(f"  Creating dendrograms...")
            p_vals = {40}
            try:
                p_vals.add(
                    int(self.optimal_ks['0060_to_0080']['k'] + 1)
                )
                p_vals.add(
                    int(self.optimal_ks['0100_to_0200']['k'] + 1)
                )
            except Exception as e:
                log.error(f"  {e}")

            for p_ in LogTQDM(p_vals):
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
                        f"-p_{p_:03d}"
                        f".png"
                    ),
                    dpi=200, bbox_inches='tight', pad_inches=0.2
                )
                # plt.show()
                plt.close(fig)
                del fig
            mlflow.log_artifacts(self.path_local_model_figures, 'figures')

        except Exception as e:
            log.error(f"Dendrogram failed:\n  {e}")

    def _get_cluster_ids_and_labels_and_supervised_metrics(
            self,
            df_embeddings: pd.DataFrame,
            df_subs: pd.DataFrame = None,
    ):
        """For a number of K-values, we need to get the labels"""
        # try to add other columns from the df_subs meta
        l_cols_ground_truth = [
            # 'rating_name',  # ignore rating, it's noisy for clustering
            'primary_topic',
        ]
        l_cols_to_add = l_cols_ground_truth + [
            'posts_for_modeling_count',
        ]
        if df_subs is not None:
            l_cols_to_add = [c for c in l_cols_to_add if c in df_subs.columns]

            self.df_labels_ = (
                df_embeddings[self.l_ix_subs]
                .merge(
                    df_subs[self.l_ix_subs + l_cols_to_add],
                    how='left',
                    on=self.l_ix_subs,
                )
                .reset_index(drop=True)  # MUST reset index for list_leaves merge to be accurate
            ).copy()
        else:
            self.df_labels_ = (
                df_embeddings[self.l_ix_subs]
                .reset_index(drop=True)  # MUST reset index for list_leaves merge to be accurate
                .copy()
            )

        try:
            log.info(f"  Add the model's sort order (distances) to df_labels")
            # NOTE: this join assumes that we've reset_index for df_labels_ before joining
            df_leaves_order = pd.DataFrame(
                {
                    self.col_model_leaves_order: range(len(self.df_labels_))
                },
                index=leaves_list(self.X_linkage),
            )
            self.df_labels_ = df_leaves_order.merge(
                self.df_labels_,
                how='right',
                left_index=True,
                right_index=True,
            )
        except Exception as e:
            log.error(f"Failed to append model leaves order\n  {e}")

        # for plots we'd like a smoother or more constant interval between k's,
        #  but for the final df_labels table (which will be uploaded to bigquery)
        #  we want fewer k's because each k will be a column and we want to make it easier
        #  for people to pick one k -- they could get overwhelmed if we give them a ton
        #  of columns and they wouldn't pick any of them
        s_k_to_evaluate = (
            set(np.arange(10, 100, 10)) |
            set(np.arange(100, 200, 25))
        )
        # increase interval between k's as we go bigger
        if 200 <= self.n_max_clusters_to_check_for_optimal_k:
            s_k_to_evaluate = (
                s_k_to_evaluate |
                set(np.arange(200, 1 + min(1000, self.n_max_clusters_to_check_for_optimal_k), 100))
            )
        if 1000 <= self.n_max_clusters_to_check_for_optimal_k:
            s_k_to_evaluate = (
                s_k_to_evaluate |
                set(np.arange(1000, 1 + min(3000, self.n_max_clusters_to_check_for_optimal_k), 250))
            )
        if 3000 <= self.n_max_clusters_to_check_for_optimal_k:
            s_k_to_evaluate = (
                s_k_to_evaluate |
                set(np.arange(3000, 1 + self.n_max_clusters_to_check_for_optimal_k, 200))
            )
        # explicitly add n_max clusters in case intervals missed it
        s_k_to_evaluate = s_k_to_evaluate | {self.n_max_clusters_to_check_for_optimal_k}

        # add the 'optimal' k_ values:
        for interval_, d_ in self.optimal_ks.items():
            s_k_to_evaluate.add(int(d_['k']))

        log.info(f"Get cluster IDs for each designated k...")
        label_col_prefix = 'k_'
        label_col_suffix = '_label'
        for k_ in LogTQDM(sorted(s_k_to_evaluate)):
            self.df_labels_[f"{label_col_prefix}{k_:04d}{label_col_suffix}"] = fcluster(self.X_linkage, k_, criterion='maxclust')

        log.info(self.df_labels_.shape)

        if df_subs is not None:
            # get named labels for each of the cols in l_cols_ground_truth
            d_df_crosstab_labels = dict()
            d_metrics = dict()
            l_cols_predicted = list()
            l_metrics_for_df = list()

            log.info(f"-- Get true labels & metrics --")
            # use list of optimal k's to log
            d_optimal_ks_lookup = dict()
            for interval_, d_ in self.optimal_ks.items():
                d_optimal_ks_lookup[d_['k']] = interval_

            # create folder to save supervised artifacts
            folder_ = 'df_supervised_metrics'
            folder_full_ = self.path_local_model / folder_
            folder_classifxn_ = 'df_classification_reports'
            folder_classifxn_full_ = self.path_local_model / folder_classifxn_

            for col_cls_labels in LogTQDM(
                    [c for c in self.df_labels_.columns if c.endswith(label_col_suffix)],
                    mininterval=.8,
                    desc='select k-values',
            ):
                k_int = int(col_cls_labels.replace(label_col_suffix, '').replace(label_col_prefix, ''))
                k_col_prefix = col_cls_labels.replace(label_col_suffix, '')
                # log.info(f"  k: {k_col_prefix}")

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
                    col_pred_ = f"{k_col_prefix}_majority_{c_tl}"
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

                    # store metrics for this k in this dict & combine all of them in a df later
                    d_metrics_this_split = {
                        'predicted_col': col_cls_labels,
                        'truth_col': c_tl,
                        'k': k_int,
                    }
                    # =====================
                    # Classification report
                    # ===
                    # Save confusion matrices & per-class metrics only for optimal K's
                    if k_int in d_optimal_ks_lookup.keys():
                        data_fold_name_ = f"{c_tl}-{d_optimal_ks_lookup[k_int]}"
                        log_clf_metrics_to_mlflow_ = True
                        log_classification_report_and_confusion_matrix(
                            y_true=self.df_labels_[mask_not_null_gt][c_tl],
                            y_pred=self.df_labels_[mask_not_null_gt][col_pred_],
                            data_fold_name=f"{c_tl}-{d_optimal_ks_lookup[k_int]}-{k_int}",
                            beta=1,
                            class_labels=None,
                            sort_labels_by_support=True,
                            save_path=folder_classifxn_full_,
                            log_metrics_to_console=False,
                            log_df_to_console=False,
                            log_metrics_to_mlflow=False,
                            log_artifacts_to_mlflow=False,
                            log_support_avg=True,
                            log_support_per_class=True,
                        )
                    else:
                        data_fold_name_ = f"{c_tl}-{k_int}"
                        log_clf_metrics_to_mlflow_ = False

                    # Save aggregate classification scores for all K's so we can compare & plot them
                    #  Only log to mlflow optimal k's (see check above)
                    d_metrics_this_split.update(
                        log_precision_recall_fscore_support(
                            y_true=self.df_labels_[mask_not_null_gt][c_tl],
                            y_pred=self.df_labels_[mask_not_null_gt][col_pred_],
                            data_fold_name=data_fold_name_,
                            append_fold_name_as_metric_prefix=True,
                            beta=1,
                            average='macro_and_weighted',
                            class_labels=None,
                            save_path=None,
                            log_metrics_to_console=False,
                            log_metrics_to_mlflow=log_clf_metrics_to_mlflow_,
                            log_artifacts_to_mlflow=False,
                            log_df_to_console=False,
                            log_support=False,
                            output_dict=True,
                            append_fold_name_to_output_dict=False,
                        )
                    )

                    # ===============
                    # Other metrics
                    # ===
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

                        # Only log to MLflow metrics for optimal Ks
                        if k_int in d_optimal_ks_lookup.keys():
                            mlflow.log_metric(
                                f"{c_tl}-{d_optimal_ks_lookup[k_int]}-{m_name}",
                                d_metrics_this_split[m_name]
                            )
                    l_metrics_for_df.append(d_metrics_this_split)

            self.df_supervised_metrics_ = pd.DataFrame(l_metrics_for_df)
            log.info(f"{self.df_supervised_metrics_.shape} <- df_supervised metrics shape")

            log.info(f"  Saving df_supervised_metrics...")
            save_df_and_log_to_mlflow(
                df=self.df_supervised_metrics_,
                path=self.path_local_model,
                subfolder=folder_,
                index=True,
            )
            joblib.dump(d_df_crosstab_labels,
                        folder_full_ / 'd_df_crosstab_labels.gzip'
                        )
            log.info(f"  Plotting supervised metrics...")
            for tc_val in self.df_supervised_metrics_['truth_col'].unique():
                df_plot = self.df_supervised_metrics_[
                    self.df_supervised_metrics_['truth_col'] == tc_val
                ]
                for metrics_group in ['_score', '_avg']:
                    self._plot_metric_scores(
                        df_plot=df_plot,
                        true_col=tc_val,
                        metric_cols_suffix=metrics_group,
                        save_path=folder_full_,
                    )

            # Log general supervised metrics
            mlflow.log_artifacts(folder_full_, artifact_path=folder_)

            # Log classification report dataframes
            mlflow.log_artifacts(folder_classifxn_full_, folder_classifxn_)

        save_df_and_log_to_mlflow(
            df=self.df_labels_,
            path=self.path_local_model,
            subfolder='df_labels',
            index=True,
        )

    def _plot_metric_scores(
            self,
            df_plot: pd.DataFrame,
            true_col: str,
            metric_cols_suffix: str,
            save_path: Path,
    ):
        """Plot metric scores at different k-values
        Since we'll be plotting a lot of different metrics, better to use it to split into
        two plots:
        - one for clustering metrics (homogeneity, adjusted mutual info, adjusted rand score)
        - another one for classification metrics (precision, recall, f1)
        """
        fig = plt.figure(figsize=(14, 8))

        if metric_cols_suffix == '_score':
            save_suffix = 'clustering'
        elif metric_cols_suffix == '_avg':
            save_suffix = 'classification'
        else:
            raise NotImplementedError

        # only plot cols known to end in a metric suffix
        metric_cols = [
            c for c in self.df_supervised_metrics_.columns if
            c.endswith(metric_cols_suffix)
        ]
        # legend order will be based on order that column was aded to plot
        #  We can make it easier to read by adding metrics in ascending order
        metric_cols = df_plot[metric_cols].max().sort_values(ascending=False).index.to_list()
        for c_ in metric_cols:
            sns.lineplot(data=df_plot, x='k', y=c_,
                         label=c_)

        plt.title(f"Scores for: {true_col}")
        plt.ylabel(f"score")
        plt.xlabel(f"k (number of clusters)")
        plt.savefig(
            save_path / (
                f"metrics_for_known_labels-{true_col}-{save_suffix}.png"
            ),
            dpi=200, bbox_inches='tight', pad_inches=0.2
        )
        plt.show()
        plt.close(fig)
        del fig

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
                    # skip itself config file b/c it can lead to weird nested recursion
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
        # TODO(djb): make sure to remove fileHandler after job completes run_aggregation()
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


if __name__ == "__main__":
    cluster_embeddings()


#
# ~ fin
#
