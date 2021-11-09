"""
Refactored  module to handle millions of comments & posts

This module focuses on aggregating embeddings AFTER they've been vectorized, for example
by functions from `models/vectorize_text.py`

Vectorize text > Aggregate embeddings > Compress embeddings | Cluster posts | Cluster subs
"""
from datetime import datetime, timedelta
import gc
from functools import partial
import logging
from logging import info
import math
from pathlib import Path
from typing import Tuple, Union, List

# config
import hydra
from hydra import initialize, compose
from omegaconf import OmegaConf, DictConfig

import mlflow
import dask.dataframe
from dask import dataframe as dd
import pandas as pd
import numpy as np
from tqdm import tqdm

# try modin instead of pandas?
# os.environ["MODIN_ENGINE"] = 'dask'
# # os.environ["MODIN_BACKEND"] = 'pandas'
# os.environ["MODIN_CPUS"] = "10"
# import modin.pandas as pd

from sklearn.metrics.pairwise import cosine_similarity

from ..data.data_loaders import LoadSubreddits, LoadPosts, LoadComments
from ..data.transform_distance_data_for_bq import reshape_distances_to_pairwise_bq
from ..utils.mlflow_logger import MlflowLogger, save_pd_df_to_parquet_in_chunks
from ..utils import mlflow_logger
from ..utils import get_project_subfolder
from ..utils.eda import elapsed_time, value_counts_and_pcts


class AggregateEmbeddings:
    """
    Class to orchestrate different strategies to aggregate embeddings from post & comment-level up to
    - post-aggregates (e.g., post + comment) and
    - subreddit (e.g., post + comment + subreddit descriptions).

    TODO(djb): open question: do we want to calculate distances in a separate job or do we calculate them here?
      could do it for subreddits as a demo, but might be better off doing it separately for posts given
      how many more there are.

    """
    def __init__(
            self,
            bucket_name: str = 'i18n-subreddit-clustering',
            folder_subreddits_text_and_meta: str = 'subreddits/de/2021-06-16',
            folder_comments_text_and_meta: str = 'comments/de/2021-06-16',
            folder_posts_text_and_meta: str = 'posts/de/2021-06-16',

            posts_uuid: str = 'db7a4d8aff04420eb4229d6499055e04',
            posts_folder: str = 'df_vect_posts',
            col_text_post_word_count: str = 'text_word_count',
            col_post_id: str = 'post_id',
            df_v_posts: pd.DataFrame = None,

            comments_uuid: str = 'db7a4d8aff04420eb4229d6499055e04',
            comments_folder: str = 'df_vect_comments',
            col_comment_id: str = 'comment_id',
            col_text_comment_word_count: str = 'comment_text_word_count',
            col_comment_text_len: str = 'comment_text_len',
            min_comment_text_len: int = 11,
            df_v_comments: pd.DataFrame = None,

            subreddit_desc_uuid: str = 'db7a4d8aff04420eb4229d6499055e04',
            subreddit_desc_folder: str = 'df_vect_subreddits_description',
            col_subreddit_id: str = 'subreddit_id',
            df_v_sub: pd.DataFrame = None,

            mlflow_experiment: str = 'use_multilingual_v1_aggregates',
            run_name: str = 'aggregate_embeddings_dask',
            mlflow_tracking_uri: str = 'sqlite',

            n_sample_posts_files: float = None,
            n_sample_comments_files: float = None,

            agg_comments_to_post_weight_col: str = 'comment_text_len',
            agg_post_post_weight: int = 70,
            agg_post_comment_weight: int = 20,
            agg_post_subreddit_desc_weight: int = 10,
            agg_post_to_subreddit_weight_col: str = None,

            df_subs_meta: pd.DataFrame = None,
            df_posts_meta: pd.DataFrame = None,
            df_comments_meta: pd.DataFrame = None,

            embeddings_read_fxn: callable = dd.read_parquet,
            metadata_read_fxn: callable = pd.read_parquet,
            calculate_similarites: bool = False,
            logs_path: str = 'logs/AggregateEmbeddings',
            unique_checks: bool = False,
            **kwargs
    ):
        """

        Args:
            bucket_name:
            folder_subreddits_text_and_meta:
            folder_comments_text_and_meta:
            folder_posts_text_and_meta:
            posts_uuid:
            posts_folder:
            col_text_post_word_count:
            col_post_id:
            df_v_posts:
            comments_uuid:
            comments_folder:
            col_comment_id:
            col_text_comment_word_count:
            col_comment_text_len:
            min_comment_text_len:
            df_v_comments:
            subreddit_desc_uuid:
            subreddit_desc_folder:
            col_subreddit_id:
            df_v_sub:
            mlflow_experiment:
            run_name:
            mlflow_tracking_uri:
            n_sample_posts_files:
            n_sample_comments_files:
            agg_comments_to_post_weight_col:
            agg_post_post_weight:
            agg_post_comment_weight:
            agg_post_subreddit_desc_weight:
            agg_post_to_subreddit_weight_col:
            df_subs_meta:
            df_posts_meta:
            df_comments_meta:
            embeddings_read_fxn:
            metadata_read_fxn:
            calculate_similarites:
            logs_path:
            unique_checks:
            **kwargs:
        """
        self.bucket_name = bucket_name
        self.folder_subreddits_text_and_meta = folder_subreddits_text_and_meta
        self.folder_comments_text_and_meta = folder_comments_text_and_meta
        self.folder_posts_text_and_meta = folder_posts_text_and_meta

        self.mlflow_experiment = mlflow_experiment
        self.run_name = run_name
        self.mlflow_tracking_uri = mlflow_tracking_uri

        self.df_v_posts = df_v_posts
        self.posts_uuid = posts_uuid
        self.posts_folder = posts_folder
        self.col_post_id = col_post_id
        self.col_text_post_word_count = col_text_post_word_count

        self.df_v_comments = df_v_comments
        self.comments_uuid = comments_uuid
        self.comments_folder = comments_folder
        self.col_comment_id = col_comment_id
        self.col_text_comment_word_count = col_text_comment_word_count
        self.col_comment_text_len = col_comment_text_len
        self.min_comment_text_len = min_comment_text_len

        self.df_v_sub = df_v_sub
        self.subreddit_desc_uuid = subreddit_desc_uuid
        self.subreddit_desc_folder = subreddit_desc_folder
        self.col_subreddit_id = col_subreddit_id

        self.n_sample_posts_files = n_sample_posts_files
        self.n_sample_comments_files = n_sample_comments_files

        # use pre-loaded metadata if running in interactive mode
        self.df_subs_meta = df_subs_meta
        self.df_posts_meta = df_posts_meta
        self.df_comments_meta = df_comments_meta

        # set columns & weights for rolling up weights
        # if None, all comments/posts get the same weight
        self.agg_comments_to_post_weight_col = agg_comments_to_post_weight_col
        self.agg_post_post_weight = agg_post_post_weight
        self.agg_post_comment_weight = agg_post_comment_weight
        self.agg_post_subreddit_desc_weight = agg_post_subreddit_desc_weight
        self.agg_post_to_subreddit_weight_col = agg_post_to_subreddit_weight_col

        self.embeddings_read_fxn = embeddings_read_fxn
        self.metadata_read_fxn = metadata_read_fxn

        # use as flag to know whether or not to calculate subredit similarities
        #  Set to False by default -- want to make similarity its own step
        self.calculate_similarites = calculate_similarites

        # Save logs here
        self.logs_path = logs_path

        # When sampling, set this to True to compute unique checks
        self.unique_checks = unique_checks

        # Create path to store local run
        self.path_local_model = None

        # Set mlflowLogger instance for central tracker
        self.mlf = MlflowLogger(tracking_uri=self.mlflow_tracking_uri)

        # Keep track of comments per post here
        self.df_comment_count_per_post = None

        # dfs with output of aggregates
        self.df_posts_agg_b = None
        self.df_posts_agg_c = None

        self.df_subs_agg_a = None
        self.df_subs_agg_b = None
        self.df_subs_agg_c = None

        # dfs with subreddit similarities
        self.df_subs_agg_a_similarity = None
        self.df_subs_agg_b_similarity = None
        self.df_subs_agg_c_similarity = None

        self.df_subs_agg_a_similarity_pair = None
        self.df_subs_agg_b_similarity_pair = None
        self.df_subs_agg_c_similarity_pair = None

    def _init_file_log(self) -> None:
        """Create a file & FileHandler to log data"""
        # TODO(djb): make sure to remove fileHandler after job is run_aggregation()
        if self.logs_path is not None:
            logger = logging.getLogger()

            path_logs = Path(self.logs_path)
            Path.mkdir(path_logs, parents=False, exist_ok=True)
            self.f_log_file = str(
                path_logs /
                f"{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_{self.run_name}.log"
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

    def run_aggregation(self) -> None:
        """Main function to run full aggregation job

        TODO(djb): Should I try to emulate fit, fit_transform, & transform methods from sklearn for this class?
          Because once I run it on some subset of data, I might need to apply it to new/unseen data
          Or is the plan to run the process/method from scratch every time?
          Need to think how this will work incrementally - e.g., if we run every week we don't need to
          re-run embeddings process on old posts, only need to update new posts (and give less weight to old posts)
          right?
        """
        # set & add logger for file
        self._init_file_log()
        t_start_agg_embed = datetime.utcnow()
        info(f"== Start run_aggregation() method ==")

        info(f"MLflow tracking URI: {mlflow.get_tracking_uri()}")
        self.mlf.set_experiment(self.mlflow_experiment)
        mlflow.start_run(run_name=self.run_name)
        self.mlf.add_git_hash_to_active_run()
        self.mlf.set_tag_hostname(key='host_name')
        self.mlf.log_param_hostname(key='host_name')
        self.mlf.log_cpu_count()
        self.mlf.log_ram_stats(param=True, only_memory_used=False)

        # create local path to store artifacts before logging to mlflow
        self.path_local_model = get_project_subfolder(
            f"data/models/aggregate_embeddings/{datetime.utcnow().strftime('%Y-%m-%d_%H%M%S')}-{self.run_name}"
        )
        Path(self.path_local_model).mkdir(exist_ok=True, parents=True)
        info(f"  Local model saving directory: {self.path_local_model}")

        # Log configuration so we can replicate run
        self._create_and_log_config()
        mlflow.log_params(self.config_to_log_and_store)

        # ---------------------
        # Load raw embeddings
        # ---
        self._load_raw_embeddings()
        self.mlf.log_ram_stats(only_memory_used=True)

        # ---------------------
        # Load metadata from files
        #   Needed if we're filtering or adding weights, examples:
        #   - text len
        #   - word count
        #   - date posted/created
        #   - up votes
        # ---
        # TODO(djb): re-enable after fixing other bugs &/or move its order?
        #  b/c we don't use it until later...
        self._load_metadata()
        self.mlf.log_ram_stats(only_memory_used=True)

        # Filter out short comments using metadata
        # ---
        # if self.min_comment_text_len is not None:
        #     info(f"{self.min_comment_text_len} <- Removing comments shorter than {self.min_comment_text_len} characters.")
        #     short_comments_to_remove = self.df_comments_meta[
        #         self.df_comments_meta[self.col_comment_text_len] <= self.min_comment_text_len
        #     ][self.col_comment_id]
        #
        #     self.df_v_comments = (
        #         self.df_v_comments
        #         [~(self.df_v_comments.index.get_level_values(self.col_comment_id).isin(short_comments_to_remove))]
        #     )
        #     info(f"  {self.df_v_comments.shape} <- df_v_comments.shape AFTER removing short comments")
        #     gc.collect()

        # ---------------------
        # Merge all comments at post-level
        # Weights by:
        # - text len or word count (how to account for emoji & ASCII art?)
        #     - Word count regex breaks b/c it doesn't work on non-latin alphabets
        # - up-votes
        # ---
        self._agg_comments_to_post_level()
        self.mlf.log_ram_stats(only_memory_used=True)

        # ---------------------
        # Merge at post-level basic
        #  - B) post + comments
        #  - C) post + comments + subreddit description
        # Weights by inputs, e.g., 70% post, 20% comments, 10% subreddit description
        # ---
        self._agg_posts_and_comments_to_post_level()
        self.mlf.log_ram_stats(only_memory_used=True)
        self._agg_posts_comments_and_sub_descriptions_to_post_level()
        self.mlf.log_ram_stats(only_memory_used=True)

        # ---------------------
        # TODO(djb): Merge at post-level with subreddit lag
        #  - D) post + comments + subreddit aggregate
        # After we calculate all post-level basic embeddings:
        # - For each day a subreddit has a post, calculate subreddit embeddings of previous N-days
        # TODO(djb) For any post-strategy above, also combine previous n-days of posts in a subreddit
        #  Similar to what Elliott does in semantic job, but in there it's limited to:
        #  - past 7-days
        #  - top 250 posts (by views?) for each subreddit
        #  Not sure those parameters would work well for subs that are not very active - like the
        #    ambassador communities or in non-English languages
        # ---

        # ---------------------
        # Merge at subreddit-level
        #  - A) posts only
        #  - B) posts + comments only
        #  - C) posts + comments + subreddit description
        # TODO(djb): Create weighted embeddings - Weights by:
        # - text len or word count (how to account for emoji & ASCII art?)
        #     - Word count regex breaks b/c it doesn't work on non-latin alphabets
        # - number of up-votes
        # - number of comments
        # - number of days since post was created (more recent posts get more weight)
        # ---
        self._agg_post_aggregates_to_subreddit_level()
        self.mlf.log_ram_stats(only_memory_used=True)
        # TODO(djb): break up (save & log fxn):
        #    save & log aggregates ASAP - this way I can start working on
        #    creating clusters w/o having to wait for distances to be computed
        # TODO(djb): log and save C) before B or A (C is what gives the best outputs)
        self._save_and_log_aggregate_and_similarity_dfs()
        self.mlf.log_ram_stats(only_memory_used=True)

        # ---------------------
        # Calculate subreddit similarity/distance
        # ---
        if self.calculate_similarites:
            self._calculate_subreddit_similarities()
            self.mlf.log_ram_stats(only_memory_used=True)

            self._save_and_log_aggregate_and_similarity_dfs()
            self.mlf.log_ram_stats(only_memory_used=True)

        # finish logging total time + end mlflow run
        total_fxn_time = elapsed_time(start_time=t_start_agg_embed, log_label='Total Agg fxn time', verbose=True)
        mlflow.log_metric('vectorizing_time_minutes',
                          total_fxn_time / timedelta(minutes=1)
                          )

        info(f"== COMPLETE run_aggregation() method ==")
        # TODO(djb): log file-log to mlflow
        self._send_log_file_to_mlflow()

        mlflow.end_run()
        info(f"    Removing fileHandler...")
        self._remove_file_logger()

    def _send_log_file_to_mlflow(self) -> None:
        """If log file exists, send it to MLFlow
        In case a job fails, it's helpful to have this stand-alone method to send the log-file.
        """
        if self.f_log_file is not None:
            try:
                if mlflow.active_run() is not None:
                    info(f"Logging log-file to mlflow...")
                    # TODO(djb): could I add the mlflow UUID as an attribute to this
                    #  object and reactivate it in case the run was killed?
                    mlflow.log_artifact(self.f_log_file)
                else:
                    info(f"Could NOT log to MLFLow, there's no active run.")
            except Exception as e:
                logging.warning(f"Error logging log-file: \n{e}")

    def _create_and_log_config(self):
        """Convert inputs into a dictionary we can save to replicate the run

        Don't log dfs with meta or raw embeddings! they could be dfs that take up gigs of storage
        """

        self.config_to_log_and_store = {
            'bucket_name': self.bucket_name,
            'folder_subreddits_text_and_meta': self.folder_subreddits_text_and_meta,
            'folder_comments_text_and_meta': self.folder_comments_text_and_meta,
            'folder_posts_text_and_meta': self.folder_posts_text_and_meta,

            'mlflow_experiment': self.mlflow_experiment,
            'run_name': self.run_name,
            'mlflow_tracking_uri': self.mlflow_tracking_uri,

            'posts_uuid': self.posts_uuid,
            'posts_folder': self.posts_folder,
            'col_post_id': self.col_post_id,
            'col_text_post_word_count': self.col_text_post_word_count,

            'comments_uuid': self.comments_uuid,
            'comments_folder': self.comments_folder,
            'col_comment_id': self.col_comment_id,
            'col_text_comment_word_count': self.col_text_comment_word_count,
            'min_comment_text_len': self.min_comment_text_len,

            'subreddit_desc_uuid': self.subreddit_desc_uuid,
            'subreddit_desc_folder': self.subreddit_desc_folder,
            'col_subreddit_id': self.col_subreddit_id,

            'n_sample_posts_files': self.n_sample_posts_files,
            'n_sample_comments_files': self.n_sample_comments_files,

            'agg_comments_to_post_weight_col': self.agg_comments_to_post_weight_col,
            'agg_post_post_weight': self.agg_post_post_weight,
            'agg_post_comment_weight': self.agg_post_comment_weight,
            'agg_post_subreddit_desc_weight': self.agg_post_subreddit_desc_weight,
            'agg_post_to_subreddit_weight_col': self.agg_post_to_subreddit_weight_col,
        }

        mlflow_logger.save_and_log_config(
            self.config_to_log_and_store,
            local_path=self.path_local_model,
            name_for_artifact_folder='config',
        )

    def _load_raw_embeddings(self):
        """Load raw embeddings if we don't receive pre-loaded embeddings
        Only use pre-loaded embeddings for testing!
        """
        info(f"-- Start _load_raw_embeddings() method --")
        active_run = mlflow.active_run()
        t_start_read_raw_embeds = datetime.utcnow()
        # ------------------------
        # Load and check SUBREDDITS
        # ---
        if self.df_v_sub is None:
            info(f"Loading subreddit description embeddings...")
            self.df_v_sub = self.mlf.read_run_artifact(
                run_id=self.subreddit_desc_uuid,
                artifact_folder=self.subreddit_desc_folder,
                read_function=self.embeddings_read_fxn,
                cache_locally=True,
            )
            # why are we tyring to drop `subreddit_id` all the time? to reduce compute or RAM?
            try:
                self.df_v_sub = self.df_v_sub.drop(self.col_subreddit_id, axis=1)
            except KeyError:
                pass
        else:
            info(f"Raw subreddit embeddings pre-loaded")
            # copy so that the internal object is different from the pre-loaded object
            self.df_v_sub = self.df_v_sub.copy()

        r_sub, c_sub = get_dask_df_shape(self.df_v_sub)
        info(f"  {r_sub:10,.0f} | {c_sub:4,.0f} <- Raw vectorized subreddit description shape")
        if active_run is not None:
            mlflow.log_metrics({'sub_description_raw_rows': r_sub, 'sub_description_raw_cols': c_sub})
        info(f"  Unique check for subreddit description...")
        assert (r_sub == self.df_v_sub['subreddit_name'].nunique().compute()), (f"** Index not unique. "
                                                                                f"Check duplicates df_v_sub **")

        # ------------------------
        # Load and check POSTS
        # ---
        if self.df_v_posts is None:
            info(f"Loading POSTS embeddings...")
            if self.n_sample_posts_files is not None:
                info(f"  Sampling POSTS FILES down to: {self.n_sample_posts_files:,.0f}")

            self.df_v_posts = self.mlf.read_run_artifact(
                run_id=self.posts_uuid,
                artifact_folder=self.posts_folder,
                read_function=self.embeddings_read_fxn,
                cache_locally=True,
                n_sample_files=self.n_sample_posts_files,
            )
            try:
                self.df_v_posts = self.df_v_posts.drop(self.col_subreddit_id, axis=1)
            except KeyError:
                pass
        else:
            info(f"POSTS embeddings pre-loaded")
            # copy so that the internal object is different from the pre-loaded object
            self.df_v_posts = self.df_v_posts.copy()

        info(f"  Getting df_v_posts.shape ...")
        r_post, c_post = get_dask_df_shape(self.df_v_posts)
        info(f"  {r_post:10,.0f} | {c_post:4,.0f} <- Raw POSTS shape")
        # Sampling only works reliably in pandas, it takes forever to compute in dask,
        #  so we only sample at file-level

        if active_run is not None:
            mlflow.log_metrics({'posts_raw_rows': r_post, 'posts_raw_cols': c_post})
        info(f"  Checking that posts are unique...")
        assert (r_post == self.df_v_posts[self.col_post_id].nunique().compute()), (f"** Post-ID NOT unique. "
                                                                                   f"Check duplicates df_v_posts **")

        # ------------------------
        # Load and check COMMENTS
        # ---
        if self.df_v_comments is None:
            info(f"Loading COMMENTS embeddings...")
            if self.n_sample_comments_files is not None:
                info(f"  Sampling COMMENTS FILES down to: {self.n_sample_comments_files:,.0f}")

            # If we get a list of multiple UUIDs, we need to:
            #   - load each mlflow UUID df independently
            #   - concat the two dask-dataframes
            if isinstance(self.comments_uuid, str):
                self.df_v_comments = self.mlf.read_run_artifact(
                    run_id=self.comments_uuid,
                    artifact_folder=self.comments_folder,
                    read_function=self.embeddings_read_fxn,
                    cache_locally=True,
                    n_sample_files=self.n_sample_comments_files,
                )
            else:
                info(f"  Found {len(self.comments_uuid)} run UUIDs with COMMENT embeddings...")
                if self.n_sample_comments_files is not None:
                    n_files_per_run = math.ceil(self.n_sample_comments_files / len(self.comments_uuid))
                    info(f"    Sampling {n_files_per_run} FILES per run UUID")
                else:
                    n_files_per_run = None

                self.df_v_comments = dd.concat(
                    [
                        self.mlf.read_run_artifact(
                            run_id=comm_uuid_,
                            artifact_folder=self.comments_folder,
                            read_function=self.embeddings_read_fxn,
                            cache_locally=True,
                            n_sample_files=n_files_per_run,
                        ) for comm_uuid_ in self.comments_uuid
                    ],
                    axis=0,
                )
            try:
                self.df_v_comments = self.df_v_comments.drop(self.col_subreddit_id, axis=1)
            except KeyError:
                pass
        else:
            info(f"COMMENTS embeddings pre-loaded")
            self.df_v_comments = self.df_v_comments.copy()
        # not worth computing the shape of comments, process & filter one at a time
        #  in another step
        # r_com_raw, c_com_raw = get_dask_df_shape(self.df_v_comments)
        # info(f"  {r_com_raw:10,.0f} | {c_com_raw:4,.0f} <- Raw COMMENTS shape")

        # No longer need to use index.get_level_values() b/c I reset_index() before saving
        #  But now need to use .compute() before .isin() b/c dask doesn't work otherwise...
        if self.n_sample_comments_files is not None:
            info(f"  Keep only comments for posts with embeddings")
            self.df_v_comments = self.df_v_comments[
                self.df_v_comments[self.col_post_id].isin(
                    self.df_v_posts[self.col_post_id].compute()
                 )
            ]

            if self.n_sample_comments_files <= 9:
                r_com, c_com = get_dask_df_shape(self.df_v_comments)
                info(f"  {r_com:11,.0f} | {c_com:4,.0f} <- COMMENTS shape, after keeping only existing posts")

        # if active_run is not None:
        #     mlflow.log_metrics({'comments_raw_rows': r_com, 'comments_raw_cols': c_com})
        # assert (r_com == self.df_v_comments[self.col_comment_id].nunique().compute()),
        # f"** Index not unique. Check duplicates df_v_comments **"

        # Set columns for index checking
        # Keep only one column for subreddit-level index
        #  because carrying around name & ID makes some things complicated and can take up a ton of memory
        #  we can always add subreddit_id at the end
        self.l_ix_sub_level = ['subreddit_name']
        self.l_ix_post_level = self.l_ix_sub_level + [self.col_post_id]
        self.l_ix_comment_level = self.l_ix_post_level + [self.col_comment_id]
        # The assumptions are:
        # - numeric cols that are not index cols are embeddings cols
        # - Embedding cols have the same names for all 3 sources
        self.l_embedding_cols = [c for c in self.df_v_comments.select_dtypes('number').columns if
                                 c not in [self.col_subreddit_id] + self.l_ix_comment_level]

        elapsed_time(start_time=t_start_read_raw_embeds, log_label='Total raw embeddings load', verbose=True)
        gc.collect()

    def _load_metadata(self):
        """Load metadata to filter comments or add weights based on metadata

        Read subs meta AFTER posts so that we can use post data to create aggregates.
        """
        info(f"-- Start _load_metadata() method --")
        t_start_read_meta = datetime.utcnow()

        if self.df_posts_meta is None:
            info(f"Loading POSTS metadata...")
            self.df_posts_meta = LoadPosts(
                bucket_name=self.bucket_name,
                folder_path=self.folder_posts_text_and_meta,
                columns='aggregate_embeddings_',
            ).read_and_apply_transformations()
        else:
            info(f"Posts META pre-loaded")
        info(f"  {self.df_posts_meta.shape} <- Raw META POSTS shape")

        if self.df_subs_meta is None:
            info(f"Loading subs metadata...")
            self.df_subs_meta = LoadSubreddits(
                bucket_name=self.bucket_name,
                folder_path=self.folder_subreddits_text_and_meta,
                folder_posts=self.folder_posts_text_and_meta,
                columns=None,
            ).read_apply_transformations_and_merge_post_aggs(df_posts=self.df_posts_meta)
        else:
            info(f"Subreddits META pre-loaded")
        info(f"  {self.df_subs_meta.shape} <- Raw META subreddit description shape")

        if self.df_comments_meta is None:
            info(f"Loading COMMENTS metadata...")
            self.df_comments_meta = LoadComments(
                bucket_name=self.bucket_name,
                folder_path=self.folder_comments_text_and_meta,
                columns='aggregate_embeddings_',
                df_format='dask',
            ).read_raw()
        else:
            info(f"Comments META pre-loaded")
        info(f"  {self.df_comments_meta.shape} <- Raw META COMMENTS shape")

        elapsed_time(start_time=t_start_read_meta, log_label='Total metadata loading', verbose=True)
        gc.collect()

    def _agg_comments_to_post_level(self):
        """We'll roll all comments to a post-level
        so we might have 4 comments (4 rows) to a post and at the end of this function we'll
        only have 1 row (1 post) that aggregates all comments for that row
        """
        info(f"-- Start _agg_comments_to_post_level() method --")
        gc.collect()
        # Check active run for each method because we don't know order of calls
        active_run = mlflow.active_run()
        t_start_agg_comments = datetime.utcnow()

        # Calculate comment count per post so we can know which posts to include & exclude
        #  from aggregation function (which can be expensive)
        self._calculate_comment_count_per_post()

        info(f"Filtering which comments need to be averaged...")
        mask_single_comments = self.df_v_comments['post_id'].isin(
            self.df_comment_count_per_post[self.df_comment_count_per_post[self.col_comment_count] == 1]
            ['post_id'].compute()
        )
        mask_single_comments_pandas = mask_single_comments.compute()
        n_comments_single = mask_single_comments_pandas.sum()
        n_comments_to_average = (~mask_single_comments_pandas).sum()
        info(f"  {n_comments_single:11,.0f} <- Comments that DON'T need to be averaged")
        info(f"  {n_comments_to_average:11,.0f} <- Comments that need to be averaged")
        try:
            mlflow.log_metrics(
                {'n_comments_single_no_agg_needed': n_comments_single,
                 'n_comments_multiple_agg_needed': n_comments_to_average,
                 'comments_raw_rows': n_comments_single + n_comments_to_average,
                 }
            )
        except Exception as e:
            logging.warning(f"Error logging metrics: {e}")

        if self.agg_comments_to_post_weight_col is None:
            info(f"No column to weight comments, simple mean for comments at post level")

            self.df_v_com_agg = dd.concat(
                [
                    # First, calculate average for posts with 2+ comments
                    self.df_v_comments
                    [~mask_single_comments]
                    .groupby(self.l_ix_post_level)
                    [self.l_embedding_cols]
                    .mean()
                    .reset_index(),
                    # And append the values for of single comments  (no agg needed)
                    self.df_v_comments
                    [mask_single_comments]
                    [self.l_ix_post_level + self.l_embedding_cols]
                ],
                interleave_partitions=True
            )

        else:
            # TODO(djb): add new calculation to get weighted average for comments
            raise NotImplementedError("Currently, only .mean() is implemented")


        # TODO(djb): Fix this check. Dask fails computing this check with len... try something else
        #  Instead of len:
        #       - count?
        #       - index count?
        #  or is it .nunique() that is failing?
        #   maybe simply iterate one subreddt at a time?
        #  pattern to try:
        #   - create an empty list where we'll store tuples to compare
        #   - loop through subreddits in post-level data
        #       - create a mask for df_v_com_agg for only comments that belong to the subreddit
        #       - using the mask, create a tuple:
        #           - unique posts in masked df
        #           - count of posts in masked df
        #        - in this loop check compute the values of the unique() and count() fxns
        #  Alternative: don't compute in that loop and create a 2nd loop where the tuples actually get compared
        r_com_agg = len(self.df_v_com_agg[self.col_post_id].compute())
        c_com_agg = len(self.df_v_com_agg.columns)
        if active_run is not None:
            mlflow.log_metrics({'df_v_com_agg_rows': r_com_agg, 'df_v_com_agg_cols': c_com_agg})
        if self.n_sample_comments_files is not None:
            if all([self.n_sample_comments_files <= 4, self.unique_checks]):
                logging.warning(f"Checking that index is unique after aggregation... [only when testing]")
                assert (r_com_agg == self.df_v_com_agg[self.col_post_id].nunique().compute()), "Index not unique"
        info(f"  {r_com_agg:11,.0f} | {c_com_agg:4,.0f} <- df_v_com_agg SHAPE")
        elapsed_time(start_time=t_start_agg_comments, log_label='Total comments to post agg loading', verbose=True)

    def _calculate_comment_count_per_post(self):
        """Calculate comment count per post if it hasn't been computed

        We should be able to use 'count' (which is faster) instead of 'nunique' because
         we checked for unique index after loading the dfs
        """
        if self.df_comment_count_per_post is None:
            info(f"Getting count of comments per post...")
            # dask does not support pandas' “named aggregation”,
            # so we need to calculate and then rename after calculation
            # https://github.com/dask/dask/issues/5294

            # First get count for posts with comments
            # hold off on calling .compute() until later so dask can optimize the DAG
            self.col_comment_count = 'comment_count'
            df_comment_count_per_post_only_1_or_more = (
                self.df_v_comments
                .groupby(self.l_ix_post_level)
                [self.col_comment_id].count()
                .reset_index()
                .rename(columns={self.col_comment_id: self.col_comment_count})
                # .compute()
            )

            info(f"Create MASK of posts with comments...")
            self.mask_posts_posts_with_comments = self.df_v_posts['post_id'].isin(
                self.df_v_comments['post_id'].compute()
            )

            # 2nd, add posts with zero comments
            #  Make sure to add the same columns & merge at the same index level!
            # Use concat instead of merge b/c it's less compute-intensive
            info(f"Concat dfs: comment_count>=1 & comment_count==0")
            self.df_comment_count_per_post = dd.concat(
                [
                    df_comment_count_per_post_only_1_or_more,
                    self.df_v_posts[~self.mask_posts_posts_with_comments][self.l_ix_post_level].assign(
                        **{self.col_comment_count: 0}
                    )
                ],
                axis=0,
            )

            if self.n_sample_comments_files is not None:
                if self.n_sample_comments_files <= 4:
                    # only compute for test runs, otherwise it wastes A LOT of compute & TIME
                    df_counts_summary = value_counts_and_pcts(
                        self.df_comment_count_per_post['comment_count'].compute(),
                        add_col_prefix=False,
                        count_type='posts',
                        reset_index=True,
                        sort_index=True,
                        return_df=True,
                        cumsum=False,
                     )

                    # dask alternative:
                    # s_val_counts = self.df_comment_count_per_post[col_comment_count].value_counts(normalize=True).head(10)
                    info(f"Comments per post summary:\n{df_counts_summary}")
                    del df_counts_summary

                    info(f"TESTING that all post IDs are counted in df_comment_count_per_post...")
                    set_post_ids_in_posts = set(self.df_v_posts[self.col_post_id].compute())
                    set_post_ids_comment_count = set(self.df_comment_count_per_post[self.col_post_id].compute())
                    test_set = set_post_ids_in_posts == set_post_ids_comment_count
                    info(f"  {test_set} <- Post IDs in df_v_posts == df_comment_count_per_post")
                    if not test_set:
                        logging.error(
                            f"  Post IDs ARE NOT EQUAL!"
                            f"\n    {len(set_post_ids_in_posts - set_post_ids_comment_count)} Posts - Comment count"
                            f"\n    {len(set_post_ids_comment_count - set_post_ids_in_posts)} Comment count - Posts"
                        )
                        raise Exception(f"Error calculating comment count per post")

            # Don't compute this now, wait for later when we need to create a mask to get IDs
            #  for posts & comments that need to be averaged
            # info(f"  {(self.df_comment_count_per_post['comment_count'].compute() >= 2).sum():10,.0f}"
            #      f" <- Posts with 2+ comments (total posts that need COMMENT weighted average)")

            gc.collect()

    def _agg_posts_and_comments_to_post_level(self):
        """roll up post & comment embeddings to post-level

        Single posts = posts where there's only one comment, so we don't need to calculate weights
        """
        info(f"-- Start (df_posts_agg_b) _agg_posts_and_comments_to_post_level() method --")
        # temp column to add averaging weights
        col_weights = '_col_method_weight_'

        t_start_method = datetime.utcnow()

        self._calculate_comment_count_per_post()

        # Create df with:
        #  - posts with 1+ comments
        #    - add new col with input weight
        #  - comments for posts
        #    - one row per post, these are already aggregated
        #    - add new col with input weight
        self.df_posts_for_weights = dd.concat(
            [
                self.df_v_posts[self.mask_posts_posts_with_comments].assign(
                    **{col_weights: self.agg_post_post_weight}
                ),
                self.df_v_com_agg.assign(
                    **{col_weights: self.agg_post_comment_weight}
                ),
             ],
            interleave_partitions=True
        )
        try:
            if self.n_sample_comments_files <= 4:
                # use alias instead of calculating again
                mask_posts_without_comments = ~self.mask_posts_posts_with_comments

                mask_posts_without_comments_pandas = mask_posts_without_comments.compute()
                info(f"  {mask_posts_without_comments_pandas.sum():11,.0f} <- Posts that DON'T need weighted average")
                info(f"  {(~mask_posts_without_comments_pandas).sum():11,.0f} <- Posts that need weighted average")

                r_weights, c_weights = get_dask_df_shape(self.df_posts_for_weights)
                info(f"  {r_weights:11,.0f} | {c_weights:4,.0f} <- Shape of df(posts+comments) to weight")
        except (ValueError, TypeError):
            pass

        info(f"DEFINE agg_posts_w_comments...")
        # When using dask, we don't need to manually iterate, we can let dask figure that out:
        # HOWEVER, when using multi-index, we need to add more metadata
        #  https://github.com/dask/dask/issues/6286
        # There's a weird bug with dask where after .apply(), it'll create an empty column called "index"
        #  So we need to drop it *sigh*
        df_expected_agg_output = pd.DataFrame(
            columns=self.l_embedding_cols,
            index=self.l_ix_post_level,
            dtype=np.float32,
        )
        # TODO(djb): I've run into memory errors with 16 ad 12 workers (and 520 GB RAM)
        #  would it be better if I computed this df in 2 steps:
        #  - create group of 2 (or 3) subreddit batches
        #  - concat the batches as 2 separate functions
        #  It's possible that the masking step could be expensive, but it might limit
        #  the amount of RAM needed by a single worker and allow to spread out the work
        self.ddf_agg_posts_w_comments = (
            self.df_posts_for_weights
            .groupby(self.l_ix_post_level)
            .apply(
                partial(
                    weighted_mean_for_groupby_np,
                    cols_to_avg=self.l_embedding_cols,
                    col_weights=col_weights,
                ),
                # meta={c: np.float32 for c in self.l_embedding_cols} # this gives us the weird index error
                meta=df_expected_agg_output,
            )
            .reset_index()
        )

        # info(f"Compute agg_posts_w_comments...")
        # df_agg_posts_w_comments = ddf_agg_posts_w_comments.compute()
        info(f"  {self.ddf_agg_posts_w_comments.shape} <- df_agg_posts_w_comments.shape (only posts with comments)")

        # Merge into a a single dataframe:
        # - posts w/ multiple comments (already averaged out)
        # - posts with 1 comment (no need for weights)
        # NVM for now: Sort because we want most posts for a subreddit in one file or
        #  adjacent files when we save to multiple dfs
        info(f"Concat aggregated comments+posts with posts-without comments")
        self.df_posts_agg_b = dd.concat(
            [
                self.ddf_agg_posts_w_comments,
                self.df_v_posts[~self.mask_posts_posts_with_comments]
            ],
            interleave_partitions=True,
            axis=0
        )
        # We need to drop this blank `index` column because of the weird dask .multi-index error
        try:
            self.df_posts_agg_b = self.df_posts_agg_b.drop(['index'], axis=1)
        except (ValueError, KeyError):
            pass

        r_agg_posts, c_agg_posts = get_dask_df_shape(self.df_posts_agg_b, col_len_check=self.col_post_id)
        info(f"  {r_agg_posts:11,.0f} | {c_agg_posts:4,.0f} <- df_posts_agg_b shape after aggregation")

        if self.n_sample_comments_files is not None:
            if all([self.n_sample_comments_files <= 4, self.unique_checks]):
                logging.warning(f"Unique check only applied when sampling/testing")
                assert (r_agg_posts == self.df_posts_agg_b[self.col_post_id].nunique().compute()), "Index not unique"

        elapsed_time(start_time=t_start_method, log_label='Total (df_posts_agg_b) posts & comments agg', verbose=True)

    def _agg_posts_comments_and_sub_descriptions_to_post_level(self):
        """roll up post & comment embeddings to post-level

        Single posts = posts where there's only one comment, so we don't need to calculate weights
        """
        info(f"-- Start (df_posts_agg_c) _agg_posts_comments_and_sub_descriptions_to_post_level() method --")
        t_start_method = datetime.utcnow()
        # temp column to add averaging weights
        col_weights = '_col_method_weight_'

        # instead of re-calculating post + comment weights, reuse it
        if self.df_posts_agg_b is None:
            self._agg_posts_and_comments_to_post_level()

        # In this case we'll always want to average each post with a sub, so no need
        # to filter out cases that don't need a weight

        # Create df with:
        #  - all posts that already include weight from comments
        #    - add new col with input weight
        #  - subreddit descriptions
        #    - create new df: one row per post, each row has the embeddings for the sub
        #    - add new col with input weight
        self.df_posts_for_weights = dd.concat(
            [
                self.df_posts_agg_b.assign(
                    **{col_weights: (self.agg_post_post_weight + self.agg_post_comment_weight)}
                ),
                (
                    self.df_posts_agg_b[self.l_ix_post_level].merge(
                        self.df_v_sub,
                        how='left',
                        on=self.l_ix_sub_level,
                    )
                    .reset_index()
                ).assign(
                    **{col_weights: self.agg_post_subreddit_desc_weight}
                ),
            ]
        )

        df_expected_agg_output = pd.DataFrame(
            columns=self.l_embedding_cols,
            index=[self.col_post_id],
            dtype=np.float32,
        )
        # With Dask, we don't need to manually iterate, it can manage the iterating for us
        self.df_agg_posts_w_sub = (
            self.df_posts_for_weights
            .groupby(self.col_post_id)
            .apply(
                partial(
                    weighted_mean_for_groupby_np,
                    cols_to_avg=self.l_embedding_cols,
                    col_weights=col_weights,
                ),
                meta=df_expected_agg_output,
            )
            .reset_index()
            # Renaming the `index` is a workaround for a dask failing to rename the
            #  actual index column ('post_id') column after .reset_index()
            .rename(columns={'index': self.col_post_id})
        )
        info(f"  {self.df_agg_posts_w_sub.shape} <- df_agg_posts_w_sub.shape (only posts with comments)")

        # Re-append index columns so it's the same in original and new output (but not as index)
        self.df_posts_agg_c = (
            self.df_agg_posts_w_sub
            .merge(
                self.df_v_posts[self.l_ix_post_level].drop_duplicates(),
                how='left',
                on=[self.col_post_id],
            )
            # .reset_index()
        )  # .sort_index()

        if self.n_sample_comments_files is not None:
            if all([self.n_sample_comments_files <= 5, self.unique_checks]):
                # Only calculate with fewer than 10 files, this might be taking up 30+ minutes!!
                r_agg_posts, c_agg_posts = get_dask_df_shape(self.df_posts_agg_c, col_len_check=self.col_post_id)
                info(f"  {r_agg_posts:11,.0f} | {c_agg_posts:4,.0f} <- df_posts_agg_c shape after aggregation")

                logging.warning(f"Unique check only applied when sampling/testing")
                assert (r_agg_posts == self.df_posts_agg_c[self.col_post_id].nunique().compute()), "Index not unique"

        elapsed_time(start_time=t_start_method, log_label='Total (df_posts_agg_c) posts+comments+subs agg', verbose=True)

    def _agg_post_aggregates_to_subreddit_level(self):
        """Roll up post-level aggregations to subreddit-level"""
        info(f"-- Start _agg_post_aggregates_to_subreddit_level() method --")
        t_start_method = datetime.utcnow()
        # temp column to add averaging weights
        col_weights = '_col_method_weight_'

        if self.agg_post_to_subreddit_weight_col is None:
            info(f"No column to weight comments, simple mean to roll up posts to subreddit-level...")

            # A - posts only
            info(f"A - posts only")
            self.df_subs_agg_a = (
                self.df_v_posts
                [self.l_ix_sub_level + self.l_embedding_cols]
                .groupby(self.l_ix_sub_level)
                .mean()
                .reset_index()
            )  # .sort_index()
            info(f"  {self.df_subs_agg_a.shape} <- df_subs_agg_a.shape (only posts)")

            # B - posts + comments
            info(f"B - posts + comments")
            self.df_subs_agg_b = (
                self.df_posts_agg_b
                [self.l_ix_sub_level + self.l_embedding_cols]
                .groupby(self.l_ix_sub_level)
                .mean()
                .reset_index()
            )  # .sort_index()
            info(f"  {self.df_subs_agg_b.shape} <- df_subs_agg_b.shape (posts + comments)")

            # C - posts + comments + sub descriptions
            info(f"C - posts + comments + sub descriptions")
            self.df_subs_agg_c = (
                self.df_posts_agg_c
                .reset_index()
                [self.l_ix_sub_level + self.l_embedding_cols]
                .groupby(self.l_ix_sub_level)
                .mean()
                .reset_index()
            )  # .sort_index()
            info(f"  {self.df_subs_agg_c.shape} <- df_subs_agg_c.shape (posts + comments + sub description)")

        else:
            raise NotImplementedError(f"Using weighted average (posts) to roll up to subreddits not implemented.")

        elapsed_time(start_time=t_start_method, log_label='Total for ALL subreddit-level agg', verbose=True)

    def _calculate_subreddit_similarities(self):
        """For each subreddit aggregation, calculate subreddit similarity/distances
        We want to do it with raw data/full embeddings to get most accurate similarity
        (instead of doing it after compression)
        """
        info(f"-- Start _calculate_subreddit_similarities() method --")
        t_start_method = datetime.utcnow()

        info(f"A...")
        ix_a = self.df_subs_agg_a['subreddit_name'].compute()
        self.df_subs_agg_a_similarity = pd.DataFrame(
            cosine_similarity(self.df_subs_agg_a[self.l_embedding_cols]),
            index=ix_a,
            columns=ix_a,
        )
        self.df_subs_agg_a_similarity.columns.name = None
        self.df_subs_agg_a_similarity.index.name = 'subreddit_name'
        info(f"  {self.df_subs_agg_a_similarity.shape} <- df_subs_agg_a_similarity.shape")

        # Keep the DF that has all the distances, not just the top
        self.df_subs_agg_a_similarity_pair, _ = reshape_distances_to_pairwise_bq(
            df_distance_matrix=self.df_subs_agg_a_similarity,
            df_sub_metadata=self.df_subs_meta,
            top_subs_to_keep=20,
        )
        del _
        gc.collect()

        info(f"B...")
        ix_b = self.df_subs_agg_b['subreddit_name'].compute()
        self.df_subs_agg_b_similarity = pd.DataFrame(
            cosine_similarity(self.df_subs_agg_b[self.l_embedding_cols]),
            index=ix_b,
            columns=ix_b,
        )
        self.df_subs_agg_b_similarity.columns.name = None
        self.df_subs_agg_b_similarity.index.name = 'subreddit_name'
        info(f"  {self.df_subs_agg_b_similarity.shape} <- df_subs_agg_b_similarity.shape")
        self.df_subs_agg_b_similarity_pair, _ = reshape_distances_to_pairwise_bq(
            df_distance_matrix=self.df_subs_agg_b_similarity,
            df_sub_metadata=self.df_subs_meta,
            top_subs_to_keep=20,
        )
        del _
        gc.collect()

        info(f"C...")
        ix_c = self.df_subs_agg_c['subreddit_name'].compute()
        self.df_subs_agg_c_similarity = pd.DataFrame(
            cosine_similarity(self.df_subs_agg_c[self.l_embedding_cols]),
            index=ix_c,
            columns=ix_c,
        )
        self.df_subs_agg_c_similarity.columns.name = None
        self.df_subs_agg_c_similarity.index.name = 'subreddit_name'
        info(f"  {self.df_subs_agg_c_similarity.shape} <- df_subs_agg_c_similarity.shape")
        self.df_subs_agg_c_similarity_pair, _ = reshape_distances_to_pairwise_bq(
            df_distance_matrix=self.df_subs_agg_c_similarity,
            df_sub_metadata=self.df_subs_meta,
            index_name='subreddit_name',
            top_subs_to_keep=20,
        )
        del _
        gc.collect()

        elapsed_time(start_time=t_start_method, log_label='Total for _calculate_subreddit_similarities()', verbose=True)

    def _save_and_log_aggregate_and_similarity_dfs(self):
        """use custom function to save dfs in multiple files & log them to mlflow"""
        info(f"-- Start _save_and_log_aggregate_and_similarity_dfs() method --")
        t_start_method = datetime.utcnow()
        # Merged at post-level
        #  - B) post + comments
        #  - C) post + comments + subreddit description

        # Merged at subreddit-level
        #  - A) posts only
        #  - B) posts + comments only
        #  - C) posts + comments + subreddit description

        # Subreddit-similarities AND pairs (same as subreddit-level)
        #  - A) posts only
        #  - B) posts + comments only
        #  - C) posts + comments + subreddit description

        d_dfs_to_save = {
            'df_sub_level_agg_c_post_comments_and_sub_desc': self.df_subs_agg_c,
            'df_sub_level_agg_c_post_comments_and_sub_desc_similarity': self.df_subs_agg_c_similarity,
            'df_sub_level_agg_c_post_comments_and_sub_desc_similarity_pair': self.df_subs_agg_c_similarity_pair,

            'df_sub_level_agg_b_post_and_comments': self.df_subs_agg_b,
            'df_sub_level_agg_b_post_and_comments_similarity': self.df_subs_agg_b_similarity,
            'df_sub_level_agg_b_post_and_comments_similarity_pair': self.df_subs_agg_b_similarity_pair,

            'df_sub_level_agg_a_post_only': self.df_subs_agg_a,
            'df_sub_level_agg_a_post_only_similarity': self.df_subs_agg_a_similarity,
            'df_sub_level_agg_a_post_only_similarity_pair': self.df_subs_agg_a_similarity_pair,

            'df_post_level_agg_b_post_and_comments': self.df_posts_agg_b,
            'df_post_level_agg_c_post_comments_sub_desc': self.df_posts_agg_c,
        }

        # create dict to make it easier to reload dataframes logged as artifacts
        # e.g., we should be able to get a list of the expected df subfolders even if we change the name of a folder
        d_dfs_folders_to_log = {k: k for k, v in d_dfs_to_save.items() if v is not None}
        info(f"Dictionary of dfs to log & save (only dfs that have been created):\n"
             f"{d_dfs_folders_to_log}")
        mlflow_logger.save_and_log_config(
            config=d_dfs_folders_to_log,
            local_path=self.path_local_model,
            name_for_artifact_folder='d_logged_dfs_subfolders',
        )

        # Only save dfs that have been created
        for folder_, df_ in tqdm({k: v for k, v in d_dfs_to_save.items() if v is not None}.items(),
                                 ascii=True, ncols=80, position=0):
            t_start_df_ = datetime.utcnow()

            path_sub_local = self.path_local_model / folder_
            info(f"** {folder_} **")
            if Path(path_sub_local).exists():
                info(f"  ** SKIPPING because path already exits **")
                continue
            else:
                info(f"  Saving locally...")

            # The assumption is that similarity DFs should be pandas DFs
            #  so we should be safe saving index for them
            # save_pd_df_to_parquet_in_chunks can handle either pd.dfs OR dd.dfs
            if folder_.endswith('_similarity'):
                info(f"  Keeping index intact...")
                save_pd_df_to_parquet_in_chunks(
                    df=df_,
                    path=path_sub_local,
                    write_index=True,
                )
            else:
                save_pd_df_to_parquet_in_chunks(
                    df=df_.reset_index(),
                    path=path_sub_local,
                    write_index=False,
                )

            if isinstance(df_, pd.DataFrame):
                info(f"  Logging df shape...")
                rows_, cols_ = df_.shape
                mlflow.log_metrics(
                    {f"{folder_}-rows": rows_,
                     f"{folder_}-cols": cols_,
                     }
                )
            else:
                # TODO(djb) run separate job to add dask df shape AFTER df is saved,
                #   otherwise it takes A LOT of extra compute to get len(df)
                info(f"  NOT logging shape of dask df to save time & compute")

            info(f"  Logging artifact to mlflow...")
            mlflow.log_artifacts(path_sub_local, artifact_path=folder_)
            elapsed_time(start_time=t_start_df_, log_label=f"Total for saving & logging ** {folder_} **",
                         verbose=True)

        elapsed_time(start_time=t_start_method, log_label='Total for _save_and_log_aggregate_and_similarity_dfs()',
                     verbose=True)


def get_dask_df_shape(
        ddf: dd.DataFrame,
        col_len_check: str = None,
) -> Tuple[int, int]:
    """
    Convenience wrapper around Dask DF to compute and return df shape
    Use it since I call .shape to log progress a few times and it's annoying
    to have to .compute() or check whether it was computed every time we call .shape
    """
    # Turns out that .shape can also run out of memory...
    #  index.size is faster and takes up less RAM
    # r_, c_ = ddf.shape

    c_ = len(ddf.columns)
    if col_len_check is None:
        return ddf.index.size.compute(), c_
    else:
        return len(ddf[col_len_check].compute()), c_


def weighted_mean_for_groupby_np(
        partition: dask.dataframe,
        cols_to_avg: Union[list, iter],
        col_weights: str,
        output_dtype=np.float32,
):
    """Wrapper to get weighted average"""

    # when calculating a single value, the values are NOT np arrays,
    # . but when creating a batch, they do become NP arrays... ?? seems confusing
    try:
        return pd.Series(
            np.average(
                partition[cols_to_avg].values,
                weights=partition[col_weights].values,
                axis=0,
            ),
            index=cols_to_avg
        ).astype(output_dtype)
    except ValueError as e:
        print(e)
        logging.info(e)

        return pd.Series(
            np.average(
                partition[cols_to_avg].values.compute(),
                weights=partition[col_weights].values.compute(),
                axis=0,
            ),
            index=cols_to_avg
        ).astype(output_dtype)


class AggregateEmbeddingsConfig:
    """Hydra-based config to load & override config

    Example uses:
    config_test = AggregateEmbeddingsConfig(
        config_path="../config",
        config_name='aggregate_embeddings',
        overrides=['mlflow_experiment=v0.3.2_use_multi_aggregates_test', 'n_sample_posts=1000', 'n_sample_comments=2000']
    )

    mlflow_experiment_full = 'v0.3.2_use_multi_aggregates'
    config_full_lc_false = AggregateEmbeddingsConfig(
        config_path="../config",
        config_name='aggregate_embeddings',
        overrides=[f"mlflow_experiment={mlflow_experiment_full}",
                   'n_sample_posts=null',
                   'n_sample_comments=null',
                   'data_embeddings_to_aggregate=top_subs-2021_07_16-use_muti_lower_case_false',
                  ]
    )
    """
    def __init__(
            self,
            config_path: str = "../config",
            config_name: str = 'aggregate_embeddings',
            overrides: List[str] = None,
    ):
        """

        Args:
            config_path:
                Path to root config, relative to current file
            config_name:
                Name of config, exclude `.yaml` extension
            overrides:
                List of items to override from default config.
                Note: If you add `+` to beginning of override item, it will ADD it, instead of
                overriding it.
        """
        with initialize(config_path=config_path):
            if overrides is not None:
                self.config = compose(config_name=config_name, overrides=overrides)
            else:
                self.config = compose(config_name=config_name)

        self.config_dict = OmegaConf.to_container(self.config)

        # Note: it only goes one level
        self.config_flat = dict()
        for k, v in self.config_dict.items():
            if isinstance(v, dict):
                for k_nested, v_nested in v.items():
                    self.config_flat[k_nested] = v_nested
            else:
                self.config_flat[k] = v


def load_config_agg_jupyter(
        config_path: str = "../config",
        config_name: str = 'aggregate_embeddings',
        overrides: List[str] = None,
        return_dict: bool = False,
        return_flat: bool = False,
) -> Union[DictConfig, dict]:
    """
    Wrapper around hydra API to load configs.

    Example use:
    load_default_config_agg_jupyter(
        return_dict=True,
        overrides=['data_text_and_metadata=german_subs_2021_06_16']
    )

    Args:
        config_path:
            Path to root config, relative to current file
        config_name:
            Name of config, exclude `.yaml` extension
        overrides:
            List of items to override from default config.
            Note: If you add `+` to beginning of override item, it will ADD it, instead of
            overriding it.
        return_dict:
            Set to True to return a python dictionary.
            By default, function will return an `OmegaConf` object.

    Returns:
        `OmegaConf` object or python dict
    """
    with initialize(config_path=config_path):
        if overrides is not None:
            cfg = compose(config_name=config_name, overrides=overrides)
        else:
            cfg = compose(config_name=config_name)

    if return_dict:
        return OmegaConf.to_container(cfg)

    elif return_flat:
        cfg_nested = OmegaConf.to_container(cfg)
        cfg_flat = dict()
        for k, v in cfg_nested.items():
            if isinstance(v, dict):
                for k_nested, v_nested in v.items():
                    cfg_flat[k_nested] = v_nested
            else:
                cfg_flat[k] = v
        return cfg_flat

    else:
        return cfg


@hydra.main(config_path="../config", config_name="aggregate_embeddings")
def load_config_agg_cli(
        cfg: DictConfig,
        return_dict: bool = False,
) -> Union[DictConfig, dict]:
    """"""
    if return_dict:
        return OmegaConf.to_container(cfg)
    else:
        return cfg


#
# ~ fin
#
