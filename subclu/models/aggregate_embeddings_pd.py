"""
This module focuses on aggregating embeddings AFTER they've been vectorized, for example
by functions from `models/vectorize_text.py`

Vectorize text > Aggregate embeddings > Compress embeddings | Cluster posts | Cluster subs

"""
from datetime import datetime, timedelta
import gc
import logging
from logging import info
import math
from pathlib import Path
from typing import Tuple, Union, List

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
            min_comment_text_len: int = 5,
            df_v_comments: pd.DataFrame = None,

            subreddit_desc_uuid: str = 'db7a4d8aff04420eb4229d6499055e04',
            subreddit_desc_folder: str = 'df_vect_subreddits_description',
            col_subreddit_id: str = 'subreddit_id',
            df_v_sub: pd.DataFrame = None,

            mlflow_experiment: str = 'use_multilingual_v1_aggregates',
            run_name: str = 'aggregate_embeddings_pd',
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

            # embeddings_read_fxn: callable = dd.read_parquet,
            # metadata_read_fxn: callable = pd.read_parquet,
            calculate_similarites: bool = False,
            logs_path: str = 'logs/AggregateEmbeddings',
            unique_checks: bool = False,
            **kwargs
    ):
        """"""
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

        # self.embeddings_read_fxn = embeddings_read_fxn
        # self.metadata_read_fxn = metadata_read_fxn

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
        self._load_metadata()
        self.mlf.log_ram_stats(only_memory_used=True)

        # Filter out short comments using metadata
        # ---
        if self.min_comment_text_len is not None:
            info(f"{self.min_comment_text_len} <- Removing comments shorter than {self.min_comment_text_len} characters.")
            short_comments_to_remove = self.df_comments_meta[
                self.df_comments_meta[self.col_comment_text_len] <= self.min_comment_text_len
            ][self.col_comment_id]

            self.df_v_comments = (
                self.df_v_comments
                [~(self.df_v_comments.index.get_level_values(self.col_comment_id).isin(short_comments_to_remove))]
            )
            info(f"  {self.df_v_comments.shape} <- df_v_comments.shape AFTER removing short comments")
            gc.collect()

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

        # TODO(djb): instead of manually logging everything, use vars(self)
        #  to get all params & filter out:
        #  - things that start with `df_`
        #  - things named `mlf` (it's an mlflowLogger object)
        self.config_to_log_and_store = dict()
        for k_, v_ in vars(self).items():
            try:
                if any([k_.startswith('df_'), k_ == 'mlf']):
                    continue
                elif any([isinstance(v_, pd.DataFrame),
                          isinstance(v_, logging.FileHandler),
                          isinstance(v_, dict),
                          isinstance(v_, Path),
                          ]):
                    # Ignore dicts and other objects that won't be easy to pickle
                    # would it be better to only keep things that should be easy to pickle instead?
                    #  e.g., string, list, numeric, None ?
                    continue
                else:
                    self.config_to_log_and_store[k_] = v_
            except Exception as e:
                logging.warning(f"Error logging {k_}:\n  {e}")

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
                read_function=pd.read_parquet,
                cache_locally=True,
            )
        else:
            info(f"Raw subreddit embeddings pre-loaded")
            # copy so that the internal object is different from the pre-loaded object
            self.df_v_sub = self.df_v_sub.copy()
        r_sub, c_sub = self.df_v_sub.shape
        info(f"  {r_sub:10,.0f} | {c_sub:,.0f} <- Raw vectorized subreddit description shape")
        if active_run is not None:
            mlflow.log_metrics({'sub_description_raw_rows': r_sub, 'sub_description_raw_cols': c_sub})
        info(f"  Unique check for subreddit description...")
        assert (r_sub == self.df_v_sub[self.col_subreddit_id].nunique()), (f"** Index not unique. "
                                                                           f"Check duplicates df_v_sub **")

        # ------------------------
        # Load and check POSTS
        # ---
        if self.df_v_posts is None:
            info(f"Loading POSTS embeddings...")
            if self.n_sample_posts_files is not None:
                info(f"  Sampling POSTS FILES down to: {self.n_sample_posts_files:,.0f}")

            # pd.read_parquet will load all files, if you want to sample, then you need to
            # dd.read_parquet & compute()
            self.df_v_posts = self.mlf.read_run_artifact(
                run_id=self.posts_uuid,
                artifact_folder=self.posts_folder,
                read_function=dd.read_parquet,
                cache_locally=True,
                n_sample_files=self.n_sample_posts_files,
            ).compute()
        else:
            info(f"POSTS embeddings pre-loaded")
            # copy so that the internal object is different from the pre-loaded object
            self.df_v_posts = self.df_v_posts.copy()

        r_post, c_post = self.df_v_posts.shape
        info(f"  {r_post:10,.0f} | {c_post:4,.0f} <- Raw POSTS shape")
        # Sampling only works reliably in pandas, it takes forever to compute in dask,
        #  so we only sample at file-level
        if active_run is not None:
            mlflow.log_metrics({'posts_raw_rows': r_post, 'posts_raw_cols': c_post})
        info(f"  Checking that posts are unique...")
        assert (r_post == self.df_v_posts[self.col_post_id].nunique()), (f"** Post-ID NOT unique. "
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
            #   - concat the N dataframes
            if isinstance(self.comments_uuid, str):
                # It might be faster to load as dask df (because it loads each file in parallel)
                #  and then call .compute() to convert to a single df
                self.df_v_comments = self.mlf.read_run_artifact(
                    run_id=self.comments_uuid,
                    artifact_folder=self.comments_folder,
                    read_function=dd.read_parquet,
                    cache_locally=True,
                    n_sample_files=self.n_sample_comments_files,
                ).compute()

            else:
                info(f"  Found {len(self.comments_uuid)} run UUIDs with COMMENT embeddings...")
                if self.n_sample_comments_files is not None:
                    n_files_per_run = math.ceil(self.n_sample_comments_files / len(self.comments_uuid))
                    info(f"    Sampling {n_files_per_run} FILES per run UUID")
                else:
                    n_files_per_run = None

                self.df_v_comments = pd.concat(
                    [
                        self.mlf.read_run_artifact(
                            run_id=comm_uuid_,
                            artifact_folder=self.comments_folder,
                            read_function=dd.read_parquet,
                            cache_locally=True,
                            n_sample_files=n_files_per_run,
                        ).compute() for comm_uuid_ in self.comments_uuid
                    ],
                    axis=0, ignore_index=False,
                )

        else:
            info(f"COMMENTS embeddings pre-loaded")
            self.df_v_comments = self.df_v_comments.copy()

        r_com, c_com = self.df_v_comments.shape
        info(f"  {r_com:10,.0f} | {c_com:4,.0f} <- Raw COMMENTS shape")
        info(f"  Keep only comments for posts with embeddings")
        # The index is now empty (it's an integer), so instead call columns directly
        self.df_v_comments = (
            self.df_v_comments
            [self.df_v_comments['post_id'].isin(
                self.df_v_posts['post_id'].unique()
             )]
        )
        r_com, c_com = self.df_v_comments.shape
        info(f"  {r_com:10,.0f} | {c_com:4,.0f} <- COMMENTS shape, after keeping only comments to loaded posts")

        if active_run is not None:
            mlflow.log_metrics({'comments_raw_rows': r_com, 'comments_raw_cols': c_com})
        assert (r_com == self.df_v_comments[self.col_comment_id].nunique()), (f"** comment-id NOT unique. "
                                                                              f"Check duplicates df_v_comments **")

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
                folder_path=self.folder_meta_posts,
                columns='aggregate_embeddings_',
            ).read_and_apply_transformations()
        else:
            info(f"Posts META pre-loaded")
        info(f"  {self.df_posts_meta.shape} <- Raw META POSTS shape")

        if self.df_subs_meta is None:
            info(f"Loading subs metadata...")
            self.df_subs_meta = LoadSubreddits(
                bucket_name=self.bucket_name,
                folder_path=self.folder_meta_subreddits,
                folder_posts=self.folder_meta_posts,
                columns=None,
            ).read_apply_transformations_and_merge_post_aggs(df_posts=self.df_posts_meta)
        else:
            info(f"Subreddits META pre-loaded")
        info(f"  {self.df_subs_meta.shape} <- Raw META subreddit description shape")

        if self.df_comments_meta is None:
            info(f"Loading COMMENTS metadata...")
            self.df_comments_meta = LoadComments(
                bucket_name=self.bucket_name,
                folder_path=self.folder_meta_comments,
                columns='aggregate_embeddings_',
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
        t_start_agg_comments = datetime.utcnow()
        l_ix_post_level = ['subreddit_name', 'subreddit_id', 'post_id', ]
        if self.agg_comments_to_post_weight_col is None:
            info(f"No column to weight comments, simple mean for comments at post level")
            self.df_v_com_agg = (
                self.df_v_comments
                .reset_index()
                .groupby(l_ix_post_level)
                .mean()
            )

        else:
            # Check which posts have more than 1 comment.
            # Single comments = comments to posts where there's only one comment, so we don't need to weight
            #  or loop for posts with a single comment
            l_embedding_cols = list(self.df_v_comments.columns)

            self._calculate_comment_count_per_post()
            mask_single_comments = self.df_v_comments.index.get_level_values('post_id').isin(
                self.df_comment_count_per_post[self.df_comment_count_per_post['comment_count'] == 1]['post_id']
            )
            info(f"  {(~mask_single_comments).sum():9,.0f} <- Comments to use for weighted average")

            # TODO(djb)/debug: This merge function fails (runs out of memory) when I run on all
            #  comments (around 1.2 million)

            # Merge the comments that need to be averaged with the column that has the weights
            #  to average out
            df_comms_with_weights = (
                self.df_v_comments[~mask_single_comments]
                .reset_index()
                .merge(
                    self.df_comments_meta[['post_id', self.agg_comments_to_post_weight_col]],
                    how='left',
                    on=['post_id']
                )
            )

            # TODO(djb)/refactor?: This loop used to fail (runs out of memory) with 300k+ comments
            #  temp fix: so instead all comments at once only batch comments for one subreddit at a time
            #    however, that's not very efficient because some subs only have 10 posts and others have 1k+
            #  Next fixes
            #    - create batches of 750 posts at a time
            #    - limit number of comments per post (e.g., only keep "top" 20 comments?)
            #  Example:
            #  - create df: post count per subreddit
            #  - sort df descending by post count
            #  - subreddits with more than 500 posts to aggregate, process by themselves
            #  - for subreddits with fewer than 500 posts:
            #    - iteratively (recursively?) do a cumulative sum until we reach 500 posts & process
            #      that group of subreddits at the same time
            d_weighted_mean_agg = dict()
            for sub_ in tqdm(df_comms_with_weights['subreddit_name'].unique()):
                mask_sub = df_comms_with_weights['subreddit_name'] == sub_
                try:
                    for id_, df in tqdm(df_comms_with_weights[mask_sub].groupby('post_id')):
                        # TODO(djb): add limit of comments per post
                        #   sort by upvotes (desc) & text len (descending) -> keep only top 20 comments
                        d_weighted_mean_agg[id_] = np.average(
                            df[l_embedding_cols],
                            weights=np.log(2 + df[self.agg_comments_to_post_weight_col]),
                            axis=0,
                        )
                    del df
                    gc.collect()
                except MemoryError as me_:
                    try:
                        df_with_error_ids = (
                            df.drop(l_embedding_cols + [self.col_comment_id], axis=1)
                            .drop_duplicates()
                        )
                        logging.error(
                            f"MemoryError!"
                            f"\n  {id_} -> Post ID"
                            f"\n  {df.shape} -> df_.shape"
                            f"\n  {df_with_error_ids} -> df_ IDs"
                        )
                        del df, df_with_error_ids
                        gc.collect()
                    except UnboundLocalError:
                        logging.error(f"Memory error when calculating aggregate weighted mean"
                                      f"\n{me_}")
                        raise MemoryError

            df_agg_multi_comments = pd.DataFrame(d_weighted_mean_agg).T
            del d_weighted_mean_agg
            gc.collect()
            df_agg_multi_comments.columns = l_embedding_cols
            df_agg_multi_comments.index.name = 'post_id'
            info(f"  {df_agg_multi_comments.shape} <- df_agg_multi_comments shape, weighted avg only")

            info(f"  {self.df_v_comments[mask_single_comments].shape}"
                 f" <- df_v_comments shape for comments that DO NOT need to be aggregated")

            # Merge back so we have the same multi-index cols in original and new output
            df_agg_multi_comments = (
                df_agg_multi_comments
                .merge(
                    self.df_v_comments.index.to_frame(index=False)[l_ix_post_level].drop_duplicates(),
                    how='left',
                    on=['post_id'],
                )
                .set_index(l_ix_post_level)
            )
            assert (len(df_agg_multi_comments) == df_agg_multi_comments.index.nunique()), "Index not unique"

            # Merge into a a single dataframe:
            # - posts w/ multiple comments (already averaged out)
            # - posts with 1 comment (no need for weights)
            self.df_v_com_agg = pd.concat(
                [
                    df_agg_multi_comments,
                    (
                        self.df_v_comments[mask_single_comments]
                        .reset_index()
                        [l_ix_post_level + l_embedding_cols]
                        .set_index(l_ix_post_level)
                    )
                 ],
                ignore_index=False,
                axis=0
            ).sort_index()

        assert (len(self.df_v_com_agg) == self.df_v_com_agg.index.nunique()), "Index not unique"
        info(f"  {self.df_v_com_agg.shape} <- df_v_com_agg shape after aggregation")
        elapsed_time(start_time=t_start_agg_comments, log_label='Total comments to post agg loading', verbose=True)

    def _calculate_comment_count_per_post(self):
        """Calculate comment count per post if it hasn't been computed

        We should be able to use 'count' (which is faster) instead of 'nunique' because
         we checked for unique index after loading the dfs
        """
        if self.df_comment_count_per_post is None:
            info(f"Getting count of comments per post...")
            self.df_comment_count_per_post = (
                self.df_v_comments.index.to_frame(index=False)
                    .groupby(['post_id'], as_index=False)
                    .agg(
                    comment_count=('comment_id', 'count')
                )
            )

            # add posts with zero comments
            self.df_comment_count_per_post = self.df_comment_count_per_post.merge(
                self.df_v_posts.index.get_level_values('post_id').to_frame(index=False),
                how='outer',
                on=['post_id']
            )
            self.df_comment_count_per_post = self.df_comment_count_per_post.fillna(0)
            self.df_comment_count_per_post['comment_count_'] = np.where(
                self.df_comment_count_per_post['comment_count'] >= 4,
                "4+",
                self.df_comment_count_per_post['comment_count'].astype(str)
            )

            df_counts_summary = value_counts_and_pcts(
                self.df_comment_count_per_post['comment_count_'],
                add_col_prefix=False,
                count_type='posts',
                reset_index=True,
                sort_index=True,
                return_df=True,
             )
            info(f"Comments per post summary:\n{df_counts_summary}")
            info(f"  {(self.df_comment_count_per_post['comment_count'] >= 2).sum():9,.0f}"
                 f" <- Posts with 2+ comments (total posts that need COMMENT weighted average)")
            del df_counts_summary
            gc.collect()

    def _agg_posts_and_comments_to_post_level(self):
        """roll up post & comment embeddings to post-level

        Single posts = posts where there's only one comment, so we don't need to calculate weights
        """
        info(f"-- Start _agg_posts_and_comments_to_post_level() method --")
        # temp column to add averaging weights
        col_weights = '_col_method_weight_'

        t_start_method = datetime.utcnow()
        l_ix_post_level = ['subreddit_name', 'subreddit_id', 'post_id', ]
        l_embedding_cols = list(self.df_v_posts.columns)

        self._calculate_comment_count_per_post()
        mask_posts_without_comments = self.df_v_posts.index.get_level_values('post_id').isin(
            self.df_comment_count_per_post[self.df_comment_count_per_post['comment_count'] == 0]['post_id']
        )
        info(f"  {(~mask_posts_without_comments).sum():9,.0f} <- Posts that need weighted average")

        # Create df with:
        #  - posts with 1+ comments
        #    - add new col with input weight
        #  - comments for posts
        #    - one row per post, these are already aggregated
        #    - add new col with input weight
        df_posts_for_weights = pd.concat(
            [
                self.df_v_posts[~mask_posts_without_comments].assign(
                    **{col_weights: self.agg_post_post_weight}
                ),
                self.df_v_com_agg.assign(
                    **{col_weights: self.agg_post_comment_weight}
                ),
             ]
        )

        # iterate to get weighted average for each post_id that has comments
        d_weighted_mean_agg = dict()
        for id_, df in tqdm(df_posts_for_weights.groupby('post_id')):
            d_weighted_mean_agg[id_] = np.average(
                df[l_embedding_cols],
                weights=df[col_weights],
                axis=0,
            )
        gc.collect()
        # Convert dict to df so we can reshape to input multi-index
        df_agg_posts_w_comments = pd.DataFrame(d_weighted_mean_agg).T
        df_agg_posts_w_comments.columns = l_embedding_cols
        df_agg_posts_w_comments.index.name = 'post_id'
        info(f"  {df_agg_posts_w_comments.shape} <- df_agg_posts_w_comments.shape (only posts with comments)")

        # Re-append multi-index so it's the same in original and new output
        df_agg_posts_w_comments = (
            df_agg_posts_w_comments
            .merge(
                self.df_v_posts.index.to_frame(index=False).drop_duplicates(),
                how='left',
                on=['post_id'],
            )
            .set_index(l_ix_post_level)
        )
        assert (len(df_agg_posts_w_comments) == df_agg_posts_w_comments.index.nunique()), "Index not unique"

        # Merge into a a single dataframe:
        # - posts w/ multiple comments (already averaged out)
        # - posts with 1 comment (no need for weights)
        # Sort because we want most posts for a subreddit in one file or
        #  adjacent files when we save to multiple dfs
        self.df_posts_agg_b = pd.concat(
            [
                df_agg_posts_w_comments,
                self.df_v_posts[mask_posts_without_comments]
            ],
            ignore_index=False,
            axis=0
        ).sort_index()

        assert (len(self.df_posts_agg_b) == self.df_posts_agg_b.index.nunique()), "Index not unique"
        info(f"  {self.df_posts_agg_b.shape} <- df_posts_agg_b shape after aggregation")

        elapsed_time(start_time=t_start_method, log_label='Total posts & comments agg', verbose=True)

    def _agg_posts_comments_and_sub_descriptions_to_post_level(self):
        """roll up post & comment embeddings to post-level

        Single posts = posts where there's only one comment, so we don't need to calculate weights
        """
        info(f"-- Start _agg_posts_and_comments_to_post_level() method --")
        t_start_method = datetime.utcnow()
        # temp column to add averaging weights
        col_weights = '_col_method_weight_'
        l_ix_post_level = ['subreddit_name', 'subreddit_id', 'post_id', ]
        l_embedding_cols = list(self.df_v_posts.columns)

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
        #    - TODO: maybe do it within a loop?
        #        b/c we'll get a lot of copies for same data, but maybe 1 merge function is
        #        better than thousands of merge calls
        df_posts_for_weights = pd.concat(
            [
                self.df_posts_agg_b.reset_index().assign(
                    **{col_weights: (self.agg_post_post_weight + self.agg_post_comment_weight)}
                ),
                (
                    self.df_posts_agg_b.index.to_frame(index=False)
                    .merge(
                        self.df_v_sub,
                        how='left',
                        left_on=['subreddit_name', 'subreddit_id'],
                        right_index=True,
                    )
                ).assign(
                    **{col_weights: self.agg_post_subreddit_desc_weight}
                ),
            ]
        )

        # iterate to get weighted average for each post_id that has comments
        d_weighted_mean_agg = dict()
        for id_, df in tqdm(df_posts_for_weights.groupby('post_id')):
            d_weighted_mean_agg[id_] = np.average(
                df[l_embedding_cols],
                weights=df[col_weights],
                axis=0,
            )
        gc.collect()
        # Convert dict to df so we can reshape to input multi-index
        df_agg_posts_w_sub = pd.DataFrame(d_weighted_mean_agg).T
        df_agg_posts_w_sub.columns = l_embedding_cols
        df_agg_posts_w_sub.index.name = 'post_id'
        info(f"  {df_agg_posts_w_sub.shape} <- df_agg_posts_w_sub.shape (only posts with comments)")

        # Re-append multi-index so it's the same in original and new output
        self.df_posts_agg_c = (
            df_agg_posts_w_sub
            .merge(
                self.df_v_posts.index.to_frame(index=False).drop_duplicates(),
                how='left',
                on=['post_id'],
            )
            .set_index(l_ix_post_level)
        ).sort_index()
        assert (len(self.df_posts_agg_c) == self.df_posts_agg_c.index.nunique()), "Index not unique"

        info(f"  {self.df_posts_agg_c.shape} <- df_posts_agg_c shape after aggregation")

        elapsed_time(start_time=t_start_method, log_label='Total posts+comments+subs agg', verbose=True)

    def _agg_post_aggregates_to_subreddit_level(self):
        """Roll up post-level aggregations to subreddit-level"""
        info(f"-- Start _agg_posts_and_comments_to_post_level() method --")
        t_start_method = datetime.utcnow()
        # temp column to add averaging weights
        col_weights = '_col_method_weight_'
        # l_ix_post_level = ['subreddit_name', 'subreddit_id', 'post_id', ]
        l_ix_sub_level = ['subreddit_name', 'subreddit_id', ]
        l_embedding_cols = list(self.df_v_posts.columns)

        if self.agg_post_to_subreddit_weight_col is None:
            info(f"No column to weight comments, simple mean to roll up posts to subreddit-level...")

            # A - posts only
            info(f"A - posts only")
            self.df_subs_agg_a = (
                self.df_v_posts
                .reset_index()
                [l_ix_sub_level + l_embedding_cols]
                .groupby(l_ix_sub_level)
                .mean()
            ).sort_index()
            info(f"  {self.df_subs_agg_a.shape} <- df_subs_agg_a.shape (only posts)")

            # B - posts + comments
            info(f"B - posts + comments")
            self.df_subs_agg_b = (
                self.df_posts_agg_b
                .reset_index()
                [l_ix_sub_level + l_embedding_cols]
                .groupby(l_ix_sub_level)
                .mean()
            ).sort_index()
            info(f"  {self.df_subs_agg_b.shape} <- df_subs_agg_b.shape (posts + comments)")

            # C - posts + comments + sub descriptions
            info(f"C - posts + comments + sub descriptions")
            self.df_subs_agg_c = (
                self.df_posts_agg_c
                .reset_index()
                [l_ix_sub_level + l_embedding_cols]
                .groupby(l_ix_sub_level)
                .mean()
            ).sort_index()
            info(f"  {self.df_subs_agg_c.shape} <- df_subs_agg_c.shape (posts + comments + sub description)")

        else:
            raise NotImplementedError(f"Using weighted average (posts) to roll up to subreddits not implemented.")

        elapsed_time(start_time=t_start_method, log_label='Total for all subreddit-level agg', verbose=True)

    def _calculate_subreddit_similarities(self):
        """For each subreddit aggregation, calculate subreddit similarity/distances
        We want to do it with raw data/full embeddings to get most accurate similarity
        (instead of doing it after compression)
        """
        info(f"-- Start _calculate_subreddit_similarities() method --")
        t_start_method = datetime.utcnow()

        info(f"A...")
        ix_a = self.df_subs_agg_a.index.droplevel('subreddit_id')
        self.df_subs_agg_a_similarity = pd.DataFrame(
            cosine_similarity(self.df_subs_agg_a.droplevel('subreddit_id', axis='index')),
            index=ix_a,
            columns=ix_a,
        )
        self.df_subs_agg_a_similarity.columns.name = None
        self.df_subs_agg_a_similarity.index.name = 'subreddit_name'
        info(f"  {self.df_subs_agg_a_similarity.shape} <- df_subs_agg_a_similarity.shape")

        _, self.df_subs_agg_a_similarity_pair = reshape_distances_to_pairwise_bq(
            df_distance_matrix=self.df_subs_agg_a_similarity,
            df_sub_metadata=self.df_subs_meta,
            top_subs_to_keep=20,
        )
        del _
        gc.collect()

        info(f"B...")
        ix_b = self.df_subs_agg_b.index.droplevel('subreddit_id')
        self.df_subs_agg_b_similarity = pd.DataFrame(
            cosine_similarity(self.df_subs_agg_b.droplevel('subreddit_id', axis='index')),
            index=ix_b,
            columns=ix_b,
        )
        self.df_subs_agg_b_similarity.columns.name = None
        self.df_subs_agg_b_similarity.index.name = 'subreddit_name'
        info(f"  {self.df_subs_agg_b_similarity.shape} <- df_subs_agg_b_similarity.shape")
        _, self.df_subs_agg_b_similarity_pair = reshape_distances_to_pairwise_bq(
            df_distance_matrix=self.df_subs_agg_b_similarity,
            df_sub_metadata=self.df_subs_meta,
            top_subs_to_keep=20,
        )
        del _
        gc.collect()

        info(f"C...")
        ix_c = self.df_subs_agg_c.index.droplevel('subreddit_id')
        self.df_subs_agg_c_similarity = pd.DataFrame(
            cosine_similarity(self.df_subs_agg_c.droplevel('subreddit_id', axis='index')),
            index=ix_c,
            columns=ix_c,
        )
        self.df_subs_agg_c_similarity.columns.name = None
        self.df_subs_agg_c_similarity.index.name = 'subreddit_name'
        info(f"  {self.df_subs_agg_c_similarity.shape} <- df_subs_agg_c_similarity.shape")
        _, self.df_subs_agg_c_similarity_pair = reshape_distances_to_pairwise_bq(
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
            'df_post_level_agg_b_post_and_comments': self.df_posts_agg_b,
            'df_post_level_agg_c_post_comments_sub_desc': self.df_posts_agg_c,

            'df_sub_level_agg_a_post_only': self.df_subs_agg_a,
            'df_sub_level_agg_a_post_only_similarity': self.df_subs_agg_a_similarity,
            'df_sub_level_agg_a_post_only_similarity_pair': self.df_subs_agg_a_similarity_pair,

            'df_sub_level_agg_b_post_and_comments': self.df_subs_agg_b,
            'df_sub_level_agg_b_post_and_comments_similarity': self.df_subs_agg_b_similarity,
            'df_sub_level_agg_b_post_and_comments_similarity_pair': self.df_subs_agg_b_similarity_pair,

            'df_sub_level_agg_c_post_comments_and_sub_desc': self.df_subs_agg_c,
            'df_sub_level_agg_c_post_comments_and_sub_desc_similarity': self.df_subs_agg_c_similarity,
            'df_sub_level_agg_c_post_comments_and_sub_desc_similarity_pair': self.df_subs_agg_c_similarity_pair,
        }

        # create dict to make it easier to reload dataframes logged as artifacts
        # e.g., we should be able to get a list of the expected df subfolders even if we change the name of a folder
        d_dfs_folders_to_log = {k: k for k in d_dfs_to_save.keys()}
        mlflow_logger.save_and_log_config(
            config=d_dfs_folders_to_log,
            local_path=self.path_local_model,
            name_for_artifact_folder='d_logged_dfs_subfolders',
        )

        for folder_, df_ in tqdm(d_dfs_to_save.items()):
            info(f"** {folder_} **")

            info(f"Saving locally...")
            path_sub_local = self.path_local_model / folder_

            if folder_.endswith('_similarity'):
                info(f"Keeping index intact...")
                rows_, cols_ = df_.shape
                save_pd_df_to_parquet_in_chunks(
                    df=df_,
                    path=path_sub_local,
                    write_index=True,
                )
            else:
                rows_, cols_ = df_.reset_index().shape
                save_pd_df_to_parquet_in_chunks(
                    df=df_.reset_index(),
                    path=path_sub_local,
                    write_index=False,
                )

            mlflow.log_metrics(
                {f"{folder_}-rows": rows_,
                 f"{folder_}-cols": cols_,
                 }
            )

            info(f"Logging artifact to mlflow...")
            mlflow.log_artifacts(path_sub_local, artifact_path=folder_)

        elapsed_time(start_time=t_start_method, log_label='Total for _save_and_log_aggregate_and_similarity_dfs()', verbose=True)




"""
Out of memory testing using dask

            # WIP: code from notebook that crashed
            # mem_usage_mb = job_agg2.df_v_comments.memory_usage(deep=True).sum() / 1048576
            # info(f"  {mem_usage_mb:6,.1f} MB <- Memory usage")
            #
            # if mem_usage_mb < 50:
            #     target_mb_size = 30
            # elif 100 <= mem_usage_mb < 500:
            #     target_mb_size = 40
            # elif 1000 <= mem_usage_mb < 1000:
            #     target_mb_size = 60
            # else:
            #     target_mb_size = 75
            #
            # n_dask_partitions = 1 + int(mem_usage_mb // target_mb_size)
            #
            # info(f"  {n_dask_partitions:6,.0f}\t<- target Dask partitions"
            #      f"\t {target_mb_size:6,.1f} <- target MB partition size"
            #      )
            # l_posts_for_weighted_average = (
            #     job_agg2.df_v_comments[~mask_single_comments].index.get_level_values('post_id').unique().to_list()
            # )
            # ddf_comms_with_weights = (
            #     dd.from_pandas(
            #         job_agg2.df_v_comments[~mask_single_comments].reset_index(),
            #         npartitions=n_dask_partitions,
            #     )
            #         .merge(
            #         dd.from_pandas(
            #             job_agg2.df_comments_meta[['post_id', job_agg2.agg_comments_to_post_weight_col]],
            #             npartitions=n_dask_partitions,
            #         ),
            #         how='left',
            #         on=['post_id']
            #     )
            # )
            
            # ddf_groups = ddf_comms_with_weights.groupby(['post_id'])
            # __iter__ is not implemented in dask... so we need to explicitly call each group ourselves
            #  *sigh*...
            # see: https://github.com/dask/dask/issues/5124#issuecomment-524384571
            # for id_ in tqdm(l_posts_for_weighted_average):
            #     df_ = ddf_groups.get_group(id_).compute()
            #
            #     d_weighted_mean_agg[id_] = np.average(
            #         df_[l_embedding_cols],
            #         weights=np.log(2 + df_[job_agg2.agg_comments_to_post_weight_col]),
            #         axis=0,
            #     )

"""


# def get_groupby_weighted_average(
#         df_embeddings: pd.DataFrame,
#         col_groupby: str,
#         df_weights: pd.DataFrame,
#         col_merge: str,
#         col_weights: str,
#         cols_to_aggregate: Union[List[str], iter],
#         apply_log_to_weights: bool = True,
# ) -> pd.DataFrame:
#     """Helper function to do a groupby & get a weighted average
#     Function takes a single column to groupby & returns a df with that column as the index
#
#     For example:
#     df=comments, groupby=post_id -> get 1 row per post_id
#     df=posts, groupby=subreddit_name -> get 1 row per subreddit_name
#
#     Args:
#         df_embeddings:
#             This df is assumed to have a multi-index that contains col_merge & col_groupby
#         col_groupby:
#             Which column to apply weighted average on
#         df_weights:
#             df with at least 2 columns:
#                 - col_merge (for merging with df_embeddings)
#                 - col_weights (to apply weighted average)
#         col_merge:
#             Column to merge df_embeddings & df_weights
#         col_weights:
#             Column name with weights to apply
#         cols_to_aggregate:
#             Columns to aggregate (roll up)
#         apply_log_to_weights:
#             If true, apply a log function to weights to prevent rows with large weight values
#             to completely overshadow rows with small weights
#
#     Returns:
#         df with weighted averages
#     """
#     # Check which posts have more than 1 comment, doesn't make sense to loop through posts w/ 1 comment
#     df_comment_count_per_post = (
#         df_embeddings.index.to_frame(index=False)
#             .groupby([col_groupby], as_index=False)
#             .agg(
#             comment_count=(col_merge, 'count')
#         )
#     )
#     mask_posts_only_1_comment = self.df_v_comments.index.get_level_values(self.col_comment_id).isin(
#         df_comment_count_per_post[df_comment_count_per_post['comment_count'] == 1]
#     )
#
#     # TODO(djb): aggregate posts with multiple comments
#     df_comms_with_weights = (
#         self.df_v_comments[~mask_posts_only_1_comment]
#             .reset_index()
#             .merge(
#             self.df_comments_meta[['post_id', self.agg_comments_to_post_weight_col]],
#             how='left',
#             on=['post_id']
#         )
#     )
#     d_weighted_mean_agg = dict()
#     for name, df in tqdm(df_comms_with_weights.groupby('post_id')):
#         d_weighted_mean_agg[name] = np.average(
#             df[l_embedding_cols],
#             weights=np.log(2 + df[self.agg_comments_to_post_weight_col]),
#             axis=0,
#         )
#     df_agg_multi_comments = None





#
# ~ fin
#
