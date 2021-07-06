"""
This module focuses on aggregating embeddings AFTER they've been vectorized, for example
by functions from `models/vectorize_text.py`

Vectorize text > Aggregate embeddings > Compress embeddings | Cluster posts | Cluster subs

"""

from datetime import datetime, timedelta
import gc
import logging
from logging import info
from pathlib import Path
from typing import Tuple

import mlflow
import pandas as pd
import numpy as np

from ..data.data_loaders import LoadSubreddits, LoadPosts, LoadComments

from ..utils.mlflow_logger import MlflowLogger
from ..utils import mlflow_logger
from ..utils import get_project_subfolder
from ..utils.eda import elapsed_time


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
            folder_meta_subreddits: str = 'subreddits/de/2021-06-16',
            folder_meta_comments: str = 'comments/de/2021-06-16',
            folder_meta_posts: str = 'posts/de/2021-06-16',

            posts_uuid: str = 'db7a4d8aff04420eb4229d6499055e04',
            posts_folder: str = 'df_vect_posts',
            col_text_post_word_count: str = 'text_word_count',
            col_post_id: str = 'post_id',
            df_v_posts: pd.DataFrame = None,

            comments_uuid: str = 'db7a4d8aff04420eb4229d6499055e04',
            comments_folder: str = 'df_vect_comments',
            col_comment_id: str = 'comment_id',
            col_text_comment_word_count: str = 'comment_text_word_count',
            min_text_len_comment: int = None,
            df_v_comments: pd.DataFrame = None,

            subreddit_desc_uuid: str = 'db7a4d8aff04420eb4229d6499055e04',
            subreddit_desc_folder: str = 'df_vect_subreddits_description',
            col_subreddit_id: str = 'subreddit_id',
            df_v_sub: pd.DataFrame = None,

            mlflow_experiment: str = 'use_multilingual_v1_aggregates',
            run_name: str = None,
            mlflow_tracking_uri: str = 'sqlite',

            n_sample_posts: int = None,
            n_sample_comments: int = None,
    ):
        """"""
        self.bucket_name = bucket_name
        self.folder_meta_subreddits = folder_meta_subreddits
        self.folder_meta_comments = folder_meta_comments
        self.folder_meta_posts = folder_meta_posts

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

        self.df_v_sub = df_v_sub
        self.subreddit_desc_uuid = subreddit_desc_uuid
        self.subreddit_desc_folder = subreddit_desc_folder
        self.col_subreddit_id = col_subreddit_id

        self.n_sample_posts = n_sample_posts
        self.n_sample_comments = n_sample_comments

        # Create path to store local run
        self.path_local_model = None

        # Set mlflowLogger instance for central tracker
        self.mlf = MlflowLogger(tracking_uri=self.mlflow_tracking_uri)

    def run_aggregation(self) -> Tuple[pd.DataFrame]:
        """Main function to run full aggregation job

        TODO(djb): Should I try to emulate fit, fit_transform, & transform methods from sklearn for this class?
          Because once I run it on some subset of data, I might need to apply it to new/unseen data
          Or is the plan to run the process/method from scratch every time?
          Need to think how this will work incrementally - e.g., if we run every week we don't need to
          re-run embeddings process on old posts, only need to update new posts (and give less weight to old posts)
          right?
        """
        t_start_agg_embed = datetime.utcnow()
        info(f"Start aggregate function")

        info(f"MLflow tracking URI: {mlflow.get_tracking_uri()}")
        self.mlf.set_experiment(self.mlflow_experiment)
        mlflow.start_run(run_name=self.run_name)
        self.mlf.add_git_hash_to_active_run()
        self.mlf.set_tag_hostname(key='host_name')
        self.mlf.log_param_hostname(key='host_name')

        # create local path to store artifacts before logging to mlflow
        self.path_local_model = get_project_subfolder(
            f"data/models/aggregate_embeddings/{datetime.utcnow().strftime('%Y-%m-%d_%H%M%S')}-{self.run_name}"
        )
        Path(self.path_local_model).mkdir(exist_ok=True, parents=True)
        info(f"  Local model saving directory: {self.path_local_model}")

        # Log configuration so we can replicate run
        self._create_and_log_config()
        mlflow.log_params(self.config_to_log_and_store)

        # Initialize values to output
        # TODO(djb): delete these initial values, only keep them while testing
        (df_subs_agg_a, df_subs_agg_b, df_subs_agg_c,
         df_posts_agg_b, df_posts_agg_c, df_posts_agg_d
         ) = None, None, None, None, None, None,

        # ---------------------
        # Load raw embeddings
        # ---
        t_start_read_raw_embeds = datetime.utcnow()
        self._load_raw_embeddings()
        elapsed_time(start_time=t_start_read_raw_embeds, log_label='Total raw embeddings load', verbose=True)


        # ---------------------
        # TODO(djb): Load metadata from files
        #   Needed if I'm filtering by:
        #   - text len
        #   - word count
        #   - date posted/created
        #   Or if I'm adding weights by upvotes or text length
        # ---
        t_start_read_meta = datetime.utcnow()
        self._load_metadata()
        elapsed_time(start_time=t_start_read_meta, log_label='Total metadata loading', verbose=True)

        # TODO(djb): Filter out short comments
        # ---
        logging.warning(f"Currently not filtering out short comments...")

        # ---------------------
        # TODO(djb): Merge all comments at post-level
        # Weights by:
        # - text len or word count (how to account for emoji & ASCII art?)
        #     - Word count regex breaks b/c it doesn't work on non-latin alphabets
        # - up-votes
        # ---

        # ---------------------
        # TODO(djb): Merge at post-level basic
        #  - B) post + comments
        #  - C) post + comments + subreddit description
        # TODO(djb): Weights by inputs, e.g., 70% post, 20% comments, 10% subreddit description
        # ---

        # ---------------------
        # TODO(djb): Merge at post-level with subreddit lag
        #  - D) post + comments + subreddit aggregate
        # After we calculate all post-level basic embeddings:
        # - For each day a subreddit has a post, calculate subreddit embeddings of previous N-days
        # TODO(djb) For any post-strategy above, also combine previous n-days of posts in a subreddit
        #  Similar to
        # ---

        # ---------------------
        # TODO(djb): Merge at subreddit-level
        #  - A) posts only
        #  - B) posts + comments only
        #  - C) posts + comments + subreddit description
        # Weights by:
        # - text len or word count (how to account for emoji & ASCII art?)
        #     - Word count regex breaks b/c it doesn't work on non-latin alphabets
        # - number of up-votes
        # - number of comments
        # - number of days since post was created (more recent posts get more weight)
        # ---

        # TODO(djb): when saving a df to parquet, save in multiple files, otherwise
        #  reading from a single file can take over 1 minute for ~1.2 million rows

        # finish logging total time + end mlflow run
        info(f"Aggregation job COMPLETE")
        total_fxn_time = elapsed_time(start_time=t_start_agg_embed, log_label='Total Agg fxn time', verbose=True)
        mlflow.log_metric('vectorizing_time_minutes',
                          total_fxn_time / timedelta(minutes=1)
                          )
        mlflow.end_run()
        return (
            self.df_v_sub, self.df_v_posts, self.df_v_comments,
            self.df_subs_meta, self.df_posts_meta, self.df_comments_meta,
            df_subs_agg_a, df_subs_agg_b, df_subs_agg_c, df_posts_agg_b, df_posts_agg_c, df_posts_agg_d
        )

    def _create_and_log_config(self):
        """Convert inputs into a dictionary we can save to replicate the run"""
        self.config_to_log_and_store = {
            'bucket_name': self.bucket_name,
            'folder_meta_subreddits': self.folder_meta_subreddits,
            'folder_meta_comments': self.folder_meta_comments,
            'folder_meta_posts': self.folder_meta_posts,

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

            'subreddit_desc_uuid': self.subreddit_desc_uuid,
            'subreddit_desc_folder': self.subreddit_desc_folder,
            'col_subreddit_id': self.col_subreddit_id,

            # 'train_exclude_duplicated_docs': self.train_exclude_duplicated_docs,
            # 'train_min_word_count': self.train_min_word_count,
            # 'train_use_comments': self.train_use_comments,
            #
            # 'tf_batch_inference_rows': self.tf_batch_inference_rows,
            # 'tf_limit_first_n_chars': self.tf_limit_first_n_chars,

            'n_sample_posts': self.n_sample_posts,
            'n_sample_comments': self.n_sample_comments,
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
        if self.df_v_sub is None:
            info(f"Loading subreddit description embeddings...")
            self.df_v_sub = self.mlf.read_run_artifact(
                run_id=self.subreddit_desc_uuid,
                artifact_folder=self.subreddit_desc_folder,
                read_function=pd.read_parquet,
            )
        else:
            info(f"Raw subreddit embeddings pre-loaded")
        info(f"    {self.df_v_sub.shape} <- Raw vectorized subreddit description shape")

        if self.df_v_posts is None:
            info(f"Loading POSTS embeddings...")
            self.df_v_posts = self.mlf.read_run_artifact(
                run_id=self.posts_uuid,
                artifact_folder=self.posts_folder,
                read_function=pd.read_parquet,
            )
        else:
            info(f"POSTS embeddings pre-loaded")
        info(f"    {self.df_v_posts.shape} <- Raw POSTS shape")
        if self.n_sample_posts is not None:
            info(f"  Sampling posts down to: {self.n_sample_posts:,.0f}")
            self.df_v_posts = self.df_v_posts.sample(n=self.n_sample_posts)
            info(f"  {self.df_v_posts.shape} <- df_posts.shape AFTER sampling")

        r_post, c_post = self.df_v_posts.shape
        mlflow.log_metrics({'posts_raw_rows': r_post, 'posts_raw_cols': c_post})

        if self.df_v_comments is None:
            info(f"Loading COMMENTS embeddings...")
            self.df_v_comments = self.mlf.read_run_artifact(
                run_id=self.comments_uuid,
                artifact_folder=self.comments_folder,
                read_function=pd.read_parquet,
            )
        else:
            info(f"COMMENTS embeddings pre-loaded")
        info(f"    {self.df_v_comments.shape} <- Raw COMMENTS shape")
        info(f"  Keep only comments for posts with embeddings")
        self.df_v_comments = (
            self.df_v_comments
            [self.df_v_comments.index.get_level_values('post_id').isin(
                self.df_v_posts.index.get_level_values('post_id').unique()
            )]
        )
        info(f"    {self.df_v_comments.shape} <- COMMENTS shape, after keeping only existing posts")

        if self.n_sample_comments is not None:
            if len(self.df_v_comments) > self.n_sample_comments:
                info(f"  Sampling posts down to: {self.n_sample_comments:,.0f}")
                self.df_v_comments = self.df_v_comments.sample(n=self.n_sample_comments)
                info(f"  {self.df_v_comments.shape} <- df_v_comments.shape AFTER sampling")
            else:
                info(f"  No need to sample comments because sample greater than rows in df_comments")

        r_com, c_com = self.df_v_comments.shape
        mlflow.log_metrics({'comments_raw_rows': r_com, 'comments_raw_cols': c_com})

    def _load_metadata(self):
        """Load metadata to filter comments or add weights based on metadata"""
        info(f"Loading subs metadata...")
        self.df_subs_meta = LoadSubreddits(
            bucket_name=self.bucket_name,
            folder_path=self.folder_meta_subreddits,
            folder_posts=self.folder_meta_posts,
            columns=None,
        ).read_raw()
        info(f"  {self.df_subs_meta.shape} <- Raw META subreddit description shape")

        info(f"Loading POSTS metadata...")
        self.df_posts_meta = LoadPosts(
            bucket_name=self.bucket_name,
            folder_path=self.folder_meta_posts,
            columns='aggregate_embeddings_',
        ).read_raw()
        info(f"  {self.df_posts_meta.shape} <- Raw META POSTS shape")

        info(f"Loading COMMENTS metadata...")
        self.df_comments_meta = LoadComments(
            bucket_name=self.bucket_name,
            folder_path=self.folder_meta_comments,
            columns='aggregate_embeddings_',
        ).read_raw()
        info(f"  {self.df_comments_meta.shape} <- Raw META COMMENTS shape")




#
# ~ fin
#
