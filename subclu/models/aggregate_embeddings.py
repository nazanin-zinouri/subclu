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
from typing import Tuple, Union, List

import mlflow
import pandas as pd
import numpy as np
from tqdm.auto import tqdm

from ..data.data_loaders import LoadSubreddits, LoadPosts, LoadComments

from ..utils.mlflow_logger import MlflowLogger
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
            col_comment_text_len: str = 'comment_text_len',
            min_comment_text_len: int = 11,
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

            agg_comments_to_post_weight_col: str = 'comment_text_len',
            agg_post_post_weight: int = 70,
            agg_post_comment_weight: int = 20,
            agg_post_subreddit_desc_weight: int = 10,

            df_subs_meta: pd.DataFrame = None,
            df_posts_meta: pd.DataFrame = None,
            df_comments_meta: pd.DataFrame = None,
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
        self.min_comment_text_len = min_comment_text_len

        self.df_v_sub = df_v_sub
        self.subreddit_desc_uuid = subreddit_desc_uuid
        self.subreddit_desc_folder = subreddit_desc_folder
        self.col_subreddit_id = col_subreddit_id

        self.n_sample_posts = n_sample_posts
        self.n_sample_comments = n_sample_comments

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

        # Create path to store local run
        self.path_local_model = None

        # Set mlflowLogger instance for central tracker
        self.mlf = MlflowLogger(tracking_uri=self.mlflow_tracking_uri)

        # Keep track of comments per post here
        self.df_comment_count_per_post = None

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
        info(f"== Start run_aggregation() method ==")

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
        self._load_raw_embeddings()

        # ---------------------
        # Load metadata from files
        #   Needed if we're filtering or adding weights, examples:
        #   - text len
        #   - word count
        #   - date posted/created
        #   - up votes
        # ---
        self._load_metadata()

        # Filter out short comments using metadata
        # ---
        if self.min_comment_text_len is not None:
            info(f"{self.min_comment_text_len} <- Removing comments shorter than {self.min_comment_text_len} characters.")
            short_comments_to_remove = self.df_comments_meta[
                self.df_comments_meta['comment_text_len'] <= self.min_comment_text_len
            ][self.col_comment_id]

            self.df_v_comments = (
                self.df_v_comments
                [~(self.df_v_comments.index.get_level_values(self.col_comment_id).isin(short_comments_to_remove))]
            )
            info(f"  {self.df_v_comments.shape} <- df_v_comments.shape AFTER removing short comments")
            gc.collect()

        # ---------------------
        # TODO(djb): Merge all comments at post-level
        # Weights by:
        # - text len or word count (how to account for emoji & ASCII art?)
        #     - Word count regex breaks b/c it doesn't work on non-latin alphabets
        # - up-votes
        # ---
        self._agg_comments_to_post_level()

        # ---------------------
        # TODO(djb): Merge at post-level basic
        #  - B) post + comments
        #  - C) post + comments + subreddit description
        # TODO(djb): Weights by inputs, e.g., 70% post, 20% comments, 10% subreddit description
        # ---
        self._agg_posts_and_comments_to_post_level()
        logging.warning(f"TODO: post-aggregation that includes subreddit description")
        # self._agg_posts_comments_and_sub_descriptions_to_post_level()

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
        total_fxn_time = elapsed_time(start_time=t_start_agg_embed, log_label='Total Agg fxn time', verbose=True)
        mlflow.log_metric('vectorizing_time_minutes',
                          total_fxn_time / timedelta(minutes=1)
                          )
        mlflow.end_run()
        info(f"== COMPLETE run_aggregation() method ==")
        return (
            df_subs_agg_a, df_subs_agg_b, df_subs_agg_c, self.df_posts_agg_b, df_posts_agg_c, df_posts_agg_d
        )

    def _create_and_log_config(self):
        """Convert inputs into a dictionary we can save to replicate the run

        Don't log dfs with meta or raw embeddings! they could be dfs that take up gigs of storage
        """

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
            'min_comment_text_len': self.min_comment_text_len,

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

            'agg_comments_to_post_weight_col': self.agg_comments_to_post_weight_col,
            'agg_post_post_weight': self.agg_post_post_weight,
            'agg_post_comment_weight': self.agg_post_comment_weight,
            'agg_post_subreddit_desc_weight': self.agg_post_subreddit_desc_weight,
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
        if self.df_v_sub is None:
            info(f"Loading subreddit description embeddings...")
            self.df_v_sub = self.mlf.read_run_artifact(
                run_id=self.subreddit_desc_uuid,
                artifact_folder=self.subreddit_desc_folder,
                read_function=pd.read_parquet,
            )
        else:
            info(f"Raw subreddit embeddings pre-loaded")
            # copy so that the internal object is different from the pre-loaded object
            self.df_v_sub = self.df_v_sub.copy()
        info(f"  {self.df_v_sub.shape} <- Raw vectorized subreddit description shape")
        if active_run is not None:
            r_sub, c_sub = self.df_v_sub.shape
            mlflow.log_metrics({'sub_description_raw_rows': r_sub, 'sub_description_raw_cols': c_sub})
        assert (len(self.df_v_sub) == self.df_v_sub.index.nunique()), f"** Index not unique. Check duplicates df_v_sub **"

        if self.df_v_posts is None:
            info(f"Loading POSTS embeddings...")
            self.df_v_posts = self.mlf.read_run_artifact(
                run_id=self.posts_uuid,
                artifact_folder=self.posts_folder,
                read_function=pd.read_parquet,
            )
        else:
            info(f"POSTS embeddings pre-loaded")
            # copy so that the internal object is different from the pre-loaded object
            self.df_v_posts = self.df_v_posts.copy()

        info(f"  {self.df_v_posts.shape} <- Raw POSTS shape")
        if self.n_sample_posts is not None:
            if len(self.df_v_posts) > self.n_sample_posts:
                info(f"  Sampling POSTS down to: {self.n_sample_posts:,.0f}")
                self.df_v_posts = self.df_v_posts.sample(n=self.n_sample_posts)
                info(f"  {self.df_v_posts.shape} <- df_posts.shape AFTER sampling")
            else:
                info(f"  No need to sample POSTS because sample greater than rows in df_posts")
        if active_run is not None:
            r_post, c_post = self.df_v_posts.shape
            mlflow.log_metrics({'posts_raw_rows': r_post, 'posts_raw_cols': c_post})
        assert (len(self.df_v_posts) == self.df_v_posts.index.nunique()), f"** Index not unique. Check duplicates df_v_posts **"

        if self.df_v_comments is None:
            info(f"Loading COMMENTS embeddings...")
            self.df_v_comments = self.mlf.read_run_artifact(
                run_id=self.comments_uuid,
                artifact_folder=self.comments_folder,
                read_function=pd.read_parquet,
            )
        else:
            info(f"COMMENTS embeddings pre-loaded")
            self.df_v_comments = self.df_v_comments.copy()
        info(f"  {self.df_v_comments.shape} <- Raw COMMENTS shape")
        info(f"  Keep only comments for posts with embeddings")
        self.df_v_comments = (
            self.df_v_comments
            [self.df_v_comments.index.get_level_values('post_id').isin(
                self.df_v_posts.index.get_level_values('post_id').unique()
             )]
        )
        info(f"  {self.df_v_comments.shape} <- COMMENTS shape, after keeping only existing posts")

        if self.n_sample_comments is not None:
            if len(self.df_v_comments) > self.n_sample_comments:
                info(f"  Sampling COMMENTS down to: {self.n_sample_comments:,.0f}")
                self.df_v_comments = self.df_v_comments.sample(n=self.n_sample_comments)
                info(f"  {self.df_v_comments.shape} <- df_v_comments.shape AFTER sampling")
            else:
                info(f"  No need to sample comments because sample greater than rows in df_comments")

        if active_run is not None:
            r_com, c_com = self.df_v_comments.shape
            mlflow.log_metrics({'comments_raw_rows': r_com, 'comments_raw_cols': c_com})
        assert (len(self.df_v_comments) == self.df_v_comments.index.nunique()), f"** Index not unique. Check duplicates df_v_comments **"

        elapsed_time(start_time=t_start_read_raw_embeds, log_label='Total raw embeddings load', verbose=True)
        gc.collect()

    def _load_metadata(self):
        """Load metadata to filter comments or add weights based on metadata"""
        info(f"-- Start _load_metadata() method --")
        t_start_read_meta = datetime.utcnow()
        if self.df_subs_meta is None:
            info(f"Loading subs metadata...")
            self.df_subs_meta = LoadSubreddits(
                bucket_name=self.bucket_name,
                folder_path=self.folder_meta_subreddits,
                folder_posts=self.folder_meta_posts,
                columns=None,
            ).read_raw()
        else:
            info(f"Subreddits META pre-loaded")
        info(f"  {self.df_subs_meta.shape} <- Raw META subreddit description shape")

        if self.df_posts_meta is None:
            info(f"Loading POSTS metadata...")
            self.df_posts_meta = LoadPosts(
                bucket_name=self.bucket_name,
                folder_path=self.folder_meta_posts,
                columns='aggregate_embeddings_',
            ).read_raw()
        else:
            info(f"Posts META pre-loaded")
        info(f"  {self.df_posts_meta.shape} <- Raw META POSTS shape")

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

            df_comms_with_weights = (
                self.df_v_comments[~mask_single_comments]
                .reset_index()
                .merge(
                    self.df_comments_meta[['post_id', self.agg_comments_to_post_weight_col]],
                    how='left',
                    on=['post_id']
                )
            )

            d_weighted_mean_agg = dict()
            for id_, df in tqdm(df_comms_with_weights.groupby('post_id')):
                d_weighted_mean_agg[id_] = np.average(
                    df[l_embedding_cols],
                    weights=np.log(2 + df[self.agg_comments_to_post_weight_col]),
                    axis=0,
                )
            gc.collect()

            df_agg_multi_comments = pd.DataFrame(d_weighted_mean_agg).T
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

        # TODO(djb): Create df with:
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
        # Convert dict to df so we can merge it with
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
