"""
Fork of `vectorize_text.py` to better manage memory.

Main differences:
- Only meant for USE or other tensor-hub models (Not meant to use FSE/FastText)
- Load one file at a time (instead of loading all files at a time and wasting memory)
"""

import gc
import logging
from datetime import datetime, timedelta
from functools import partial
from logging import info
from pathlib import Path
from typing import Union, Tuple, List, Optional

import mlflow
import pandas as pd
import numpy as np
# from sklearn.pipeline import Pipeline
from tqdm.auto import tqdm

import tensorflow_hub as hub

from .registry_tf_hub import D_MODELS_TF_HUB
from ..utils import get_project_subfolder
from ..utils.eda import elapsed_time
from ..utils.mlflow_logger import MlflowLogger


def vectorize_text_to_embeddings(
        mlflow_experiment: str,
        model_name: str = 'use_multilingual',
        run_name: str = None,
        tokenize_function: Union[str, callable] = 'sklearn',
        tokenize_lowercase: bool = False,

        bucket_name: str = 'i18n-subreddit-clustering',
        subreddits_path: str = 'subreddits/de/2021-06-16',
        posts_path: str = 'posts/de/2021-06-16',
        comments_path: str = 'comments/de/2021-06-16',
        preprocess_text_folder: str = None,

        col_text_post: str = 'text',
        col_text_post_word_count: str = 'text_word_count',
        col_text_post_url: str = 'post_url_for_embeddings',
        col_post_id: str = 'post_id',

        col_comment_id: str = 'comment_id',
        col_text_comment: str = 'comment_body_text',
        col_text_comment_word_count: str = 'comment_text_word_count',

        col_subreddit_id: str = 'subreddit_id',
        col_text_subreddit_description: str = 'subreddit_name_title_and_clean_descriptions',
        col_text_subreddit_word_count: str = 'subreddit_name_title_and_clean_descriptions_word_count',

        tf_batch_inference_rows: int = 1800,
        tf_limit_first_n_chars: int = 1200,

        n_sample_posts: int = None,
        n_sample_comments: int = None,
) -> Tuple[callable, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Take files in GCS as input and run them through selected model to extract embeddings.

    run inference to vectorize the text in:
    - posts_path[col_text_post]
    - posts_path[col_text_post_url] (TODO: djb)
    - comments_path[col_text_comment]
    """
    d_params_to_log = {
        'model_name': model_name,
        'tokenize_function': tokenize_function,
        'tokenize_lowercase': tokenize_lowercase,
        'bucket_name': bucket_name,
        'subreddits_path': subreddits_path,
        'posts_path': posts_path,
        'comments_path': comments_path,
        'preprocess_text_folder': preprocess_text_folder,

        'col_text_post': col_text_post,
        'col_text_post_word_count': col_text_post_word_count,
        'col_text_post_url': col_text_post_url,
        'col_post_id': col_post_id,

        'col_comment_id': col_comment_id,
        'col_text_comment': col_text_comment,
        'col_text_comment_word_count': col_text_comment_word_count,

        'col_subreddit_id': col_subreddit_id,
        'col_text_subreddit_description': col_text_subreddit_description,
        'col_text_subreddit_word_count': col_text_subreddit_word_count,

        'tf_batch_inference_rows': tf_batch_inference_rows,
        'tf_limit_first_n_chars': tf_limit_first_n_chars,
        'n_sample_posts': n_sample_posts,
        'n_sample_comments': n_sample_comments,
    }

    # load only columns needed for joining & inference
    l_cols_posts = [
        'subreddit_name',
        'subreddit_id',
        'post_id',
        col_text_post,
        col_text_post_word_count,
        col_text_post_url,
    ]
    l_cols_comments = [
        'subreddit_name',
        'subreddit_id',
        'post_id',
        'comment_id',
        col_text_comment,
        col_text_comment_word_count,
    ]
    l_cols_subreddits = [
        'subreddit_name',
        'subreddit_id',
        col_text_subreddit_description,
        col_text_subreddit_word_count,
    ]

    t_start_vectorize = datetime.utcnow()
    info(f"Start vectorize function")

    path_this_model = get_project_subfolder(
        f"data/models/{model_name}/{datetime.utcnow().strftime('%Y-%m-%d_%H%M')}"
    )
    Path(path_this_model).mkdir(exist_ok=True, parents=True)
    info(f"  Local model saving directory: {path_this_model}")

    mlf = MlflowLogger()
    info(f"MLflow tracking URI: {mlflow.get_tracking_uri()}")
    mlf.set_experiment(mlflow_experiment)
    mlflow.start_run(run_name=run_name)
    mlf.add_git_hash_to_active_run()
    mlf.set_tag_hostname(key='host_name')
    mlf.log_param_hostname(key='host_name')

    mlflow.log_params(d_params_to_log)
    mlflow.log_param(f"model_location", D_MODELS_TF_HUB[model_name])

    t_start_hub_load = datetime.utcnow()
    info(f"Loading model {model_name}..."
         # f"\n  with kwargs: {model_kwargs}"
         )
    model = hub.load(D_MODELS_TF_HUB[model_name])
    elapsed_time(t_start_hub_load, log_label='Load TF HUB model', verbose=True)

    logging.warning(f"For TF-HUB models, the only preprocessing applied is lowercase()")

    # Even if the outputs are null (not processed), these dfs are expected as output
    df_vect, df_vect_comments, df_vect_subs = None, None, None
    if subreddits_path is not None:
        info(f"Load subreddits df...")
        df_subs = pd.read_parquet(
            path=f"gs://{bucket_name}/{subreddits_path}",
            columns=l_cols_subreddits
        )
        info(f"  {df_subs.shape} <- df_subs shape")
        assert len(df_subs) == df_subs[col_subreddit_id].nunique()

    # df_posts, df_comments, df_subs = None, None, None
    if posts_path is not None:
        info(f"Loading df_posts..."
             f"\n  gs://{bucket_name}/{posts_path}")
        t_start_posts = datetime.utcnow()
        df_posts = pd.read_parquet(
            path=f"gs://{bucket_name}/{posts_path}",
            columns=l_cols_posts
        )

        elapsed_time(t_start_posts, log_label='df_post', verbose=True)
        info(f"  {df_posts.shape} <- df_posts.shape")
        assert len(df_posts) == df_posts[col_post_id].nunique()

        if n_sample_posts is not None:
            info(f"  Sampling posts down to: {n_sample_posts:,.0f}")
            df_posts = df_posts.sample(n=n_sample_posts)
            info(f"  {df_posts.shape} <- df_posts.shape AFTER sampling")

    if comments_path is not None:
        info(f"Load comments df...")
        df_comments = pd.read_parquet(
            path=f"gs://{bucket_name}/{comments_path}",
            columns=l_cols_comments
        )
        info(f"  {df_comments.shape} <- df_comments shape")
        assert len(df_comments) == df_comments[col_comment_id].nunique()

        try:
            info(f"Keep only comments that match posts IDs in df_posts...")
            df_comments = df_comments[df_comments[col_post_id].isin(df_posts[col_post_id])]
            info(f"  {df_comments.shape} <- updated df_comments shape")
        except TypeError:
            info(f"df_posts missing, so we can't filter comments...")

        if n_sample_comments is not None:
            info(f"  Sampling COMMENTS down to: {n_sample_comments:,.0f}")
            df_comments = df_comments.sample(n=n_sample_comments)
            info(f"  {df_comments.shape} <- df_comments.shape AFTER sampling")

    if subreddits_path is not None:
        info(f"Vectorizing subreddit descriptions...")
        df_vect_subs = get_embeddings_as_df(
            model=model,
            df=df_subs,
            col_text=col_text_subreddit_description,
            cols_index='subreddit_default_',
            lowercase_text=tokenize_lowercase,
            batch_size=tf_batch_inference_rows,
            limit_first_n_chars=tf_limit_first_n_chars,
        )
        save_df_and_log_to_mlflow(
            df=df_vect_subs,
            local_path=path_this_model,
            df_filename='df_vect_subreddits_description',
            name_for_metric_and_artifact_folder='df_vect_subreddits_description',
        )
        del df_subs
        gc.collect()

    if posts_path is not None:
        info(f"Vectorizing POSTS...")
        df_vect = get_embeddings_as_df(
            model=model,
            df=df_posts,
            col_text=col_text_post,
            cols_index='post_default_',
            lowercase_text=tokenize_lowercase,
            batch_size=tf_batch_inference_rows,
            limit_first_n_chars=tf_limit_first_n_chars,
        )
        save_df_and_log_to_mlflow(
            df=df_vect,
            local_path=path_this_model,
            df_filename='df_vectorized_posts',
            name_for_metric_and_artifact_folder='df_vect_posts',
        )
        del df_posts
        gc.collect()

    if comments_path is not None:
        info(f"Vectorizing COMMENTS...")
        df_vect_comments = get_embeddings_as_df(
            model=model,
            df=df_comments,
            col_text=col_text_comment,
            cols_index='comment_default_',
            lowercase_text=tokenize_lowercase,
            batch_size=tf_batch_inference_rows,
            limit_first_n_chars=tf_limit_first_n_chars,
        )
        save_df_and_log_to_mlflow(
            df=df_vect_comments,
            local_path=path_this_model,
            df_filename='df_vectorized_comments',
            name_for_metric_and_artifact_folder='df_vect_comments',
        )
        del df_comments
        gc.collect()

    # finish logging total time + end mlflow run
    total_fxn_time = elapsed_time(start_time=t_start_vectorize, log_label='Total vectorize fxn', verbose=True)
    mlflow.log_metric('vectorizing_time_minutes',
                      total_fxn_time / timedelta(minutes=1)
                      )
    mlflow.end_run()
    return model, df_vect, df_vect_comments, df_vect_subs
