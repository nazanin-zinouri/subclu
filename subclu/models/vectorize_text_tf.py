"""
Fork of `vectorize_text.py` to better manage memory.

Main differences:
- Only meant for USE or other tensor-hub models (Not meant to use FSE/FastText)
- Load one file at a time (instead of loading all files at a time and wasting memory)
"""

import gc
import logging
from datetime import datetime, timedelta
from logging import info
from pathlib import Path
from typing import Union, List, Optional, Tuple

import mlflow
import pandas as pd
# import numpy as np
# from sklearn.pipeline import Pipeline
from tqdm import tqdm

import tensorflow_hub as hub
from tensorflow import errors

from .registry_tf_hub import D_MODELS_TF_HUB
from ..utils import get_project_subfolder
from ..utils.eda import elapsed_time
from ..utils.mlflow_logger import (
    MlflowLogger, save_pd_df_to_parquet_in_chunks,
    save_and_log_config,
)
from ..utils.tqdm_logger import FileLogger, LogTQDM


log = logging.getLogger(__name__)


def vectorize_text_to_embeddings(
        subreddits_path: str,
        posts_path: str,
        comments_path: str,
        mlflow_experiment: str,
        subreddits_path_exclude: str = None,
        model_name: str = 'use_multilingual',
        run_name: str = None,
        tokenize_lowercase: bool = False,

        bucket_name: str = 'i18n-subreddit-clustering',
        preprocess_text_folder: str = None,

        col_text_post: str = 'text',
        col_text_post_word_count: str = 'text_word_count',
        col_text_post_url: str = 'post_url_for_embeddings',
        col_post_id: str = 'post_id',

        col_comment_id: str = 'comment_id',
        col_text_comment: str = 'comment_body_text',
        col_text_comment_word_count: str = 'comment_text_word_count',
        cols_index_comment: list = None,
        local_comms_subfolder_relative: str = 'df_vect_comments',
        mlflow_comments_folder: str = 'df_vect_comments',
        cols_comment_text_to_concat: List[str] = None,
        col_comment_text_to_concat: str = 'flair_post_ocr_url_text',

        col_subreddit_id: str = 'subreddit_id',
        col_text_subreddit_description: str = 'subreddit_name_title_and_clean_descriptions',
        col_text_subreddit_word_count: str = 'subreddit_name_title_and_clean_descriptions_word_count',

        tf_batch_inference_rows: int = 1000,
        tf_limit_first_n_chars: int = 1000,

        n_sample_post_files: int = None,
        n_sample_comment_files: int = None,
        n_comment_files_slice_start: int = None,
        n_comment_files_slice_end: int = None,

        batch_comment_files: bool = True,
        n_sample_posts: int = None,
        n_sample_comments: int = None,
        get_embeddings_verbose: bool = False,
        log_each_batch_df_to_mlflow_invididually: bool = False,
) -> None:
    """
    Take files in GCS as input and run them through selected model to extract embeddings.

    run inference to vectorize the text in:
    - posts_path[col_text_post]
    - posts_path[col_text_post_url] (TODO: djb)
    - comments_path[col_text_comment]
    """
    print(f"new function loaded")

    # TODO(djb): is there a way to just log all the inputs to the fxn?
    d_params_to_log = {
        'model_name': model_name,
        'tokenize_lowercase': tokenize_lowercase,
        'bucket_name': bucket_name,
        'subreddits_path': subreddits_path,
        'posts_path': posts_path,
        'comments_path': comments_path,
        'preprocess_text_folder': preprocess_text_folder,
        'subreddits_path_exclude': subreddits_path_exclude,

        'col_text_post': col_text_post,
        'col_text_post_word_count': col_text_post_word_count,
        'col_text_post_url': col_text_post_url,
        'col_post_id': col_post_id,

        'col_comment_id': col_comment_id,
        'col_text_comment': col_text_comment,
        'col_text_comment_word_count': col_text_comment_word_count,
        'cols_comment_text_to_concat': cols_comment_text_to_concat,
        'mlflow_comments_folder': mlflow_comments_folder,
        'cols_index_comment': cols_index_comment,
        'col_comment_text_to_concat': col_comment_text_to_concat,

        'col_subreddit_id': col_subreddit_id,
        'col_text_subreddit_description': col_text_subreddit_description,
        'col_text_subreddit_word_count': col_text_subreddit_word_count,

        'tf_batch_inference_rows': tf_batch_inference_rows,
        'tf_limit_first_n_chars': tf_limit_first_n_chars,

        'n_sample_post_files': n_sample_post_files,
        'n_sample_comment_files': n_sample_comment_files,
        'n_comment_files_slice_start': n_comment_files_slice_start,
        'n_comment_files_slice_end': n_comment_files_slice_end,

        'batch_comment_files': batch_comment_files,
        'n_sample_posts': n_sample_posts,
        'n_sample_comments': n_sample_comments,
    }

    # load only columns needed for joining & inference
    l_cols_ix_posts = [
        'subreddit_name',
        'subreddit_id',
        'post_id',
    ]
    l_cols_posts = l_cols_ix_posts + [
        col_text_post,
        col_text_post_word_count,
        col_text_post_url,
    ]
    l_cols_ix_comments = [
        'subreddit_name',
        'subreddit_id',
        col_comment_id,
    ]
    l_cols_comments = l_cols_ix_comments + [
        col_text_comment,
        col_text_comment_word_count,
    ]
    print(f"new function loaded")
    if col_post_id is not None:
        # Append post ID only if not None, use this 'hack'
        #  as a way to process posts with batching
        # TODO(djb) generalize comments so I can batch them the same way as comments
        l_cols_comments.append(col_post_id)
    if cols_comment_text_to_concat is not None:
        # Add text cols, but don't duplicate them
        for c_ in (set(cols_comment_text_to_concat) - set(l_cols_comments)):
            l_cols_comments.append(c_)

    l_cols_subreddits = [
        'subreddit_name',
        'subreddit_id',
        col_text_subreddit_description,
        col_text_subreddit_word_count,
    ]

    t_start_vectorize = datetime.utcnow()
    info(f"Start vectorize function")

    path_this_model = get_project_subfolder(
        f"data/models/{model_name}/{datetime.utcnow().strftime('%Y-%m-%d_%H%M%S')}"
    )
    Path(path_this_model).mkdir(exist_ok=True, parents=True)
    f_log = FileLogger(
        logs_path=path_this_model,
        log_name='log'
    )
    f_log.init_file_log()
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
    save_and_log_config(
        d_params_to_log,
        path_this_model,
        name_for_artifact_folder='config',
    )

    t_start_hub_load = datetime.utcnow()
    info(f"Loading model {model_name}...")
    model = hub.load(D_MODELS_TF_HUB[model_name])
    elapsed_time(t_start_hub_load, log_label='Load TF HUB model', verbose=True)
    logging.warning(f"For TF-HUB models, the only preprocessing applied is lowercase()")

    # check whether to filter out subs -- this filter only applies to comments or posts
    #  not to subreddit inference (this is so small and cheap it's not worth filtering)
    df_subs_exclude = None
    if subreddits_path_exclude is not None:
        # Even if we have to process thousands of subs, these should be ok
        #  as a single file. Sometimes BigQuery can output dozens of files even
        #  if we're only processing a few thousand subs
        info(f"Load subreddits df...")
        t_start_subs_filter_load = datetime.utcnow()
        df_subs_exclude = pd.read_parquet(
            path=f"gs://{bucket_name}/{subreddits_path_exclude}",
            columns=l_cols_subreddits
        )
        elapsed_time(t_start_subs_filter_load, log_label='df_subs_exclude loading', verbose=True)
        info(f"  {df_subs_exclude.shape} <- df_subs_exclude shape")
        assert len(df_subs_exclude) == df_subs_exclude[col_subreddit_id].nunique()

    if subreddits_path is not None:
        # Even if we have to process thousands of subs, these should be ok
        #  as a single file. Sometimes BigQuery can output dozens of files even
        #  if we're only processing a few thousand subs
        info(f"Load subreddits df...")
        t_start_subs_load = datetime.utcnow()
        df_subs = pd.read_parquet(
            path=f"gs://{bucket_name}/{subreddits_path}",
            columns=l_cols_subreddits
        )
        elapsed_time(t_start_subs_load, log_label='df_subs loading', verbose=True)
        info(f"  {df_subs.shape} <- df_subs shape")
        assert len(df_subs) == df_subs[col_subreddit_id].nunique()

        info(f"Vectorizing subreddit descriptions...")
        t_start_subs_vect = datetime.utcnow()
        df_vect_subs = get_embeddings_as_df(
            model=model,
            df=df_subs.reset_index(),
            col_text=col_text_subreddit_description,
            cols_index='subreddit_default_',
            lowercase_text=tokenize_lowercase,
            batch_size=tf_batch_inference_rows,
            limit_first_n_chars=tf_limit_first_n_chars,
            verbose_init=get_embeddings_verbose,
        )
        total_time_subs_vect = elapsed_time(t_start_subs_vect, log_label='df_subs vectorizing', verbose=True)
        mlflow.log_metric('vectorizing_time_minutes_subreddit_meta',
                          total_time_subs_vect / timedelta(minutes=1)
                          )
        save_df_and_log_to_mlflow(
            df=df_vect_subs.reset_index(),
            local_path=path_this_model,
            name_for_metric_and_artifact_folder='df_vect_subreddits_description',
        )
        del df_subs, df_vect_subs
        gc.collect()

    if posts_path is not None:
        info(f"Loading df_posts..."
             f"\n  gs://{bucket_name}/{posts_path}")
        t_start_posts = datetime.utcnow()
        df_posts = pd.read_parquet(
            path=f"gs://{bucket_name}/{posts_path}",
            columns=l_cols_posts
        )

        elapsed_time(t_start_posts, log_label='df_post loading', verbose=True)
        info(f"  {df_posts.shape} <- df_posts.shape")
        assert len(df_posts) == df_posts[col_post_id].nunique()

        if df_subs_exclude is not None:
            info(f"  Excluding posts for subs to exclude...")
            df_posts = df_posts[~df_posts['subreddit_id'].isin(df_subs_exclude['subreddit_id'])]
            info(f"  {df_posts.shape} <- df_posts.shape AFTER excluding subreddits")

        if n_sample_posts is not None:
            info(f"  Sampling posts down to: {n_sample_posts:,.0f}")
            df_posts = df_posts.sample(n=n_sample_posts)
            info(f"  {df_posts.shape} <- df_posts.shape AFTER sampling")

        info(f"Vectorizing POSTS...")
        t_start_posts_vect = datetime.utcnow()
        df_vect = get_embeddings_as_df(
            model=model,
            df=df_posts,
            col_text=col_text_post,
            cols_index='post_default_',
            lowercase_text=tokenize_lowercase,
            batch_size=tf_batch_inference_rows,
            limit_first_n_chars=tf_limit_first_n_chars,
            verbose_init=get_embeddings_verbose,
        )
        total_time_posts_vect = elapsed_time(t_start_posts_vect, log_label='df_posts vectorizing', verbose=True)
        mlflow.log_metric('vectorizing_time_minutes_posts',
                          total_time_posts_vect / timedelta(minutes=1)
                          )
        save_df_and_log_to_mlflow(
            df=df_vect.reset_index(),
            local_path=path_this_model,
            name_for_metric_and_artifact_folder='df_vect_posts',
        )
        del df_vect
        # We shouldn't delete df_posts because we need the IDs to check comments,
        #  but we can drop most columns
        df_posts = df_posts[l_cols_ix_posts]
        gc.collect()

    if comments_path is not None:
        if batch_comment_files:
            # TODO(djb): when doing a batch of files, would it be faster if I download the files to local?
            #  Simon & others said that it was faster to download all files first and read
            #  them from local. Not sure if the diff is big enough
            from google.cloud import storage

            info(f"** Procesing Comments files one at a time ***")
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)

            # Use this var to track how many comments we've processed
            total_comments_count = 0
            total_time_comms_vect = 0
            local_comms_subfolder_full = Path(path_this_model) / local_comms_subfolder_relative

            # # TODO(djb): check whether list of blobs is sorted alphabetically
            # #  or force sorting it so that we can apply slices
            # l_comment_files_raw = list(bucket.list_blobs(prefix=comments_path))
            # print(f"raw list of files: \n  {l_comment_files_raw}")
            #
            # # TODO(djb): blobs can't be sorted, so maybe I should save files to local cache first...
            # l_comment_files_sorted = sorted(l_comment_files_raw)
            # print(f"SORTED list of files: \n  {l_comment_files_sorted}")
            #
            # if n_comment_files_slice_end is not None:
            #     if n_comment_files_slice_start is None:
            #         n_comment_files_slice_start = 0
            #     l_comment_files_to_process = l_comment_files_sorted[
            #                                  n_comment_files_slice_start:n_comment_files_slice_end
            #                                  ]
            # else:
            #     l_comment_files_to_process = l_comment_files_sorted[:n_sample_comment_files]
            # print(f"List of files to process: \n  {l_comment_files_to_process}")

            l_comment_files_to_process = list(bucket.list_blobs(prefix=comments_path))[:n_sample_comment_files]
            total_comms_file_count = len(l_comment_files_to_process)

            info(f"-- Loading & vectorizing COMMENTS in files: {total_comms_file_count} --"
                 f"\nExpected batch size: {tf_batch_inference_rows}"

                 )
            try:
                df_posts.shape
            except (TypeError, UnboundLocalError) as e:
                logging.warning(f"df_posts missing, so we can't filter comments without a post...\n{e}")

            if n_comment_files_slice_end is not None:
                if n_comment_files_slice_start is None:
                    n_comment_files_slice_start = 0

            # TODO(djb): instead of reading each file from GCS, cache them locally first!
            count_comms_files_processed = 1  # count from 1 because slices start at zero
            for i, blob in enumerate(LogTQDM(
                    l_comment_files_to_process, mininterval=20, ascii=True,
                    logger=logging.getLogger(__name__)
            )):
                if n_comment_files_slice_end is not None:
                    if not (n_comment_files_slice_start <= i < n_comment_files_slice_end):
                        info(f"    -- Skipping file: {blob.name} --")
                        continue

                gc.collect()
                # Use this name to map old files to new files
                f_comment_name_root = blob.name.split('/')[-1].split('.')[0]
                info(f"Processing: {blob.name}")

                # TODO(djb): instead of reading each file from GCS, cache them locally first!
                df_comments = pd.read_parquet(
                    path=f"gs://{bucket_name}/{blob.name}",
                    columns=l_cols_comments
                )
                # info(f"  {df_comments.shape} <- df_comments shape")
                if len(df_comments) > df_comments[col_comment_id].nunique():
                    logging.warning(f"Found duplicate IDs in col: {col_comment_id}")
                    info(f"Keeping only one row_per ID")
                    df_comments = df_comments.drop_duplicates(subset=l_cols_ix_comments, keep='first')
                    info(f"  {df_comments.shape} <- df_comments.shape AFTER removing duplicates")

                gc.collect()

                try:
                    # reduce logging b/c we'll go through 30+ files
                    # info(f"Keep only comments that match posts IDs in df_posts...")
                    if df_posts is not None:
                        df_comments = df_comments[df_comments[col_post_id].isin(df_posts[col_post_id])]
                        info(f"  {df_comments.shape} <- df_comments.shape AFTER removing orphan comments (w/o post)")
                except (TypeError, UnboundLocalError) as e:
                    pass

                if df_subs_exclude is not None:
                    info(f"  Excluding posts for subs to exclude...")
                    df_comments = df_comments[~df_comments['subreddit_id'].isin(df_subs_exclude['subreddit_id'])]
                    info(f"  {df_comments.shape} <- df_comments.shape AFTER excluding subreddits")

                if n_sample_comments is not None:
                    n_sample_comments_per_file = 1 + int(n_sample_comments / total_comms_file_count)
                    info(f"  Sampling COMMENTS down to: {n_sample_comments:,.0f}"
                         f"     Samples PER FILE: {n_sample_comments_per_file:,.0f}")
                    df_comments = df_comments.sample(n=n_sample_comments_per_file)
                    info(f"  {df_comments.shape} <- df_comments.shape AFTER sampling")

                if len(df_comments) == 0:
                    info(f"  No comments left to vectorize after filtering, moving to next file...")
                    continue

                # only add the comment len AFTER sampling, otherwise we can get the wrong values
                total_comments_count += len(df_comments)

                if cols_comment_text_to_concat is not None:
                    info(f"Create merged text column")
                    df_comments[col_comment_text_to_concat] = ''

                    for col_ in LogTQDM(
                            cols_comment_text_to_concat, ascii=True,
                            # logger=log
                            ):
                        mask_c_not_null = ~df_comments[col_].isnull()
                        df_comments.loc[
                            mask_c_not_null,
                            col_comment_text_to_concat
                        ] = (
                            df_comments[mask_c_not_null][col_comment_text_to_concat] + '. ' +
                            df_comments[mask_c_not_null][col_]
                        )

                    # remove the first 3 characters because they'll always be '. '
                    df_comments[col_comment_text_to_concat] = df_comments[col_comment_text_to_concat].str[2:]

                t_start_comms_vect = datetime.utcnow()
                # Reset index right away so we don't forget to do it later
                df_vect_comments = get_embeddings_as_df(
                    model=model,
                    df=df_comments,
                    col_text=col_text_comment if cols_comment_text_to_concat is None else col_comment_text_to_concat,
                    cols_index='comment_default_' if cols_index_comment is None else cols_index_comment,
                    lowercase_text=tokenize_lowercase,
                    batch_size=tf_batch_inference_rows,
                    limit_first_n_chars=tf_limit_first_n_chars,
                    verbose_init=get_embeddings_verbose,
                ).reset_index()
                total_time_comms_vect += (
                    elapsed_time(t_start_comms_vect, log_label='df_comms-batch vectorizing', verbose=False) /
                    timedelta(minutes=1)
                )

                # Save single file locally & log to mlflow right away
                #  this might take longer to upload than uploading as a batch but makes the code easier to follow
                #  than having a giant try/except
                save_df_and_log_to_mlflow(
                    df=df_vect_comments,
                    local_path=path_this_model,
                    name_for_metric_and_artifact_folder=local_comms_subfolder_relative,
                    log_to_mlflow=log_each_batch_df_to_mlflow_invididually,
                    save_in_chunks=False,
                    df_single_file_name=f_comment_name_root,
                )
                del df_comments
                # Log partial metrics to mlflow so it's easier to know whether a job is still alive
                #  or dead
                count_comms_files_processed = count_comms_files_processed + 1
                mlflow.log_metrics(
                    {
                        local_comms_subfolder_relative: total_comments_count,
                        'total_comment_files_processed': count_comms_files_processed
                     }
                )
                gc.collect()

            mlflow.log_metrics(
                {local_comms_subfolder_relative: total_comments_count,
                 'vectorizing_time_minutes_comments': total_time_comms_vect,
                 'total_comment_files_processed': count_comms_files_processed
                 }
            )

            if total_comments_count == 0:
                logging.warning(f"No comments to process, can't log artifacts to mlflow")
            else:
                try:
                    # add manual meta file for comms
                    _, c = df_vect_comments.shape
                    f_meta = f"_manual_meta-{total_comments_count}_by_{c}.txt"
                    with open(Path(local_comms_subfolder_full) / f_meta, 'w') as f_:
                        f_.write(f"Original dataframe info\n==="
                                 f"\n{total_comments_count:9,.0f}\t | rows (comments)\n{c:9,.0f} | columns of LAST FILE\n")
                        f_.write(f"\nColumn list:\n{list(df_vect_comments.columns)}")
                except UnboundLocalError:
                    pass

                # No longer need to log all artifacts at the end b/c we're logging each file individually
                if not log_each_batch_df_to_mlflow_invididually:
                    info(f"Logging COMMENT files as mlflow artifact (to GCS)...")
                    mlflow.log_artifacts(str(local_comms_subfolder_full), local_comms_subfolder_relative)

                del df_vect_comments
            gc.collect()

        else:
            # In this branch we read all comments files into memory
            #  This process breaks down with 15+ million posts/commments
            #  TODO(djb): more work to figure out what's the threshold/limit
            info(f"Load comments df...")
            df_comments = pd.read_parquet(
                path=f"gs://{bucket_name}/{comments_path}",
                columns=l_cols_comments
            )
            info(f"  {df_comments.shape} <- df_comments shape")
            assert len(df_comments) == df_comments[col_comment_id].nunique()
            gc.collect()

            try:
                info(f"Keep only comments that match posts IDs in df_posts...")
                df_comments = df_comments[df_comments[col_post_id].isin(df_posts[col_post_id])]
                info(f"  {df_comments.shape} <- updated df_comments shape")
            except (TypeError, UnboundLocalError) as e:
                logging.warning(f"df_posts missing, so we can't filter comments...\n{e}")

            if n_sample_comments is not None:
                info(f"  Sampling COMMENTS down to: {n_sample_comments:,.0f}")
                df_comments = df_comments.sample(n=n_sample_comments)
                info(f"  {df_comments.shape} <- df_comments.shape AFTER sampling")

            info(f"Vectorizing COMMENTS...")
            t_start_comms_vect = datetime.utcnow()
            df_vect_comments = get_embeddings_as_df(
                model=model,
                df=df_comments,
                col_text=col_text_comment,
                cols_index='comment_default_',
                lowercase_text=tokenize_lowercase,
                batch_size=tf_batch_inference_rows,
                limit_first_n_chars=tf_limit_first_n_chars,
                verbose_init=get_embeddings_verbose,
            )
            total_time_comms_vect = elapsed_time(t_start_comms_vect, log_label='df_posts vectorizing', verbose=True)
            mlflow.log_metric('vectorizing_time_minutes_comments',
                              total_time_comms_vect / timedelta(minutes=1)
                              )
            del df_comments
            gc.collect()
            save_df_and_log_to_mlflow(
                df=df_vect_comments.reset_index(),
                local_path=path_this_model,
                name_for_metric_and_artifact_folder=mlflow_comments_folder,
            )

    # finish logging total time + end mlflow run
    total_fxn_time = elapsed_time(start_time=t_start_vectorize, log_label='Total vectorize fxn', verbose=True)
    mlflow.log_metric('vectorizing_time_minutes_full_function',
                      total_fxn_time / timedelta(minutes=1)
                      )
    # load log file into mlflow
    mlflow.log_artifact(f_log.f_log_file)
    mlflow.end_run()
    # Don't return anything b/c it's hard to predict output, instead check mlflow for artifacts


def get_embeddings_as_df(
        model: callable,
        df: pd.DataFrame,
        col_text: str = 'text',
        cols_index: Union[str, List[str]] = None,
        col_embeddings_prefix: Optional[str] = 'embeddings',
        lowercase_text: bool = False,
        batch_size: int = None,
        limit_first_n_chars: int = 1000,
        limit_first_n_chars_retry: int = 600,
        verbose: bool = True,
        verbose_init: bool = False,
) -> pd.DataFrame:
    """Get output of TF model as a dataframe.
    Besides batching we can get OOM (out of memory) errors if the text is too long,
    so we'll be adding a limit to only embed the first N-characters in a column.

    When called on a list a TF model runs in parallel, so use that instead of trying to
    get model output on a dataframe (which would be sequential and slow).
    For reference, on 5,400 sentences:
    - ~2 seconds:   on list
    - ~1 minute:    on text column df['text'].apply(model)

    TODO(djb):  For each recursive call, use try/except!!
      That way if one batch fails, the rest of the batches can proceed!
    """
    if cols_index == 'comment_default_':
        cols_index = ['subreddit_name', 'subreddit_id', 'post_id', 'comment_id']
    elif cols_index == 'post_default_':
        cols_index = ['subreddit_name', 'subreddit_id', 'post_id']
    elif cols_index == 'subreddit_default_':
        cols_index = ['subreddit_name', 'subreddit_id']
    else:
        pass

    if cols_index is not None:
        index_output = df[cols_index]
    else:
        index_output = None

    if batch_size is None:
        iteration_chunks = None
    elif batch_size >= len(df):
        iteration_chunks = None
    else:
        iteration_chunks = range(1 + len(df) // batch_size)

    if verbose_init:
        info(f"cols_index: {cols_index}")
        info(f"col_text: {col_text}")
        info(f"lowercase_text: {lowercase_text}")
        info(f"limit_first_n_chars: {limit_first_n_chars}")
        info(f"limit_first_n_chars_retry: {limit_first_n_chars_retry}")

    gc.collect()
    if iteration_chunks is None:
        if lowercase_text:
            series_text = df[col_text].str.lower().str[:limit_first_n_chars]
        else:
            series_text = df[col_text].str[:limit_first_n_chars]

        # In tf 2.3.4 it's faster to NOT use a list comprehension
        #  These seem equivalent:
        #   - np.array(model(series_text.to_list()))
        #   - model(series_text.to_list()).numpy()
        # df_vect = pd.DataFrame(
        #     np.array([emb.numpy() for emb in model(series_text.to_list())])
        # )
        df_vect = pd.DataFrame(
            model(series_text.to_list()).numpy()
        )
        if index_output is not None:
            # Remember to reset the index of the output!
            #   Because pandas will do an inner join based on index
            df_vect = pd.concat(
                [df_vect, index_output.reset_index(drop=True)],
                axis=1,
            ).set_index(cols_index)

        if col_embeddings_prefix is not None:
            # renaming can be expensive when we're calling the function recursively
            # so only rename after all individual dfs are created
            return df_vect.rename(
                columns={c: f"{col_embeddings_prefix}_{c}" for c in df_vect.columns}
            )
        else:
            return df_vect

    else:
        gc.collect()
        # This seems like a good place for recursion(!)
        # Renaming can be expensive when we're calling the function recursively
        #   so only rename after all individual dfs are created
        if verbose:
            info(f"Getting embeddings in batches of size: {batch_size}")
        l_df_embeddings = list()
        for i in LogTQDM(
                iteration_chunks, mininterval=11, ascii=True,  ncols=80, position=0, leave=True,
                logger=log
                ):
            try:
                l_df_embeddings.append(
                    get_embeddings_as_df(
                        model=model,
                        df=df.iloc[i * batch_size:(i + 1) * batch_size],
                        col_text=col_text,
                        cols_index=cols_index,
                        col_embeddings_prefix=None,
                        lowercase_text=lowercase_text,
                        batch_size=None,
                        limit_first_n_chars=limit_first_n_chars,
                    )
                )
                gc.collect()
            except errors.ResourceExhaustedError as e:
                logging.warning(f"\nResourceExhausted, lowering character limit\n{e}\n")
                l_df_embeddings.append(
                    get_embeddings_as_df(
                        model=model,
                        df=df.iloc[i * batch_size:(i + 1) * batch_size],
                        col_text=col_text,
                        cols_index=cols_index,
                        col_embeddings_prefix=None,
                        lowercase_text=lowercase_text,
                        batch_size=None,
                        limit_first_n_chars=limit_first_n_chars_retry,
                    )
                )
                gc.collect()
        if col_embeddings_prefix is not None:
            df_vect = pd.concat(l_df_embeddings, axis=0, ignore_index=False)
            return df_vect.rename(
                columns={c: f"{col_embeddings_prefix}_{c}" for c in df_vect.columns}
            )
        else:
            gc.collect()
            return pd.concat(l_df_embeddings, axis=0, ignore_index=False)


def save_df_and_log_to_mlflow(
        df: pd.DataFrame,
        local_path: Union[Path, str],
        name_for_metric_and_artifact_folder: str,
        target_mb_size: int = None,
        write_index: bool = True,
        log_to_mlflow: bool = True,
        save_in_chunks: bool = True,
        df_single_file_name: str = 'df',  # append parquet extension later
        verbose: bool = True,
) -> None:
    """
    Convenience function for vectorized dfs: save & log them to mlflow.
    Args:
        df:
            df to save & log
        local_path:
            path for local folder to save df
        name_for_metric_and_artifact_folder:
            e.g., df_vect_posts, df_vect_comments, df_vect_sub_meta

        target_mb_size:
            how big should the target files be? This input size tends to be smaller than
            the size in disk. e.g., if input is 75MB, file size might be ~100MB
        write_index:
            Do we write the index of the file?

        log_to_mlflow:
            If we're saving files in batch, we don't want to log each one,
            we'd rather log the whole folder AFTER processing all files
        save_in_chunks:
            If we're saving files in batch, we'll only save one output file per input file
            because dask doesn't give us a way to rename chunked files and could create name
            collisions.
        df_single_file_name:
            If we're saving files in batch, we'll only save one output file per input file.
            Use this name to map input file to output file.

    Returns: None
    """
    local_subfolder = Path(local_path) / name_for_metric_and_artifact_folder
    Path.mkdir(local_subfolder, exist_ok=True, parents=True)

    r, c = df.shape
    if verbose:
        info(f"  Saving to local: {name_for_metric_and_artifact_folder}/{df_single_file_name}"
             f" | {r:,.0f} Rows by {c:,.0f} Cols")

    if log_to_mlflow:
        mlflow.log_metric(f'{name_for_metric_and_artifact_folder}_rows', r)
        mlflow.log_metric(f'{name_for_metric_and_artifact_folder}_cols', c)

    if save_in_chunks:
        # save text file with metadata, because dask doesn't let us configure naming parquet files
        f_meta = f"_manual_meta-{r}_by_{c}.txt"
        with open(Path(local_subfolder) / f_meta, 'w') as f_:
            f_.write(f"Original dataframe info\n===\nrows: {r:,.0f}\ncolumns: {c:,.0f}\n")
            f_.write(f"\nColumn list:\n{list(df.columns)}")

        save_pd_df_to_parquet_in_chunks(
            df=df,
            path=local_subfolder,
            target_mb_size=target_mb_size,
            write_index=write_index,
        )
    else:
        # save as single parquet file using pandas
        f_df_vect_posts = Path(local_subfolder) / f'{df_single_file_name}-{r}_by_{c}.parquet'
        df.to_parquet(f_df_vect_posts)

    if log_to_mlflow:
        info(f"  Logging to mlflow...")
        mlflow.log_artifacts(str(local_subfolder), name_for_metric_and_artifact_folder)


#
# ~ fin
#
