"""
Functions & models to vectorize text.

For FSE (uSIF & SIF), we might also need to "train" a model, but the focus
of these models is just to vectorize without fine-tuning or retraining a language
model. That'll be a separate job/step.
"""
import gc
import logging
from datetime import datetime, timedelta
from functools import partial
from logging import info
from pathlib import Path
from typing import Union, Tuple, List, Optional

from fse import CSplitCIndexedList
import mlflow
import pandas as pd
import numpy as np
# from sklearn.pipeline import Pipeline
from tqdm.auto import tqdm

from .preprocess_text import transform_and_tokenize_text
from .registry_cpu import D_MODELS_CPU
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

        model_kwargs: dict = None,

        train_subreddits_to_exclude: List[str] = None,
        train_exclude_duplicated_docs: bool = True,
        train_min_word_count: int = 2,
        train_use_comments: bool = False,

        tf_batch_inference_rows: int = 1800,
        tf_limit_first_n_chars: int = 1200,

        n_sample_posts: int = None,
        n_sample_comments: int = None,
) -> Tuple[callable, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    preprocess_text_folder options:
    - 'lowercase', 'remove_digits', 'lowercase_and_remove_digits'
    
    run inference to vectorize the text in:
    - posts_path[col_text_post]
    - posts_path[col_text_post_url]
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

        'train_exclude_duplicated_docs': train_exclude_duplicated_docs,
        'train_min_word_count': train_min_word_count,
        'train_use_comments': train_use_comments,

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
    if (model_kwargs is None) and ('fasttext' in model_name):
        model_kwargs = {
            'lang_id': 'de',
            'workers': 10,
            'length': 11,
            'lang_freq': 'de',
            'verbose': True,
        }

    if 'fasttext' in model_name:
        path_this_model = get_project_subfolder(
            f"data/models/fse/{datetime.utcnow().strftime('%Y-%m-%d_%H%M')}"
        )
    else:
        path_this_model = get_project_subfolder(
            f"data/models/{model_name}/{datetime.utcnow().strftime('%Y-%m-%d_%H%M')}"
        )
    Path(path_this_model).mkdir(exist_ok=True, parents=True)
    info(f"  Local model saving directory: {path_this_model}")

    df_posts, df_comments, df_subs = None, None, None
    if posts_path is not None:
        info(f"Loading df_posts..."
             f"\n  gs://{bucket_name}/{posts_path}")
        t_start_posts = datetime.utcnow()
        df_posts = pd.read_parquet(
            path=f"gs://{bucket_name}/{posts_path}",
            columns=l_cols_posts
        )
        if preprocess_text_folder:
            pass
            # TODO(djb): maybe load text one time instead of loading & dropping
            # info(f"Loading preprocessed text: {preprocess_text_folder}")
            # df_posts = df_posts.drop([col_text_post]).merge(
            #     pd.read_parquet(
            #         path=f"gs://{bucket_name}/{posts_path}",
            #         columns=[col_post_id, col_text_post]
            #     ),
            #     how='left',
            #     on=[col_post_id]
            # )
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
            info(f"  Sampling posts down to: {n_sample_comments:,.0f}")
            df_comments = df_comments.sample(n=n_sample_comments)
            info(f"  {df_comments.shape} <- df_posts.shape AFTER sampling")

    if subreddits_path is not None:
        info(f"Load subreddits df...")
        df_subs = pd.read_parquet(
            path=f"gs://{bucket_name}/{subreddits_path}",
            columns=l_cols_subreddits
        )
        info(f"  {df_subs.shape} <- df_subs shape")
        assert len(df_subs) == df_subs[col_subreddit_id].nunique()

    mlf = MlflowLogger()
    info(f"MLflow tracking URI: {mlflow.get_tracking_uri()}")
    mlf.set_experiment(mlflow_experiment)
    mlflow.start_run(run_name=run_name)
    mlf.add_git_hash_to_active_run()
    mlf.set_tag_hostname(key='host_name')
    mlf.log_param_hostname(key='host_name')

    df_vect, df_vect_comments, df_vect_subs = None, None, None

    if 'fasttext' in model_name:
        mlflow.log_params(d_params_to_log)

        if posts_path is not None:
            info(f"Filtering posts for SIF training...")
            if train_subreddits_to_exclude is None:
                train_subreddits_to_exclude = list()
            mask_exclude_subs = ~(df_posts['subreddit_name'].isin(train_subreddits_to_exclude))

            if train_exclude_duplicated_docs:
                mask_drop_dupe_text = ~(df_posts[col_text_post].duplicated(keep='first'))
            else:
                mask_drop_dupe_text = [True] * len(df_posts)

            mask_min_word_count = (df_posts[col_text_post_word_count] >= train_min_word_count)

            info(f"{(~mask_exclude_subs).sum():6,.0f} <- Exclude posts because of: subreddits filter")
            info(f"{(~mask_drop_dupe_text).sum():6,.0f} <- Exclude posts because of: duplicated posts")
            info(f"{(~mask_min_word_count).sum():6,.0f} <- Exclude posts because of: minimum word count")

            n_training_docs = int((mask_exclude_subs & mask_drop_dupe_text & mask_min_word_count).sum())
            info(f"{n_training_docs:6,.0f} <- df_posts for training")

            # FSE expects a 0-based list so we need to
            #  sort the df so that the training posts are first (at the top of the df)
            #  otherwise we need to process the text twice.
            df_posts = pd.concat(
                [
                    df_posts[
                        (mask_exclude_subs & mask_drop_dupe_text & mask_min_word_count)
                    ],
                    df_posts[
                        ~(mask_exclude_subs & mask_drop_dupe_text & mask_min_word_count)
                    ],
                ],
                ignore_index=False,
            )
            info(f"Converting df_train to fse format...")
            t_start_fse_format = datetime.utcnow()
            indexed_posts, d_ix_to_id, _ = process_text_for_fse(
                df_posts, col_text=col_text_post,
                col_id_to_map=col_post_id,
                custom_split_fxn=partial(
                    transform_and_tokenize_text, tokenizer=tokenize_function,
                    lowercase=tokenize_lowercase
                ),
            )
            indexed_train_docs = indexed_posts
            elapsed_time(t_start_fse_format, log_label='Converting to fse', verbose=True)
            mlflow.log_param('training_data', 'post_title_and_body')

        else:
            # Otherwise, vectorize & train on subreddit meta
            info(f"Filtering posts for SIF training...")
            if train_subreddits_to_exclude is None:
                train_subreddits_to_exclude = list()
            mask_exclude_subs = ~(df_subs['subreddit_name'].isin(train_subreddits_to_exclude))

            if train_exclude_duplicated_docs:
                mask_drop_dupe_text = ~(df_subs[col_text_subreddit_description].duplicated(keep='first'))
            else:
                mask_drop_dupe_text = [True] * len(df_subs)

            mask_min_word_count = (df_subs[col_text_subreddit_word_count] >= train_min_word_count)

            info(f"{(~mask_exclude_subs).sum():6,.0f} <- Exclude posts because of: subreddits filter")
            info(f"{(~mask_drop_dupe_text).sum():6,.0f} <- Exclude posts because of: duplicated posts")
            info(f"{(~mask_min_word_count).sum():6,.0f} <- Exclude posts because of: minimum word count")

            n_training_docs = int((mask_exclude_subs & mask_drop_dupe_text & mask_min_word_count).sum())
            info(f"{n_training_docs:6,.0f} <- df_subs for training")

            # FSE expects a 0-based list so we need to
            #  sort the df so that the training posts are first (at the top of the df)
            #  otherwise we need to process the text twice.
            df_subs = pd.concat(
                [
                    df_subs[
                        (mask_exclude_subs & mask_drop_dupe_text & mask_min_word_count)
                    ],
                    df_subs[
                        ~(mask_exclude_subs & mask_drop_dupe_text & mask_min_word_count)
                    ],
                ],
                ignore_index=False,
            )
            info(f"Converting df_train to fse format...")
            t_start_fse_format = datetime.utcnow()
            indexed_subs, d_ix_to_id, _ = process_text_for_fse(
                df_subs, col_text=col_text_subreddit_description,
                col_id_to_map=col_subreddit_id,
                custom_split_fxn=partial(
                    transform_and_tokenize_text, tokenizer=tokenize_function,
                    lowercase=tokenize_lowercase
                ),
            )
            indexed_train_docs = indexed_subs
            elapsed_time(t_start_fse_format, log_label='Converting to fse', verbose=True)
            mlflow.log_param('training_data', 'subreddit_description')

        mlflow.log_metric('training_docs_count', n_training_docs)

        info(f"Logging training df to mlflow...")
        # We only need to save the ix to ID because the other
        # dict is the inverse
        d_artifact_paths = {
            'd_ix_to_id': {
                'obj': d_ix_to_id,
                'path': str(path_this_model / 'd_ix_to_id.csv'),
                'col_id': col_post_id if posts_path is not None else col_subreddit_id
            },
        }
        for name_, d_ in d_artifact_paths.items():
            # Saving as a dataframe because pandas can automagically read
            # from GCS instead of having to download the file locally
            (
                pd.DataFrame(list(d_['obj'].items()),
                             columns=['training_index', d_['col_id']])
                .to_csv(d_['path'], index=False)
            )
            mlflow.log_artifact(d_['path'], name_)
            del name_, d_

        info(f"Loading model: {model_name}..."
             f"\n  with kwargs: {model_kwargs}")
        model = D_MODELS_CPU[model_name](**model_kwargs)
        elapsed_time(t_start_fse_format, log_label='Load FSE model', verbose=True)

        info(f"Start training fse model...")
        model.train([indexed_train_docs[i] for i in range(n_training_docs)])

        # TODO(djb): save model; Skip for now, only save when we have a good one
        #  otherwise each model is like over 10GB
        # fse_usif.save(str(path_this_ft_model / 'fse_usif_model_trained'))

        if posts_path is not None:
            info(f"Running inference on all POSTS...")
            df_vect = vectorize_text_with_fse(
                model=model,
                fse_processed_text=indexed_posts,
                df_to_merge=df_posts,
                dict_index_to_id=d_ix_to_id,
                col_id_to_map=col_post_id,
                cols_index='post_default',
            )
            save_df_and_log_to_mlflow(
                df=df_vect,
                local_path=path_this_model,
                df_filename='df_vectorized_posts',
                name_for_metric_and_artifact_folder='df_vect_posts',
            )
            del df_posts
            gc.collect()

        if subreddits_path is not None:
            info(f"Running inference on all SUBREDDIT description...")
            df_vect_subs = vectorize_text_with_fse(
                model=model,
                fse_processed_text=indexed_subs,
                df_to_merge=df_subs,
                dict_index_to_id=d_ix_to_id,
                col_id_to_map=col_subreddit_id,
                cols_index='subreddit_default',
                verbose=True
            )
            save_df_and_log_to_mlflow(
                df=df_vect_subs,
                local_path=path_this_model,
                df_filename='df_vect_subreddits_description',
                name_for_metric_and_artifact_folder='df_vect_subreddits_description',
            )
            del df_subs
            gc.collect()

        if comments_path is not None:
            info(f"Get vectors for comments")
            t_start_comment_vec = datetime.utcnow()
            indexed_comments, d_ix_to_id_c, _ = process_text_for_fse(
                df_comments, col_text=col_text_comment,
                col_id_to_map=col_comment_id,
                custom_split_fxn=partial(
                    transform_and_tokenize_text, tokenizer=tokenize_function,
                    lowercase=tokenize_lowercase
                ),
            )
            # TODO(djb): instead of passing a dict, pass a df that has same 0-index
            #  and merge on index to avoid expensive lookup using dictionary
            #  to map IDs to index
            df_vect_comments = vectorize_text_with_fse(
                model=model,
                fse_processed_text=indexed_comments,
                df_to_merge=df_comments,
                dict_index_to_id=d_ix_to_id_c,
                col_id_to_map=col_comment_id,
                cols_index='comment_default',
            )
            elapsed_time(t_start_comment_vec, log_label='Inference time for COMMENTS', verbose=True)
            save_df_and_log_to_mlflow(
                df=df_vect_comments,
                local_path=path_this_model,
                df_filename='df_vectorized_comments',
                name_for_metric_and_artifact_folder='df_vect_comments',
            )
            del df_comments
            gc.collect()

    else:
        import tensorflow_hub as hub

        # drop some parameters that aren't used by USE (we don't train models)
        d_params_to_log = {k: v for k, v in d_params_to_log.items() if not k.startswith('train_')}
        mlflow.log_params(d_params_to_log)
        mlflow.log_param(f"model_location", D_MODELS_TF_HUB[model_name])

        t_start_hub_load = datetime.utcnow()
        info(f"Loading model {model_name}..."
             f"\n  with kwargs: {model_kwargs}")
        model = hub.load(D_MODELS_TF_HUB[model_name])
        elapsed_time(t_start_hub_load, log_label='Load TF HUB model', verbose=True)

        logging.warning(f"For TF-HUB models, the only preprocessing applied is lowercase()")
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


def save_df_and_log_to_mlflow(
        df: pd.DataFrame,
        local_path: Union[Path, str],
        df_filename: str,
        name_for_metric_and_artifact_folder: str,
) -> None:
    """
    Convenience function for vectorized dfs: save & log them to mlflow.
    Args:
        df:
            df to save & log
        local_path:
            path for local folder to save df
        df_filename:
            filename prefix, by default function will add shape of df & .parquet filetype
        name_for_metric_and_artifact_folder:
            e.g., df_vect_posts, df_vect_comments, df_vect_sub_meta

    Returns: None
    """
    r, c = df.shape
    mlflow.log_metric(f'{name_for_metric_and_artifact_folder}_rows', r)
    mlflow.log_metric(f'{name_for_metric_and_artifact_folder}_cols', c)

    info(f"  Saving to local... {name_for_metric_and_artifact_folder}...")
    f_df_vect_posts = Path(local_path) / f'{df_filename}-{r}_by_{c}.parquet'
    df.to_parquet(f_df_vect_posts)
    info(f"  Logging to mlflow...")
    mlflow.log_artifact(str(f_df_vect_posts), name_for_metric_and_artifact_folder)


def get_embeddings_as_df(
        model: callable,
        df: pd.DataFrame,
        col_text: str = 'text',
        cols_index: Union[str, List[str]] = None,
        col_embeddings_prefix: Optional[str] = 'embeddings',
        lowercase_text: bool = False,
        batch_size: int = None,
        limit_first_n_chars: int = 1500,
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

    if iteration_chunks is None:
        if lowercase_text:
            series_text = df[col_text].str.lower().str[:limit_first_n_chars]
        else:
            series_text = df[col_text].str[:limit_first_n_chars]

        df_vect = pd.DataFrame(
            np.array([emb.numpy() for emb in model(series_text.to_list())])
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
        # This seems like a good place for recursion(!)
        # Renaming can be expensive when we're calling the function recursively
        #   so only rename after all individual dfs are created
        info(f"Getting embeddings in batches of size: {batch_size}")
        l_df_embeddings = list()
        for i in tqdm(iteration_chunks):
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
        if col_embeddings_prefix is not None:
            df_vect = pd.concat(l_df_embeddings, axis=0, ignore_index=False)
            return df_vect.rename(
                columns={c: f"{col_embeddings_prefix}_{c}" for c in df_vect.columns}
            )
        else:
            return pd.concat(l_df_embeddings, axis=0, ignore_index=False)


def process_text_for_fse(
        df: pd.DataFrame,
        col_text: str,
        col_id_to_map: str,
        custom_split_fxn: callable = None,
) -> Tuple[CSplitCIndexedList, dict, dict]:
    """"""
    # streamline converting df in text column into array needed for fse train & inference
    # The ID could be any ID that we could use to aggregate fse embeddings for:
    #   e.g., subreddit ID, post ID, comment ID; post_id might be the most common
    # When used for training, this function assumes that this df has already been filtered
    #  e.g., if we want to exclude duplicates or short posts/comments we'd do that at a
    #    separate step BEFORE this function
    # custom_split_fxn: if None -> it will apply ".split()" (split on white-space)
    # `CSplitCIndexedList` is the slowest one because it'll create a custom index
    #    & a custom split() fxn. If it's too slow may try some of the faster ones
    #    but they require preprocessing as a separate step that adds complexity and
    #    may end up taking longer

    d_id_to_ix = dict()
    d_ix_to_id = dict()
    for ix, post_id in enumerate(df[col_id_to_map]):
        d_id_to_ix[post_id] = ix
        d_ix_to_id[ix] = post_id

    indexed_text = CSplitCIndexedList(
        df[col_text].values,
        custom_index=[d_id_to_ix[post_id] for post_id in df[col_id_to_map]],
        custom_split=custom_split_fxn
    )
    return indexed_text, d_ix_to_id, d_id_to_ix


# def filter_text_for_fse_training(
#
# ):
#     """TODO, move filtering logic to a function, instead of a big block of code"""


def vectorize_text_with_fse(
        model,
        fse_processed_text,
        df_to_merge: pd.DataFrame = None,
        dict_index_to_id: dict = None,
        index_to_id_array: Union[iter, pd.Series] = None,
        col_id_to_map: str = 'post_id',
        col_embeddings_prefix: str = 'embeddings',
        cols_index: Union[str, List[str]] = 'post_default',
        verbose: bool = True,
) -> pd.DataFrame:
    """
    It looks like a lot of time was wasted looking up the post/comment-id
    to map it back to the
    Note that converting vectors to df can take a long time, for 111k posts:
        model.infer() could take 30 seconds
        but converting vectors to df & joining to df can take 4 minutes
    Even then, the added time is worth it because it makes it easy to
    be able add an index and join the data to the original post/comment

    Args:
        model:
        fse_processed_text:
        df_to_merge:
        dict_index_to_id:
        index_to_id_array:
        col_id_to_map:
        col_embeddings_prefix:
        cols_index:
        verbose:

    Returns:
    """
    # wrapper to vectorize posts/comments AND merge back to df (if not None)
    if cols_index == 'post_default':
        cols_index = ['subreddit_name', 'subreddit_id', 'post_id']
    if cols_index == 'comment_default':
        cols_index = ['subreddit_name', 'subreddit_id', 'post_id', 'comment_id']
    if cols_index == 'subreddit_default':
        cols_index = ['subreddit_name', 'subreddit_id']

    # go straight into df, maybe that can cut down time
    # vectorized_posts = model.infer(fse_processed_text)
    # info(f"{vectorized_posts.shape} <- Raw vectorized text shape")

    t_start_vec_to_df = datetime.utcnow()
    info(f"  Inference + convert to df...")
    df_vect = pd.DataFrame(model.infer(fse_processed_text))
    elapsed_time(t_start_vec_to_df, log_label='Raw inference+df only', verbose=True)
    info(f"    {df_vect.shape} <- Raw vectorized text shape")

    info(f"  Creating df from dict_index_to_id...")
    df_ix_to_id = pd.DataFrame(list(dict_index_to_id.items()),
                               columns=['index', col_id_to_map]
                               ).set_index('index')

    info(f"  Setting {col_id_to_map} as index...")
    df_vect = (
        df_vect
        .rename(columns={c: f"{col_embeddings_prefix}_{c}" for c in df_vect.columns})
        .merge(
            df_ix_to_id,
            how='left',
            left_index=True,
            right_index=True,
        )
    ).set_index(col_id_to_map)

    if df_to_merge is not None:
        info(f"  Merging df_vectors with df to get new index columns...")
        t_start_merging = datetime.utcnow()
        df_vect = (
            df_to_merge[cols_index]
            .merge(
                df_vect,
                how='right',
                left_on=[col_id_to_map],
                right_index=True,
            )
            .set_index(cols_index)
        )
        elapsed_time(t_start_merging, log_label=' Merging df_vect with ID columns', verbose=True)
    if verbose:
        elapsed_time(t_start_vec_to_df, log_label='Converting vectors to df FULL', verbose=True)

    return df_vect




#
# ~ fin
#
