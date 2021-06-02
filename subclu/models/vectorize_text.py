"""
Functions & models to vectorize text.

For FSE (uSIF & SIF), we might also need to "train" a model, but the focus
of these models is just to vectorize without fine-tuning or retraining a language
model. That'll be a separate job/step.
"""
from datetime import datetime, timedelta
from functools import partial
from logging import info
from pathlib import Path
from typing import Union, Tuple, List

from fse import CSplitCIndexedList
import mlflow
import pandas as pd
# from sklearn.pipeline import Pipeline

from .preprocess_text import transform_and_tokenize_text
from .registry_cpu import D_MODELS_CPU
from ..utils import get_project_subfolder
from ..utils.eda import elapsed_time
from ..utils.mlflow_logger import MlflowLogger


def vectorize_text_to_embeddings(
        model_name: str = 'fasttext_usif_de',
        tokenize_function: Union[str, callable] = 'sklearn',
        tokenize_lowercase: bool = False,

        bucket_name: str = 'i18n-subreddit-clustering',
        subreddits_path: str = None,
        posts_path: str = 'posts/2021-05-19',
        comments_path: str = 'comments/2021-05-19',
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
        mlflow_experiment: str = 'fse_vectorize_v1',
        train_use_comments: bool = False,

):
    """"""
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

    path_this_model = get_project_subfolder(
        f"data/models/fse/{datetime.utcnow().strftime('%Y-%m-%d_%H%M')}"
    )
    Path(path_this_model).mkdir(exist_ok=True, parents=True)
    info(f"  Local model saving directory: {path_this_model}")

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

    if comments_path is not None:
        info(f"Load comments df...")
        df_comments = pd.read_parquet(
            path=f"gs://{bucket_name}/{comments_path}",
            columns=l_cols_comments
        )
        info(f"  {df_comments.shape} <- df_comments shape")
        assert len(df_comments) == df_comments[col_comment_id].nunique()

        info(f"Keep only comments that match posts IDs in df_posts...")
        df_comments = df_comments[df_comments[col_post_id].isin(df_posts[col_post_id])]
        info(f"  {df_comments.shape} <- updated df_comments shape")

    if subreddits_path is not None:
        info(f"Load subreddits df...")
        df_subs = pd.read_parquet(
            path=f"gs://{bucket_name}/{subreddits_path}",
            columns=l_cols_subreddits
        )
        info(f"  {df_subs.shape} <- df_comments shape")
        assert len(df_subs) == df_subs[col_subreddit_id].nunique()

    if 'fasttext' in model_name:
        mlf = MlflowLogger()
        info(f"MLflow tracking URI: {mlflow.get_tracking_uri()}")
        mlf.set_experiment(mlflow_experiment)
        mlflow.start_run()
        mlf.add_git_hash_to_active_run()
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

        info(f"Loading model {model_name}..."
             f"\n  with kwargs: {model_kwargs}")
        model = D_MODELS_CPU[model_name](**model_kwargs)
        elapsed_time(t_start_fse_format, log_label='Load FSE model', verbose=True)

        info(f"Start training fse model...")
        model.train([indexed_train_docs[i] for i in range(n_training_docs)])

        # TODO(djb): save model; Skip for now, only save when we have a good one
        #  otherwise each model is like over 10GB
        # fse_usif.save(str(path_this_ft_model / 'fse_usif_model_trained'))

        if posts_path is not None:
            mlflow.log_metric('df_posts_len', len(df_posts))

            info(f"Running inference on all POSTS...")
            df_vect = vectorize_text_with_fse(
                model=model,
                fse_processed_text=indexed_posts,
                df_to_merge=df_posts,
                dict_index_to_id=d_ix_to_id,
                col_id_to_map=col_post_id,
                cols_index='post_default',
            )

            info(f"Saving inference for comments df")
            f_df_vect_posts = path_this_model / f'df_vectorized_posts-{len(df_vect)}.parquet'
            df_vect.to_parquet(f_df_vect_posts)
            mlflow.log_artifact(str(f_df_vect_posts), 'df_vect_posts')
            info(f"  Saving inference complete")

        if comments_path is not None:
            mlflow.log_metric('df_comments_len', len(df_comments))
            # TODO(djb): comments are stalling because of df_vect_comments
            #  function runs out or memory
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
            # TODO(djb)
            info(f"Save vectors for comments")
            f_df_vect_comments = path_this_model / f'df_vectorized_comments-{len(df_vect_comments)}.parquet'
            df_vect_comments.to_parquet(f_df_vect_comments)
            mlflow.log_artifact(str(f_df_vect_comments), 'df_vect_comments')

        if subreddits_path is not None:
            mlflow.log_metric('df_subs_len', len(df_subs))
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

            info(f"Saving inference for subreddits description df")
            f_df_vect_subs = path_this_model / f'df_vectorized_subreddits_description-{len(df_vect_subs)}.parquet'
            df_vect_subs.to_parquet(f_df_vect_subs)
            mlflow.log_artifact(str(f_df_vect_subs), 'df_vect_subreddits_description')
            info(f"  Saving inference complete")

        total_fxn_time = elapsed_time(start_time=t_start_vectorize, log_label='Total vectorize fxn', verbose=True)
        mlflow.log_metric('vectorizing_time_minutes',
                          total_fxn_time / timedelta(minutes=1)
                          )
        mlflow.end_run()

        if posts_path is not None:
            return model, df_posts, d_ix_to_id
        else:
            return model, df_subs, d_ix_to_id

    else:
        # TODO(djb): work on implementing use, bert or other models
        raise NotImplementedError



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
    info(f"Convert vectors to df...")
    df_vect = pd.DataFrame(model.infer(fse_processed_text))
    info(f"{df_vect.shape} <- Raw vectorized text shape")
    elapsed_time(t_start_vec_to_df, log_label='Raw vectorize to df only', verbose=True)

    info(f"Create new df from dict_index_to_id to make merging easier...")
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
        info(f"Merge vectors with df...")
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
        elapsed_time(t_start_merging, log_label='Merging df_vect with ID columns', verbose=True)
    if verbose:
        elapsed_time(t_start_vec_to_df, log_label='Converting vectors to df full', verbose=True)

    return df_vect




#
# ~ fin
#
