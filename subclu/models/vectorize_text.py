"""
Functions & models to vectorize text.

For FSE (uSIF & SIF), we might also need to "train" a model, but the focus
of these models is just to vectorize without fine-tuning or retraining a language
model. That'll be a separate job/step.
"""
from datetime import datetime
from logging import info
from typing import Union, Tuple, List

from fse import CSplitCIndexedList, SplitCIndexedList
import mlflow
import pandas as pd
from sklearn.pipeline import Pipeline

from .preprocess_text import TextPreprocessor
from .registry_cpu import D_MODELS_CPU, D_CUSTOM_SPLIT
from ..utils.eda import elapsed_time
from ..utils.mlflow_logger import MlflowLogger


def vectorize_text_to_embeddings(
        model_name: str = 'fasttext_usif_de',
        tokenize_function: Union[str, callable] = 'sklearn_lower',
        bucket_name: str = 'i18n-subreddit-clustering',
        posts_path: str = 'posts/2021-05-19',
        comments_path: str = 'comments/2021-05-19',
        col_text_post: str = 'text',
        col_text_post_word_count: str = 'text_word_count',
        col_text_post_url: str = 'post_url_for_embeddings',
        col_post_id: str = 'post_id',
        col_text_comment: str = 'comment_body_text',
        col_text_comment_word_count: str = 'comment_text_word_count',
        model_kwargs: dict = None,

        train_subreddits_to_exclude: List[str] = None,
        train_exclude_duplicated_posts: bool = True,
        train_post_min_word_count: int = 2,
        # train_use_comments

):
    """"""
    # run inference to vectorize the text in:
    # - posts_path[col_text_post]
    # - posts_path[col_text_post_url]
    # - comments_path[col_text_comment]
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
        col_text_post,
        col_text_post_word_count,
        col_text_post_url,
    ]

    t_start_posts = datetime.utcnow()
    info(f"Loading df_posts..."
         f"\n  gs://{bucket_name}/{posts_path}")
    df_posts = pd.read_parquet(
        path=f"gs://{bucket_name}/{posts_path}",
        columns=l_cols_posts
    )
    elapsed_time(t_start_posts, log_label='df_post', verbose=True)
    info(f"{df_posts.shape} <- df_posts.shape")
    assert len(df_posts) == df_posts[col_post_id].nunique()

    if 'fasttext' in model_name:
        info(f"Filtering posts for SIF training...")
        if train_subreddits_to_exclude is None:
            train_subreddits_to_exclude = list()
        mask_exclude_subs = ~(df_posts['subreddit_name'].isin(train_subreddits_to_exclude))

        if train_exclude_duplicated_posts:
            mask_drop_dupe_text = ~(df_posts[col_text_post].duplicated(keep='first'))
        else:
            mask_drop_dupe_text = [True] * len(df_posts)

        mask_min_word_count = df_posts[col_text_post_word_count] >= train_post_min_word_count

        info(f"{(~mask_exclude_subs).sum():6,.0f} <- Posts to exclude because of: subreddits filter")
        info(f"{(~mask_drop_dupe_text).sum():6,.0f} <- Posts to exclude because of: duplicated posts")
        info(f"{(~mask_min_word_count).sum():6,.0f} <- Posts to exclude because of: minimum word count")

        # df_posts_train = df_posts[
        #     mask_exclude_subs & mask_drop_dupe_text & mask_min_word_count
        # ]
        n_training_posts = (mask_exclude_subs & mask_drop_dupe_text & mask_min_word_count).sum()
        info(f"{n_training_posts} <- df_posts for training")

        # FSE expectes a 0-based list so we need to
        #  sort the df so that the training posts are first (at the top of the df)
        #  otherwise we need to process the text twice.
        df_posts = pd.concat(
            df_posts[
                (mask_exclude_subs & mask_drop_dupe_text & mask_min_word_count)
            ],
            df_posts[
                ~(mask_exclude_subs & mask_drop_dupe_text & mask_min_word_count)
            ]
        )
        info(f"Convert df_train to fse format...")
        indexed_posts, d_id_to_ix, d_ix_to_id = process_text_for_fse(
            df_posts, col_text=col_text_post,
            col_id_to_map=col_post_id,
            custom_split_fxn=tokenize_function,
        )

        # TODO(djb): start mlflow experiment
        info(f"Loading model {model_name}..."
             f"\n  with kwargs: {model_kwargs}")
        model = Pipeline(
            [
                ('preprocess',
                 TextPreprocessor(
                    tokenizer=D_CUSTOM_SPLIT[tokenize_function])
                 )
                ('model', D_MODELS_CPU[model_name](**model_kwargs))
            ]

        )

        info(f"Start training fse model...")
        model.train(indexed_posts[:n_training_posts])

        # TODO(djb): save model
        # path_this_ft_model = get_project_subfolder(
        #     f"data/models/fse/{datetime.utcnow().strftime('%Y-%m-%d_%H')}"
        # )
        # fse_usif.save(str(path_this_ft_model / 'fse_usif_model_trained'))

        info(f"Running inference on all posts...")








    else:
        # TODO(djb): work on implementing use, bert or other models
        raise NotImplementedError

    elapsed_time(start_time=t_start_vectorize, log_label='Total vectorize fxn', verbose=True)
    return df_posts


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

    return indexed_text, d_id_to_ix, d_ix_to_id


def vectorize_text_with_fse(
        model,
        fse_processed_text,
        df_to_merge: pd.DataFrame = None,
        dict_index_to_id: dict = None,
        col_id_to_map: str = 'post_id',
        col_embeddings_prefix: str = 'embeddings',
        cols_index: Union[str, List[str]] = 'post_default',
) -> pd.DataFrame:
    """"""
    # wrapper to vectorize posts/comments AND merge back to df (if not None)
    if cols_index == 'post_default':
        # TODO(djb): add defaut columns
        cols_index = ['subreddit_name', 's']

    vectorized_posts = model.infer(fse_processed_text)
    info(f"{vectorized_posts.shape} <- Raw vectorized text shape")

    df_vect = pd.DataFrame(vectorized_posts)
    df_vect = df_vect.rename(columns={c: f"{col_embeddings_prefix}_{c}" for c in df_vect.columns})

    if df_to_merge is not None:
        # TODO(djb): merge vect w/ df_to_merge to get index data so we can verify process
        pass

    return df_vect




#
# ~ fin
#
