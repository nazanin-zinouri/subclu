"""
Class & functions to get embeddings from text with USE-multilingual.
Meant to be used in kubeflow but should be flexible enough to be used outside of it too.

- Only meant for USE or other tensor-hub models (Not meant to use FSE/FastText)
"""
import gc
import logging
from datetime import datetime, timedelta
from logging import info
from pathlib import Path
from typing import Union, List, Optional

# import mlflow
# import pandas as pd
# import numpy as np
# from sklearn.pipeline import Pipeline
# from tqdm import tqdm
import pandas as pd
import tensorflow_hub as hub
from tensorflow import errors

import hydra
from hydra.utils import get_original_cwd
from omegaconf import DictConfig

from ..utils.eda import elapsed_time
from ..utils.data_loaders_sql import LoadSubredditsSQL
from ..utils.tqdm_logger import FileLogger, LogTQDM

log = logging.getLogger(__name__)


# we're going to use hydra to set default parameter values
@hydra.main(config_path='../config', config_name="vectorize_subreddits_test")
def vectorize_text(
        cfg: DictConfig,
        return_object: bool = False
) -> Union[None, object]:
    """
    The hydra runner will call the vectorizing class using kwargs
    Note: by default we DO NOT return the cluster object because because the
      object needs to be pickle-able, otherwise we'll get errors if you try
      to do a multi-run job with hydra+joblib

    Args:
        cfg: hydra/omegaconf dictionary configuration

        return_object:
            whether to return the clustering object. By default, set to False
            because setting to True can result in errors when doing multi-run

    Returns:
        By default, set to False (return None)
            because the object needs to be pickle-able, otherwise
            we'll get errors if you try to do a multi-run job with hydra+joblib
    """
    print(f"CFG keys: {cfg.keys()}")

    thing_to_vectorize = cfg['thing_to_vectorize']
    log.info(f"Creating vectorizing class for {thing_to_vectorize}...")
    # We expect only one type of thing to be vectorized per function
    #  e.g., either subreddit meta, posts, or comments, but not a combination of them
    vect = VectorizeText(
        data_loader_name=cfg['data_text'][thing_to_vectorize]['data_loader_name'],
        data_loader_kwargs=cfg['data_text'][thing_to_vectorize]['data_loader_kwargs'],
        **{k: v for k, v in cfg.items() if k not in ['data_test']}
    )

    vect.get_embeddings()

    if return_object:
        return vect


class VectorizeText:
    """
    Class to vectorize text, assumes input is a data loader class + args for the data class
    For now it works with USE-multilingual. In the future we want to try different model types
    """

    def __init__(
            self,
            model_name: str,
            data_loader_name: str,
            col_text_for_embeddings: str,
            cols_index: Union[str, iter],
            output_bucket: str,
            output_folder: str,
            data_loader_kwargs: dict = None,
            run_id: str = None,
            tokenize_lowercase: bool = False,
            batch_inference_rows: int = 2000,
            limit_first_n_chars: int = 1000,
            get_embeddings_verbose: bool = False,
            verbose: bool = False,
            **kwargs
    ) -> None:
        """"""
        DATA_LOADERS = {
            'LoadSubredditsSQL': LoadSubredditsSQL,
        }
        self.model_name = model_name
        self.col_text_for_embeddings = col_text_for_embeddings
        self.data_loader_name = data_loader_name
        self.output_bucket = output_bucket
        self.output_folder = output_folder

        self.tokenize_lowercase = tokenize_lowercase
        self.batch_inference_rows = batch_inference_rows
        self.limit_first_n_chars = limit_first_n_chars
        self.get_embeddings_verbose = get_embeddings_verbose
        self.cols_index = cols_index
        self.verbose = verbose

        self.data_loader = DATA_LOADERS[data_loader_name](
            **data_loader_kwargs
        )
        # set start time so we can use timestamp when saving outputs
        if run_id is None:
            self.run_id = f"{datetime.utcnow().strftime('%Y-%m-%d_%H%M%S')}"
        else:
            self.run_id = run_id

    def get_embeddings(self) -> pd.DataFrame:
        """Run process to get embeddings"""
        t_start_vectorize = datetime.utcnow()
        info(f"Start vectorize function")

        # TODO(djb): load model
        log.info(f"Lodaing model: {self.model_name}")
        model = self._load_model()
        log.info(f"Model loaded")

        # TODO(djb): iterate over N files to get the text & get embeddings
        #  will move away from SQL and back to GCS b/c files make it easier to run
        #  in parallel

        # get text
        df_text = self.data_loader.get_as_dataframe()

        print(df_text[self.col_text_for_embeddings].head())

        info(f"Vectorizing subreddit descriptions...")
        t_start_subs_vect = datetime.utcnow()
        df_vect = get_embeddings_as_df(
            model=model,
            df=df_text,
            col_text=self.col_text_for_embeddings,
            cols_index=self.cols_index,
            lowercase_text=self.tokenize_lowercase,
            batch_size=self.batch_inference_rows,
            limit_first_n_chars=self.limit_first_n_chars,
            verbose_init=self.get_embeddings_verbose,
        )
        total_time_subs_vect = elapsed_time(t_start_subs_vect, log_label='df_subs vectorizing', verbose=True)
        # mlflow.log_metric('vectorizing_time_minutes_subreddit_meta',
        #                   total_time_subs_vect / timedelta(minutes=1)
        #                   )
        if self.verbose:
            log.info(f"{df_vect.shape} <- df_vect.shape")
        print(df_vect.head())
        # TODO(djb): save embeddings
        self._save_embeddings(df_vect)

        # finish logging total time + end mlflow run
        total_fxn_time = elapsed_time(start_time=t_start_vectorize, log_label='Total vectorize fxn', verbose=True)

        # mlflow.log_metric('vectorizing_time_minutes_full_function',
        #                   total_fxn_time / timedelta(minutes=1)
        #                   )

        return df_vect

    def _load_model(self):
        """Load model based on input
        For some reason, you might need to import tensorflow_text,
        even if you don't use it.

        github:
        https://github.com/tensorflow/tensorflow/issues/38597
        https://github.com/tensorflow/hub/issues/463
        """
        import tensorflow_text
        D_MODELS_TF_HUB = {
            'use_multilingual_large_3': "https://tfhub.dev/google/universal-sentence-encoder-multilingual-large/3",
            'use_multilingual_3': "https://tfhub.dev/google/universal-sentence-encoder-multilingual/3",
        }
        return hub.load(D_MODELS_TF_HUB[self.model_name])

    def _save_embeddings(
            self,
            df_vect: pd.DataFrame,
            df_single_file_name: str = 'df',
            add_shape_to_name: bool = True,
    ):
        """save df embeddings"""
        #  for now, save straight to GCS, in the future shift to mlflow
        #  so we'd have to save to local first
        f_gcs_path = f"gcs://{self.output_bucket}/{self.output_folder}/{self.run_id}"
        if add_shape_to_name:
            r, c = df_vect.shape
            f_gcs_name = f"{f_gcs_path}/{df_single_file_name}-{r}_by_{c}.parquet"
        else:
            f_gcs_name = f"{f_gcs_path}/{df_single_file_name}.parquet"
        log.info(f"Saving df_embeddings to: {f_gcs_name}")
        df_vect.to_parquet(
            f_gcs_name
        )




def get_embeddings_as_df(
        model: callable,
        df: pd.DataFrame,
        col_text: str = 'text',
        cols_index: Union[str, List[str]] = None,
        col_embeddings_prefix: Optional[str] = 'embeddings',
        lowercase_text: bool = False,
        batch_size: int = 2000,
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
                iteration_chunks, mininterval=11, ascii=True,  ncols=80,  # position=0, leave=True,
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


if __name__ == "__main__":
    vectorize_text()


#
# ~fin
#
