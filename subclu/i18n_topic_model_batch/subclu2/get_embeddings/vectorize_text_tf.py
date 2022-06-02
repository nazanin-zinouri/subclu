"""
Class & functions to get embeddings from text with USE-multilingual.
Meant to be used in kubeflow but should be flexible enough to be used outside of it too.

- Only meant for USE or other tensor-hub models (Not meant to use FSE/FastText)
"""
import gc
import logging
from datetime import datetime
from logging import info
import os
from pathlib import Path
import posixpath
from typing import Union, List, Optional

# import mlflow
from google.cloud import storage
import pandas as pd

import hydra
from hydra.utils import get_original_cwd
from omegaconf import DictConfig

from ..utils.eda import elapsed_time
from ..utils.data_loaders_gcs import LoadSubredditsGCS
from ..utils.tqdm_logger import FileLogger, LogTQDM


# hide TF debugging logs. Env Var BEFORE import tf works
#  1= keep warning & error, 2= keep error, 3= hide all
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '1'

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
    print(f"CFG keys:\n  {cfg.keys()}")

    # We expect only one type of thing to be vectorized per function
    #  e.g., either subreddit meta, posts, or comments, but not a combination of them

    # Share some top-level variables with the data loader

    # GCS_path is the key we'll use from the data_text config
    #  get full value hydra call instead of having to write it twice
    key_for_gcs_path = cfg['gcs_path_text_key']

    data_loader_kwargs_ = {
        **cfg['data_loader_kwargs'],
        **{
            'bucket_name': cfg['data_text']['bucket_name'],
            'gcs_path': cfg['data_text'][key_for_gcs_path],
            'local_cache_path': cfg['local_cache_path'],

            'n_sample_files': cfg.get('n_sample_files'),
            'n_files_slice_start': cfg.get('n_files_slice_start'),
            'n_files_slice_end': cfg.get('n_sample_files'),
        }
    }
    print(f"Data Loader kwags:")
    for k, v in data_loader_kwargs_.items():
        print(f"  {k}: {v}")
        del k, v

    vect = VectorizeText(
        data_loader_kwargs=data_loader_kwargs_,
        **{k: v for k, v in cfg.items() if k not in ['data_test', 'data_loader_kwargs']},
        **{'gcs_output_path': cfg['data_text'][key_for_gcs_path]}
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
            gcs_output_path: str,
            local_model_path: str,
            data_loader_kwargs: dict = None,
            run_id: str = None,
            tokenize_lowercase: bool = False,
            batch_inference_rows: int = 2000,
            limit_first_n_chars: int = 1800,
            limit_first_n_chars_retry: int = 700,
            n_sample_files: int = None,
            n_files_slice_start: int = None,
            n_files_slice_end: int = None,
            process_individual_files: bool = True,
            get_embeddings_verbose: bool = False,
            verbose: bool = False,
            **kwargs
    ) -> None:
        """"""
        DATA_LOADERS = {
            'LoadSubredditsGCS': LoadSubredditsGCS,
        }
        self.model_name = model_name
        self.col_text_for_embeddings = col_text_for_embeddings
        self.data_loader_name = data_loader_name
        self.output_bucket = output_bucket
        self.gcs_output_path = gcs_output_path
        self.local_model_path = local_model_path
        self.f_log_file = None

        self.tokenize_lowercase = tokenize_lowercase
        self.batch_inference_rows = batch_inference_rows
        self.limit_first_n_chars = limit_first_n_chars
        self.limit_first_n_chars_retry = limit_first_n_chars_retry
        self.get_embeddings_verbose = get_embeddings_verbose
        self.cols_index = cols_index
        self.verbose = verbose

        self.n_sample_files = n_sample_files
        self.n_files_slice_start = n_files_slice_start
        self.n_files_slice_end = n_files_slice_end
        self.process_individual_files = process_individual_files

        self.data_loader = DATA_LOADERS[data_loader_name](
            **data_loader_kwargs
        )
        self.data_loader_kwargs = data_loader_kwargs
        # set start time so we can use timestamp when saving outputs
        if run_id is None:
            self.run_id = f"{datetime.utcnow().strftime('%Y-%m-%d_%H%M%S')}"
        else:
            self.run_id = run_id

        # For now, save straight to GCS, in the future shift to mlflow
        #  so we'd have to save to local first
        # For full path we'd need to append `gcs://{self.output_bucket}/`
        self.gcs_output_path_this_run = (
            f"{self.gcs_output_path}/embedding/{self.run_id}"
        )

    def get_embeddings(self) -> None:
        """Run process to get embeddings
        TODO(djb): need to define what to return in case downstream steps need input from here.
         Maybe output path (GCS path) is the way to go?
        """
        t_start_vectorize = datetime.utcnow()
        self._set_path_local_model()

        info(f"Start vectorize function")

        # TODO(djb): load model
        log.info(f"Loading model: {self.model_name}")
        model = self._load_model()
        log.info(f"Model loaded")

        t_start_subs_vect = datetime.utcnow()
        if self.process_individual_files:
            # branch A: process each file individually to save RAM overhead
            log.info(f"  Loading & Processing each file independently")
            self.data_loader.local_cache()

            for f_, df_ in LogTQDM(
                self.data_loader.yield_files_and_dfs(),
                total=self.data_loader.n_local_parquet_files_,
                desc='Files in batch: ',
                mininterval=20, ascii=True,
                logger=log
            ):
                gc.collect()
                f_name = f_.name
                f_name_root = f_name.split('.')[0]
                info(f"  Processing: {f_name}")
                df_vect = self._vectorize_single_df(
                    df_text=df_,
                    model=model,
                )
                self._save_embeddings(
                    df_vect,
                    df_single_file_name=f_name_root,
                )

        else:
            # branch B: process input as one df
            log.info(f"Loading all files as a single df...")
            df_text = self.data_loader.read_as_one_df()

            df_vect = self._vectorize_single_df(
                df_text=df_text,
                model=model,
            )
            self._save_embeddings(df_vect)

        total_time_subs_vect = elapsed_time(t_start_subs_vect, log_label='df_subs vectorizing', verbose=True)
        # mlflow.log_metric('vectorizing_time_minutes_subreddit_meta',
        #                   total_time_subs_vect / timedelta(minutes=1)
        #                   )
        # TODO(djb): log the configuration used to create these embeddings
        # log hydra config
        self._log_hydra_config_and_log_file()

        # finish logging total time + end mlflow run
        total_fxn_time = elapsed_time(start_time=t_start_vectorize, log_label='Total vectorize fxn', verbose=True)

        # mlflow.log_metric('vectorizing_time_minutes_full_function',
        #                   total_fxn_time / timedelta(minutes=1)
        #                   )

    def _load_model(self):
        """Load model based on input
        For some reason, you might need to import tensorflow_text,
        even if you don't use it.

        github:
        https://github.com/tensorflow/tensorflow/issues/38597
        https://github.com/tensorflow/hub/issues/463
        """
        import tensorflow_hub as hub
        import tensorflow_text

        D_MODELS_TF_HUB = {
            'use_multilingual_large_3': "https://tfhub.dev/google/universal-sentence-encoder-multilingual-large/3",
            'use_multilingual_3': "https://tfhub.dev/google/universal-sentence-encoder-multilingual/3",
        }
        return hub.load(D_MODELS_TF_HUB[self.model_name])

    def _vectorize_single_df(
            self,
            df_text: pd.DataFrame,
            model: callable,
    ) -> pd.DataFrame:
        """Call to vectorize a single df.
        Call this method to make sure vectorization is standardized when
        we're calling vectorization on a series of dfs.

        In general, we want a high batch_size because that'll complete faster,
        but we need to reduce it when we deal with long text b/c it can overflow
        the GPU's memory and result in OOM errors.

        If a batch (n-rows in a file) fails, get_embeddings_as_df() will retry with
         a lower `limit_first_n_chars` value. However, that may not be enough if
         too many comments in a batch are really long. In that case, I have 2 try/excepts
         to reduce the `batch_size` which should make it more likely for a job
         to complete even if the input batch_size was too high.
        """
        # TODO(djb): concat multiple fields together
        #   if cols_comment_text_to_concat is not None:
        #     info(f"Create merged text column")
        #     df_comments[col_comment_text_to_concat] = ''
        #
        #     for col_ in LogTQDM(
        #             cols_comment_text_to_concat, ascii=True,
        #             logger=log
        #     ):
        #         mask_c_not_null = ~df_comments[col_].isnull()
        #         df_comments.loc[
        #             mask_c_not_null,
        #             col_comment_text_to_concat
        #         ] = (
        #                 df_comments[mask_c_not_null][col_comment_text_to_concat] + '. ' +
        #                 df_comments[mask_c_not_null][col_]
        #         )
        #     # remove the first 3 characters because they'll always be '. '
        #     df_comments[col_comment_text_to_concat] = df_comments[col_comment_text_to_concat].str[2:]
        # col_text_ = col_text_comment if cols_comment_text_to_concat is None else col_comment_text_to_concat

        info(f"Vectorizing column: {self.col_text_for_embeddings}")

        # reset_index() because multi-index is not well supported in parquet by many tools
        try:
            df_vect = get_embeddings_as_df(
                model=model,
                df=df_text,
                col_text=self.col_text_for_embeddings,
                cols_index=self.cols_index,
                lowercase_text=self.tokenize_lowercase,
                limit_first_n_chars=self.limit_first_n_chars,
                limit_first_n_chars_retry=self.limit_first_n_chars_retry,
                verbose_init=self.get_embeddings_verbose,
                batch_size=self.batch_inference_rows,
            ).reset_index()
        except Exception as e:
            try:
                logging.error(f"Failed to vectorize comments")
                logging.error(e)
                new_batch_size = int(self.batch_inference_rows * 0.75)
                info(f"*** Retrying with smaller batch size {new_batch_size}***")
                df_vect = get_embeddings_as_df(
                    model=model,
                    df=df_text,
                    col_text=self.col_text_for_embeddings,
                    cols_index=self.cols_index,
                    lowercase_text=self.tokenize_lowercase,
                    limit_first_n_chars=self.limit_first_n_chars,
                    limit_first_n_chars_retry=self.limit_first_n_chars_retry,
                    verbose_init=self.get_embeddings_verbose,
                    batch_size=new_batch_size,
                ).reset_index()
            except Exception as er:
                logging.error(f"Failed to vectorize comments")
                logging.error(er)
                new_batch_size = int(self.batch_inference_rows * 0.5)
                info(f"*** Retrying with smaller batch size {new_batch_size}***")
                df_vect = get_embeddings_as_df(
                    model=model,
                    df=df_text,
                    col_text=self.col_text_for_embeddings,
                    cols_index=self.cols_index,
                    lowercase_text=self.tokenize_lowercase,
                    limit_first_n_chars=self.limit_first_n_chars,
                    limit_first_n_chars_retry=self.limit_first_n_chars_retry,
                    verbose_init=self.get_embeddings_verbose,
                    batch_size=new_batch_size,
                ).reset_index()

        if self.verbose:
            log.info(f"{df_vect.shape} <- df_vect.shape")

        return df_vect

    def _save_embeddings(
            self,
            df_vect: pd.DataFrame,
            df_single_file_name: str = 'df',
            add_shape_to_name: bool = True,
    ):
        """save df embeddings"""
        if add_shape_to_name:
            r, c = df_vect.shape
            f_gcs_name = f"gcs://{self.output_bucket}/{self.gcs_output_path_this_run}/{df_single_file_name}-{r}_by_{c}.parquet"
        else:
            f_gcs_name = f"gcs://{self.output_bucket}/{self.gcs_output_path_this_run}/{df_single_file_name}.parquet"
        log.info(f"Saving df_embeddings to: {f_gcs_name}")
        df_vect.to_parquet(
            f_gcs_name
        )

    def _set_path_local_model(self):
        """Set where to save artifacts locally for this run"""
        try:
            get_original_cwd()
            hydra_initialized = True
        except ValueError:
            hydra_initialized = False

        if hydra_initialized:
            log.info(f"Using hydra's path")
            # log.info(f"  Current working directory : {os.getcwd()}")
            # log.info(f"  Orig working directory    : {get_original_cwd()}")
            self.path_local_model = Path(os.getcwd())
        else:
            # create local path to store artifacts before logging to mlflow
            self.path_local_model = Path(
                f"{self.local_model_path}/{datetime.utcnow().strftime('%Y-%m-%d_%H%M%S')}"
            )
            Path(self.path_local_model).mkdir(exist_ok=True, parents=True)
            log.info(f"  Local model saving directory: {self.path_local_model}")

        self._init_file_log()
        # self.path_local_model_figures = self.path_local_model / 'figures'
        # Path(self.path_local_model_figures).mkdir(exist_ok=True, parents=True)

    def _init_file_log(self) -> None:
        """Create a file & FileHandler to log data"""
        # TODO(djb): make sure to remove fileHandler after job completes run_aggregation()
        if self.f_log_file is None:
            logger = logging.getLogger()

            path_logs = Path(self.path_local_model) / 'logs'
            Path.mkdir(path_logs, parents=False, exist_ok=True)
            self.f_log_file = str(
                path_logs /
                f"{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_vectorize_text.log"
            )
            info(f"  Log file created at: {self.f_log_file}")

            self.fileHandler = logging.FileHandler(self.f_log_file)
            self.fileHandler.setLevel(logging.INFO)

            formatter = logging.Formatter(
                '%(asctime)s | %(levelname)s | "%(message)s"',
                '%Y-%m-%d %H:%M:%S'
            )
            self.fileHandler.setFormatter(formatter)
            logger.addHandler(self.fileHandler)

    def _log_hydra_config_and_log_file(self):
        """Log hydra config to bucket. In the future this should be logged to mlflow"""
        # logs file (if it exists)
        if self.f_log_file is not None:
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(self.output_bucket)
            f_log_name = Path(self.f_log_file).name
            info(f"Saving log file...")
            (
                bucket
                .blob(posixpath.join(self.gcs_output_path_this_run, f_log_name))
                .upload_from_filename(self.f_log_file)
            )

        path_hydra_config = self.path_local_model / '.hydra'
        # For now, copy the logic from mlflow.log_artifacts()
        if path_hydra_config.is_dir():
            info(f"Saving hydra config...")
            # mlflow.log_artifacts(str(path_hydra_config), 'hydra')
            # Note: this function only expects the path, so we need to exclude
            #  `gcs://{bucket_name}/` from the run path
            upload_folder_to_gcs(
                bucket_name=self.output_bucket,
                gcs_output_root=self.gcs_output_path_this_run,
                local_dir=path_hydra_config,
                gcs_new_subfolder='hydra',
                verbose=False,
                dry_run=False
            )


def upload_folder_to_gcs(
        bucket_name: str,
        gcs_output_root: str,
        local_dir: Union[str, Path],
        gcs_new_subfolder: str = None,
        verbose: bool = False,
        dry_run: bool = False,
) -> None:
    """Wrapper around gcs to upload files in a folder recursively
    based on mlflow.log_artifacts()
    """
    if verbose:
        info(f"dry_run={dry_run}")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    if gcs_new_subfolder is None:
        dest_path = gcs_output_root
    else:
        dest_path = f"{gcs_output_root}/{gcs_new_subfolder}"

    local_dir = os.path.abspath(local_dir)
    for (root, _, filenames) in os.walk(local_dir):
        print(root)
        upload_path = dest_path

        if root != local_dir:
            rel_path = os.path.relpath(root, local_dir)
            # rel_path = relative_path_to_artifact_path(rel_path)
            upload_path = posixpath.join(dest_path, rel_path)

        for f in filenames:
            f_gcs_path = posixpath.join(upload_path, f)
            f_local = posixpath.join(root, f)
            if verbose:
                info(f"Uploading file\n  from: {f_local}\n  to: {bucket_name}/{f_gcs_path}")

            if not dry_run:
                bucket.blob(f_gcs_path).upload_from_filename(f_local)



def get_embeddings_as_df(
        model: callable,
        df: pd.DataFrame,
        col_text: str = 'text',
        cols_index: Union[str, List[str]] = None,
        col_embeddings_prefix: Optional[str] = 'embeddings',
        lowercase_text: bool = False,
        batch_size: Optional[int] = 1600,
        limit_first_n_chars: int = 2100,
        limit_first_n_chars_retry: int = 700,
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
    # Import errors here so that we can set the environment variable to suppress
    #  debugging logs before importing TF
    from tensorflow import errors

    if cols_index == 'comment_default_':
        cols_index = ['subreddit_name', 'subreddit_id', 'post_id', 'comment_id']
    elif cols_index == 'post_default_':
        cols_index = ['subreddit_name', 'subreddit_id', 'post_id']
    elif cols_index == 'subreddit_default_':
        cols_index = ['subreddit_name', 'subreddit_id']
    else:
        # Need to tweak this in case we get a weird iter input, like from OmegaConf.List
        cols_index = [str(_) for _ in cols_index]

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
            # Remember to reset the index before concat!
            #   Because pandas will do an inner join based on index
            df_vect = pd.concat(
                [df_vect, index_output.reset_index(drop=True)],
                axis=1,
            ).set_index(cols_index)
            # Main use of set_index() is to move the index cols to front of df
            #  so it's easier to inspect. We might need to reset_index() later
            #  because multi-index is not well supported in parquet by many tools

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
            iteration_chunks, mininterval=11, ascii=True,  ncols=80,
            desc='  Vectorizing: ',
            logger=log,  # position=0, leave=True,
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
