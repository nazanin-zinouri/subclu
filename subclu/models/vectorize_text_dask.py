"""
Utils to test out dask-delayed for doing inference/vectorization in parallel
"""
from datetime import datetime
from logging import info
from pathlib import Path
from typing import Union, List, Optional

import pandas as pd
import dask
import tensorflow_hub as hub

from .vectorize_text_tf import save_df_and_log_to_mlflow, get_embeddings_as_df


@dask.delayed
def save_embeddings_as_df_delayed(
        model_url: str,
        df_file: str,
        output_folder: str,
        col_text: str = 'text',
        cols_index: Union[str, List[str]] = None,
        col_embeddings_prefix: Optional[str] = 'embeddings',
        lowercase_text: bool = False,
        batch_size: int = None,
        limit_first_n_chars: int = 1000,
        limit_first_n_chars_retry: int = 600,
        verbose: bool = True,
        verbose_init: bool = False,
        log_to_mlflow: bool = False,
):
    """
    Wrapper with dask-delayed around get_embeddings_as_df

    Args:
        model_url:
        df_file:
        output_folder:
        col_text:
        cols_index:
        col_embeddings_prefix:
        lowercase_text:
        batch_size:
        limit_first_n_chars:
        limit_first_n_chars_retry:
        verbose:
        verbose_init:
        log_to_mlflow:

    Returns:

    """
    info(f"Loading model...")
    model = dask.delayed(hub.load)(model_url)
    df_input = dask.delayed(pd.read_parquet)(df_file)
    f_df_file_name = Path(df_file).name.replace('.parquet', '')

    info(f"Getting embeddings...")
    df_ = get_embeddings_as_df(
        model=model,
        df=df_input,
        col_text=col_text,
        cols_index=cols_index,
        col_embeddings_prefix=col_embeddings_prefix,
        lowercase_text=lowercase_text,
        batch_size=batch_size,
        limit_first_n_chars=limit_first_n_chars,
        limit_first_n_chars_retry=limit_first_n_chars_retry,
        verbose=verbose,
        verbose_init=verbose_init,
    )

    f_name = f"{f_df_file_name}_{datetime.utcnow().strftime('%Y-%m-%d_%H%M%S')}"
    save_df_and_log_to_mlflow(
        df=df_,
        local_path=output_folder,
        df_single_file_name=f_name,
        log_to_mlflow=log_to_mlflow,
    )
    r_, c_ = df_.shape
    return {'f_name': f_name, 'df_rows': r_, 'df_cols': c_}

