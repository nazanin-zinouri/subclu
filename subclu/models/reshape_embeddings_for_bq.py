"""
Convert the wide embedding format (512 columns, 1 per dimension) to a nested format.
Nested format = 1 column with a list of embeddings.

The nested format is preferable to serialize data for bigQuery.
"""
from datetime import datetime
import logging
from logging import info
from pathlib import Path
from typing import List, Union, Tuple, Dict

import pandas as pd
import mlflow
# import numpy as np

from ..utils.big_query_utils import load_data_to_bq_table
from .bq_embedding_schemas import embeddings_schema


def reshape_embeddings_to_ndjson(
        df_embeddings: pd.DataFrame,
        embedding_cols: List[str],
        columns_to_add: dict = None,
        f_name_prefix: str = 'sub_embeddings',
        save_path_local: Union[Path, str] = None,
        mlflow_run_id: str = None,
        log_to_mlflow: bool = True,
        sort_df: bool = False,
) -> Dict[str, str]:
    """
    Take a dataframe with embeddings and return a string that new-line delimited JSON record.
    If given path to save, it will save the file to that path.
    If log_to_mlflow=True
        log artifact to mlflow for the active run (active run needs to be created before hand)

    We return the GCS path for the mlflow artifact so that we can use it downstream
    to upload that file to create a BigQuery table from it.
    """
    local_f_name = f"{f_name_prefix}_{datetime.utcnow().strftime('%Y-%m-%d_%H%M%S')}.json"
    d_paths = {
        'f_local': None,
        'mlflow_path': None,
    }
    info(f"{df_embeddings.shape} <- Shape of input df")

    df_new = df_embeddings.drop(embedding_cols, axis=1).copy()

    if columns_to_add is not None:
        info(f"Metadata cols to add:\n  {columns_to_add}")
        for k_, v_ in columns_to_add.items():
            df_new[k_] = str(v_).strip()

    l_cols_to_front = [
        'pt',
        'mlflow_run_id',
        'model_name',
        'model_version',
        'subreddit_id',
        'subreddit_name',
        'posts_for_embeddings_count',
    ]
    # sort columns in expected order (partition & meta cols to front)
    l_new_col_order = (
        [c for c in l_cols_to_front if c in df_new.columns] +
        [c for c in df_new.columns if c not in l_cols_to_front]
    )
    df_new = df_new[l_new_col_order]
    info(f"Converting embeddings to repeated format...")
    df_new['embeddings'] = df_embeddings[embedding_cols].values.tolist()

    info(f"{df_new.shape} <- Shape of new df before converting to JSON")
    info(f"df output cols:\n  {list(df_new.columns)}")

    if sort_df:
        # Sort by most posts so that it's easier to see top subreddits in preview
        # NOTE: sorting doesn't help for large files because BQ loads lines in parallel
        try:
            df_new = (
                df_new
                .sort_values(by=['posts_for_embeddings_count', 'subreddit_name'], ascending=[False, True])
            )
        except Exception as e:
            logging.warning(f"Error sorting df:\n{e}")
    info(f"Converting embeddings to JSON...")
    str_json = df_new.to_json(orient='records', lines=True)

    if save_path_local is not None:
        save_path_local = Path(save_path_local)
        Path.mkdir(save_path_local, exist_ok=True, parents=True)
        f_local_full = save_path_local / local_f_name
        d_paths['f_local'] = f_local_full
        info(f"Saving file to:\n  {f_local_full}")
        with open(f_local_full, 'w') as f:
            f.write(str_json)

    if log_to_mlflow & (mlflow_run_id is not None):
        subfolder = save_path_local.name
        info(f"Logging to run ID: {mlflow_run_id}, artifact:\n  {subfolder}")
        with mlflow.start_run(run_id=mlflow_run_id) as run:
            mlflow.log_artifacts(str(save_path_local), subfolder)
            # get path to JSON file so that we can create a table from it
            d_paths['mlflow_artifact_path'] = mlflow.get_artifact_uri(
                artifact_path=f"{subfolder}/{local_f_name}"
            )
        info(f"Logging artifact complete!")
    return d_paths


def reshape_embeddings_and_upload_to_bq(
        df_embeddings: pd.DataFrame,
        dict_reshape_config: dict,
        save_path_local_root: Union[Path, str],
        f_name_prefix: str = 'sub_embeddings',
        embedding_col_prefix: str = 'embeddings_',
        mlflow_run_id_override: str = None,
        **kwargs
) -> None:
    """
    Take config from reshape_embeddings_for_bq to reshape & upload data
    to BigQuery table with a single call.
    """
    if mlflow_run_id_override is not None:
        mlflow_run_id = mlflow_run_id_override
    else:
        mlflow_run_id = dict_reshape_config['mlflow_run_id']

    # cols from config to add to output:
    bq_col_keys = [
        'pt',
        'mlflow_run_id',
        'model_name',
        'model_version',
    ]
    d_cols_to_add = {k: v for k, v in dict_reshape_config.items() if k in bq_col_keys}

    # subfolder to save reshaped embeddings locally
    path_local_json = (
            Path(save_path_local_root) / f"{dict_reshape_config['embeddings_artifact_path']}_ndjson"
    )
    l_embedding_cols = [c for c in df_embeddings.columns if c.startswith(embedding_col_prefix)]
    info(f"{len(l_embedding_cols):,.0f} <- # embedding columns found")

    d_paths = reshape_embeddings_to_ndjson(
        df_embeddings,
        embedding_cols=l_embedding_cols,
        columns_to_add=d_cols_to_add,
        f_name_prefix=f_name_prefix,
        save_path_local=path_local_json,
        log_to_mlflow=True,
        mlflow_run_id=mlflow_run_id,
    )

    info(f"Creating table from file:\n{d_paths['mlflow_artifact_path']}")
    load_data_to_bq_table(
        uri=d_paths['mlflow_artifact_path'],
        bq_project=dict_reshape_config['bq_project'],
        bq_dataset=dict_reshape_config['bq_dataset'],
        bq_table_name=dict_reshape_config['bq_table'],
        schema=embeddings_schema(),
        partition_column='pt',
        table_description=dict_reshape_config['bq_table_description'],
        update_table_description=True,
    )


#
# ~ fin
#
