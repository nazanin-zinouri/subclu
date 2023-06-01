"""
Utils to run inference for PN model
"""
from datetime import datetime
import json
from logging import info
from pathlib import Path
from typing import List, Union

import numpy as np
import pandas as pd
import joblib
# import polars as pl
from tqdm import tqdm


class NumpyEncoder(json.JSONEncoder):
    """
    Custom encoder for numpy data types.
    We need it because when we output data from pandas, we get np types that JSON complains about
    """
    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):
            return int(obj)

        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)

        elif isinstance(obj, (np.complex_, np.complex64, np.complex128)):
            return {'real': obj.real, 'imag': obj.imag}

        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()

        elif isinstance(obj, np.bool_):
            return bool(obj)

        elif isinstance(obj, np.void):
            return None

        return json.JSONEncoder.default(self, obj)


def run_inference_on_one_file(
        df_path: Union[str, Path],
        model_path: Union[str, Path],
        path_output: Union[str, Path],
        l_ix_columns: List[str],
        l_feature_columns: List[str] = None,
        c_pred_proba: str = 'click_proba',
        out_prefix: str = 'pred-',
        verbose: bool = False,
) -> None:
    """Given an input file & input model, run inference & save to path_output location.
    The output file will preserve the same name as the input file name with a `pred-` prefix

    l_feature_columns. If None, pull the features from the model itself
    """
    if verbose:
        info(f"Loading model...\n  {model_path}")
    model = joblib.load(
        f"{model_path}"
    )
    if l_feature_columns is None:
        # Get input feature names for modeling
        l_feature_columns = [
            '__'.join(_.split('__')[1:]) for _ in model.best_estimator_['preprocess'].get_feature_names_out()
        ]

    if verbose:
        info(f"{len(l_feature_columns)} <- Model feature count")
        info(
            f"\nFeatures:"
            f"\n  {l_feature_columns}"
        )

    if verbose:
        info(f"Loading data:\n  {df_path}")
    df_inference_raw = pd.read_parquet(
        df_path,
        columns=list(set(l_ix_columns + l_feature_columns)),
    )
    if verbose:
        info(f"Create new df for predictions...")
    df_pred = (
        df_inference_raw[l_ix_columns]
        .assign(
            **{c_pred_proba: model.predict_proba(df_inference_raw[l_feature_columns])[:, 1]}
        )
    )
    Path.mkdir(path_output, exist_ok=True, parents=True)
    df_path_out = f"{path_output}/{out_prefix}{Path(df_path).name}"

    if verbose:
        info(f"Done with predictions")
        info(f"Saving prediction to: \n{df_path_out}")
    df_pred.to_parquet(
        df_path_out,
        index=False
    )

    if verbose:
        info(f"Prediction done!")


def apply_filters_and_save_to_json(
        df: pd.DataFrame,
        path_local_save: Union[str, Path],
        d_model_meta: dict,
        ndjson_subfolder: str = 'click_proba_ndjson',
        n_max_user_rank: int = 500000,
        col_user_rank: str = 'user_rank_by_sub_and_geo',
        click_prob_threshold_min: float = 0.100,
        col_click_prob: str = 'click_proba',
        cols_for_nested_users: List[str] = None,
        l_ix_cache: List[str] = None,
        batch_num: int = 0,
        file_timestamp: str = None,
        d_rename_ix_cols: dict = None,
        verbose: bool = False,
) -> dict:
    """Returns a dict with metadata about the data saved"""
    d_meta = dict()

    if cols_for_nested_users is None:
        cols_for_nested_users = [
            'user_id',
            col_click_prob,
            col_user_rank,
        ]
    if l_ix_cache is None:
        l_ix_cache = [
            'pt',
            'target_subreddit_id',
            'target_subreddit',
            'subscribed',
            'user_geo_country_code_top'
        ]
    if d_rename_ix_cols is not None:
        l_ix_cache = [d_rename_ix_cols.get(_, _) for _ in l_ix_cache]
    else:
        d_rename_ix_cols = dict()

    if file_timestamp is None:
        file_timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H%M%S')

    info(f"Creating mask for user to process...")
    # apply the mask before the groupby to speed up the whole process
    mask_cache_ = (
            (df[col_user_rank] <= n_max_user_rank) &
            (df[col_click_prob] >= click_prob_threshold_min)
    )

    d_meta['rows_processed'] = mask_cache_.sum()
    info(f"{d_meta['rows_processed']:,.0f} <- Rows to process")

    # Create local paths & file
    info(f"Creating paths for file...")
    p_local_json = path_local_save / f"{ndjson_subfolder}"
    Path.mkdir(p_local_json, exist_ok=True, parents=True)
    d_meta['local_json_folder'] = p_local_json

    f_local_json_name = f"click_proba_ndjson-{batch_num}-{file_timestamp}-{d_meta['rows_processed']}_rows.json"
    f_local_json_full = p_local_json / f_local_json_name
    info(f"Output file:\n{f_local_json_full}")
    d_meta['local_json_filename'] = f_local_json_name
    d_meta['local_json_file'] = f_local_json_full

    # If we run this fxn times, make sure we don't append duplicated lines
    try:
        f_local_json_full.unlink()
        info(f"  Deleted existing file")
    except FileNotFoundError as e:
        info(f"  No previous file found to delete\n  {e}")

    info(f"Start saving df as ndJSON...")
    with open(f_local_json_full, 'w') as f:
        for l_ix_vals_, df_seed_ in tqdm(
            (
                df[mask_cache_]
                .rename(columns=d_rename_ix_cols)
                .groupby(l_ix_cache)
            ),
            mininterval=2
        ):
            # NOTE: Assumes we already applied rank & threshold limits to df_seed_!
            d_seed = {
                **d_model_meta,
                **{k: v for k, v in zip(l_ix_cache, l_ix_vals_)},
                **{
                    # each USER should be its own dict
                    'top_users': (
                        df_seed_[cols_for_nested_users]
                        .to_dict(orient='records')
                    )
                }
            }
            if verbose:
                info(f"{df_seed_[cols_for_nested_users].shape} <- Output shape for {l_ix_vals_}")
            f.write(json.dumps(d_seed, cls=NumpyEncoder) + "\n")

    info(f"Done saving as ndJSON")
    print(f"Example subreddit:")
    for k, v in d_seed.items():
        if isinstance(v, list):
            print(f"{k}:")
            for _ in v[:5]:
                print(f"    {_}")
        else:
            print(f"{k}:  {v}")

    return d_meta


#
# ~ fin ~
#
