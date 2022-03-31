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
            data_loader_kwargs: dict = None,
            run_id: str = None,
            **kwargs
    ) -> None:
        """"""
        DATA_LOADERS = {
            'LoadSubredditsSQL': LoadSubredditsSQL,
        }
        self.model_name = model_name
        self.col_text_for_embeddings = col_text_for_embeddings
        self.data_loader_name = data_loader_name

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

        # TODO(djb): load model
        log.info(f"Lodaing model: {self.model_name}")
        model = self._load_model()
        log.info(f"Model loaded")

        # get text
        df_text = self.data_loader.get_as_dataframe()

        print(df_text[self.col_text_for_embeddings].head())

        # TODO(djb): get embeddings

        # TODO(djb): save embeddings
        #  for now, save straight to GCS, in the future shift to mlflow
        #  so we'd have to save to local first

        return df_text[self.col_text_for_embeddings]

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


if __name__ == "__main__":
    vectorize_text()


#
# ~fin
#
