"""
Take embeddings created in `vectorize_text.py` and combine/merge them into:
- post-level embeddings
- subreddit-level embeddings

Does NOT take into account reducing the embeddings (to tSNE or UMAP)
"""
from typing import Tuple
import mlflow

import pandas as pd


def combine_embeddings(

) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Take existing embeddings & merge them in to a single embedding given different weights
    Returns:

    """

    # for now, search in all experiments




