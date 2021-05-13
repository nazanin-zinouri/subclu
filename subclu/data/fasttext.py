"""
Call these function(s) to download fastText embeddings
This is a wrapper around fastText's utilities that makes sure to save embeddings
to a default location.
"""

from copy import deepcopy
from pathlib import Path
from typing import List

import fasttext.util
import pandas as pd

from subclu.utils import set_working_directory


def download_ft_pretrained_model(
        lang_id: str,
        if_exists: str = 'ignore',
) -> Path:
    """
    Save fastText model to expected embeddings folder.
    Wrapper around fasttext.util.download_model.

    Args:
        lang_id: 2- or 3-letter language code
            See lists here:
            https://fasttext.cc/docs/en/crawl-vectors.html
            https://github.com/facebookresearch/fastText/blob/
                a20c0d27cd0ee88a25ea0433b7f03038cd728459/python/fasttext_module/
                fasttext/util/util.py#L34

        if_exists: What do do if file exists?
            'ignore' -> keep existing, don't download again
            'overwrite' -> re-download even if it exists

    Returns:
        Path to model file (absolute)
    """
    # TODO(djb): move these vars into a config file (dotenv? configparser?)
    PROJECT_NAME_FOLDER = '/subreddit_clustering_i18n'
    EMBEDDINGS_SUBFOLDER = 'data/embeddings'
    FASTTEXT_SUBFOLDER = f"{EMBEDDINGS_SUBFOLDER}/fasttext"

    path_cwd_original = Path.cwd()

    # This approach only manually checks 2 levels up from cwd
    p_1_level = path_cwd_original.parents[0]
    p_2_level = path_cwd_original.parents[1]
    if str(path_cwd_original).endswith(PROJECT_NAME_FOLDER):
        path_project = deepcopy(path_cwd_original)
    elif str(p_1_level).endswith(PROJECT_NAME_FOLDER):
        path_project = deepcopy(p_1_level)
    elif str(p_2_level).endswith(PROJECT_NAME_FOLDER):
        path_project = deepcopy(p_2_level)
    else:
        raise FileNotFoundError(f"Couldn't find project {PROJECT_NAME_FOLDER}"
                                f" in path: {path_cwd_original}")

    path_ft_embeddings = path_project / FASTTEXT_SUBFOLDER
    Path.mkdir(path_ft_embeddings, exist_ok=True, parents=True)
    print(f"Saving embeddings to:\n  {path_ft_embeddings}")

    # use context manager to change working directory to ft-embeddings
    #  and change it back to original directory
    with set_working_directory(path_ft_embeddings):
        rel_file_name = fasttext.util.download_model(lang_id, if_exists=if_exists)

    return path_ft_embeddings / rel_file_name


def get_df_for_most_similar(
        ft_model,
        list_of_words: List[str],
        print_oov_check: bool = True,
) -> pd.DataFrame:
    """
    Take a list of words and return a df to more easily compare the most similar words side by side.

    By default, "most_similar" returns a list of tuples which is not great for visualization & comparing.
    """
    l_sim = list()

    for word_ in list_of_words:
        l_this_word_ = ft_model.most_similar(word_)
        l_sim.append({
            f"'{word_}' similar_words": [t[0] for t in l_this_word_],
            f"'{word_}' similarity_score": [t[1] for t in l_this_word_],
        })
        del l_this_word_, word_

    if print_oov_check:
        for word_ in list_of_words:
            print(f"{word_ in ft_model} -> {word_} in vocabulary?")

    df = pd.DataFrame(l_sim[0])

    if 1 == len(l_sim):
        return df
    else:
        for i, l_word_ in enumerate(l_sim[1:], start=1):
            df = (
                df
                .assign(**{' ' * i: ['|'] * len(df)})
                .merge(
                    pd.DataFrame(l_word_),
                    how='outer',
                    left_index=True,
                    right_index=True,
                )
            )
        return df


#
# ~ fin
#
