"""
Call these function(s) to download fastText embeddings
This is a wrapper around fastText's utilities that makes sure to save embeddings
to a default location.
"""

from pathlib import Path
from copy import deepcopy
import fasttext.util

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


#
# ~ fin
#
