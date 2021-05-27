"""
Generic utils
"""
from copy import deepcopy
from contextlib import contextmanager
import os
from pathlib import Path
from typing import Union

from google.cloud import storage
from tqdm.auto import tqdm


@contextmanager
def set_working_directory(path: Path):
    """Sets the cwd within the context

    Args:
        path (Path): The path to the cwd

    Yields:
        None
    """
    origin = Path().absolute()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(origin)


def get_project_subfolder(
        subfolder_path: str,
        project_root: str = '/subreddit_clustering_i18n',
) -> Path:
    """
    Note: this assumes that the `project_root` is unique.
    If there's a nested folder with the same name, this function will return the deepest folder.

    If `subfolder` doesn't exist, this function will create it.

    Example subfolder_input:    'data/embeddings'
    Example output:             Path('../<project_root>/data/embeddings`)

    Args:
        subfolder_path: the location of the desired path, relative to `project_root`
        project_root: the name of the top-level project folder

    Returns:
        Path object with absolute location.
    """
    path_cwd_original = Path.cwd()

    # This approach only manually checks 2 levels up from cwd
    p_1_level = path_cwd_original.parents[0]
    p_2_level = path_cwd_original.parents[1]

    if str(path_cwd_original).endswith(project_root):
        path_project = deepcopy(path_cwd_original)
    elif str(p_1_level).endswith(project_root):
        path_project = deepcopy(p_1_level)
    elif str(p_2_level).endswith(project_root):
        path_project = deepcopy(p_2_level)
    else:
        raise FileNotFoundError(f"Couldn't find project {project_root}"
                                f" in path: {path_cwd_original}")

    path_abs_new = path_project / subfolder_path
    Path.mkdir(path_abs_new, exist_ok=True, parents=True)

    return path_abs_new


def upload_folder_to_gcp(
        local_source_path: Union[Path, str],
        destination_blob_root: str,
        bucket_name: str = 'i18n-subreddit-clustering',
) -> None:
    """"""
    # gcs doesn't seem to have a utility to sync a folder, so this function is a wrapper
    # around GCS's function to upload one file at a time
    # TODO(djb): maybe use gsutil instead?
    #  docs: https://cloud.google.com/storage/docs/gsutil/commands/cp
    #    gsutil cp -r dir gs://my-bucket
    #  or in jupyter notebook (using $<python_variables>)
    #    !gsutil -m cp -r $path_this_ft_model $bucket_upload_dir

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for f in tqdm([f for f in Path(local_source_path).glob('*') if f.is_file()]):
        f_dest = f"{destination_blob_root}/{f.name}"




#
# ~ fin
#
