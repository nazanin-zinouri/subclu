"""
Generic utils
"""
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
