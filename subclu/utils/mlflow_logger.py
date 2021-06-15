"""
Utils to set up base mlflow setup & config
Currently everything is local, but at some point we might switch to a server
"""
import os
import json
import logging

import pandas as pd
from pathlib import Path
import subprocess
from typing import List, Union

import mlflow
from mlflow.utils import mlflow_tags
from mlflow.exceptions import MlflowException


class MlflowLogger:
    """
    This class is a workaround for using mlflow WITHOUT a server.
    """
    def __init__(
            self,
            tracking_uri: str = 'sqlite',
            default_artifact_root: str = 'gs://i18n-subreddit-clustering/mlflow/mlruns',
    ):
        self.default_artifact_root = default_artifact_root

        if tracking_uri in [None, 'sqlite']:
            # TODO(djb): update path to config file?
            path_mlruns_db = Path("/home/jupyter/mlflow")
            Path.mkdir(path_mlruns_db, exist_ok=True, parents=True)
            mlflow.set_tracking_uri(f"sqlite:///{path_mlruns_db}/mlruns.db")
        else:
            mlflow.set_tracking_uri(tracking_uri)

        self.tracking_uri = tracking_uri

    def create_experiment(
            self,
            name: str,
            artifact_location: str = None,
    ) -> str:
        """Wrapper around mlflow.create_experiment()
        This one uses the `default_artifact_root` set at the class-init to set
        the experiment location by auto-incrementing based on latest ACTIVE experiment.

        There could be weird results if we delete an experiment with runs & artifacts and
         then we create a new experiment. It's possible that both experiments might share the
         same artifact location, but the UUIDs for all runs should still be unique.
        """
        if artifact_location is not None:
            artifact_location = artifact_location
        else:
            artifact_location = (
                f"{self.default_artifact_root}/"
                f"{1 + self.get_max_experiment_id()}"
            )

        return mlflow.create_experiment(
            name,
            artifact_location=artifact_location
        )

    def set_experiment(
            self,
            name: str,
            artifact_location: str = None,
    ) -> None:
        """Wrapper around mlflow.create_experiment()/set_experiment
        This one uses the `default_artifact_root` set at the class-init to set
        the experiment location by auto-incrementing based on latest ACTIVE experiment.

        There could be weird results if we delete an experiment with runs & artifacts and
         then we create a new experiment. It's possible that both experiments might share the
         same artifact location, but the UUIDs for all runs should still be unique.
        """
        try:
            self.create_experiment(name=name, artifact_location=artifact_location)
        except MlflowException:
            pass

        return mlflow.set_experiment(name)

    @staticmethod
    def add_git_hash_to_active_run() -> None:
        """
        Check whether mlflow has set a tag for git commit,
        if it doesn't, set it.

        Returns: None
        """
        active_run = mlflow.active_run()
        git_commit = active_run.data.tags.get(mlflow_tags.MLFLOW_GIT_COMMIT)
        if git_commit is None:
            mlflow.set_tag('mlflow.source.git.commit', get_git_hash())

    @staticmethod
    def reset_sqlalchemy_logging() -> None:
        """
        For some reason my function to set logging info in notebooks can reset
        sqlalchemy and other libraries to "INFO", which can add a lot of noise.

        Returns: None
        """
        logging.getLogger('sqlalchemy').setLevel(logging.WARN)
        logging.getLogger('alembic').setLevel(logging.WARN)

    def list_experiment_meta(self,
                             output_format=None
                             ) -> Union[List[dict], pd.DataFrame]:
        """Get experiment meta as list of dictionaries"""
        mlflow_client = mlflow.tracking.MlflowClient()
        # The first time we call this function, we may see a lot of 'info' logs
        self.reset_sqlalchemy_logging()
        l_exp = list()

        for exp_ in mlflow_client.list_experiments():
            l_exp.append(
                json.loads(
                    mlflow.utils.proto_json_utils.message_to_json(exp_.to_proto())
                )
            )
        if output_format == 'pandas':
            return pd.DataFrame(l_exp)
        else:
            return l_exp

    def get_max_experiment_id(self) -> int:
        """Get the largest experiment ID for ACTIVE experiments
        Use it to set the artifact location as 1+ max
        """
        return max([int(e['experiment_id']) for e in self.list_experiment_meta()])

    def search_all_runs(
            self,
            experiment_ids: Union[str, int, List[int]] = None,
    ) -> pd.DataFrame:
        """
        Get all runs in pandas format
        Returns:
        """
        if experiment_ids is None:
            experiment_ids = [d['experiment_id'] for d in self.list_experiment_meta()]
        return mlflow.search_runs(experiment_ids, output_format='pandas')

    def read_run_artifact(
            self,
            run_id: str,
            artifact_folder: str,
            experiment_ids: Union[str, int, List[int]] = None,
            read_function: callable = pd.read_parquet,
            columns: iter = None,
    ):
        """
        Example:
        df_v_sub = (
            pd.read_parquet(f"{artifact_uri}/{folder_vect_subs}")
        )
        """
        # first get a df for all runs
        # logging.info(f"  Getting all runs...")
        df_all_runs = self.search_all_runs(experiment_ids=experiment_ids)

        artifact_uri = df_all_runs.loc[
            df_all_runs['run_id'] == run_id,
            'artifact_uri'
        ].values[0]
        try:
            return read_function(f"{artifact_uri}/{artifact_folder}",
                                 columns=columns)
        except Exception as e:
            print(e)
            return read_function(f"{artifact_uri}/{artifact_folder}")


def get_git_hash() -> str:
    """
    Borrowed from s-overflow. Use it to get current git hash and add as a tag, IFF
    mlflow hasn't detected the current git tag.
    https://stackoverflow.com/questions/14989858/

    Returns: git-hash as a string
    """
    try:
        git_hash = (
        subprocess.check_output(['git', 'rev-parse', 'HEAD'])
        .strip()
        .decode('ascii')
    )
    except OSError:
        git_hash = None

    return git_hash


#
# ~ fin
#
