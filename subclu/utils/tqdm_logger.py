"""
Utils to send tqdm info to log files - this class should log to both console AND files

Inspired by open issue in github:
https://github.com/tqdm/tqdm/issues/313
"""
from datetime import datetime
import logging
from pathlib import Path
from typing import Union

from tqdm import tqdm


LOGGER = logging.getLogger(__name__)


class LogTQDM(tqdm):
    def __init__(
            self,
            *args,
            logger: logging.Logger = None,
            mininterval: float = .8,
            ascii: bool = True,
            ncols: int = 65,
            position: int = 0,
            leave: bool = True,
            desc: str = 'progress: ',
            # bar_format: str = '{desc}{percentage:3.0f}%{r_bar}',  # this only shows text w/o a progress bar
            **kwargs):
        self._logger = logger
        super().__init__(
            *args,
            mininterval=mininterval,
            ascii=ascii,
            ncols=ncols,
            position=position,
            leave=leave,
            desc=desc,
            # bar_format=bar_format,
            **kwargs
        )

    @property
    def logger(self):
        if self._logger is not None:
            return self._logger
        return LOGGER

    def display(self, msg=None, pos=None):
        if not self.n:
            # skip progress bar before having processed anything
            return
        if not msg:
            msg = self.__str__()
        self.logger.info('%s', msg)


class FileLogger():
    def __init__(
            self,
            logs_path: Union[str, Path],
            log_name: str = 'log',
            log_level = logging.INFO
    ):
        """"""
        self.logs_path = Path(logs_path)
        self.log_name = log_name
        self.log_level = log_level

        self.f_log_file = str(
            self.logs_path /
            f"{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_{self.log_name}.log"
        )
        self.fileHandler = None

    def init_file_log(self) -> None:
        """Create a file & FileHandler to log data"""
        logger = logging.getLogger()
        Path.mkdir(self.logs_path, parents=True, exist_ok=True)

        self.fileHandler = logging.FileHandler(self.f_log_file)
        self.fileHandler.setLevel(self.log_level)

        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | "%(message)s"',
            '%Y-%m-%d %H:%M:%S'
        )
        self.fileHandler.setFormatter(formatter)
        logger.addHandler(self.fileHandler)

    def remove_file_logger(self) -> None:
        """After completing job, remove logging handler to prevent
        info from other jobs getting logged to the same log file
        """
        if self.fileHandler is not None:
            logger = logging.getLogger()
            try:
                logger.removeHandler(self.fileHandler)
            except Exception as e:
                logging.warning(f"Can't remove logger\n{e}")


#
# ~ fin
#
