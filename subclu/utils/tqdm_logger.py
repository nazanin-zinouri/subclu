"""
Utils to send tqdm info to log files - this class should log to both console AND files

Inspired by open issue in github:
https://github.com/tqdm/tqdm/issues/313
"""
import logging

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
