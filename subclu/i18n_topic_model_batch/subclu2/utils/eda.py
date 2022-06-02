"""
Misc utils to make life easier
"""
from datetime import datetime, timedelta
import logging


def elapsed_time(
        start_time,
        log_label: str = ' ',
        measure=None,
        verbose: bool = False,
) -> float:
    """
    Given a datetime object as a start time, calculate how many days/hours/minutes/seconds
    since the start time.
    """
    time_now = datetime.utcnow()
    time_elapsed = (time_now - start_time)
    if measure is None:
        pass  # keep as datetime.timedelta object
    elif measure == 'seconds':
        time_elapsed = time_elapsed / timedelta(seconds=1)
    elif measure == 'minutes':
        time_elapsed = time_elapsed / timedelta(minutes=1)
    elif measure == 'hours':
        time_elapsed = time_elapsed / timedelta(hours=1)
    elif measure == 'days':
        time_elapsed = time_elapsed / timedelta(days=1)
    else:
        raise NotImplementedError(f"Measure unknown: {measure}")

    if verbose:
        if measure is not None:
            logging.info(f"  {time_elapsed:,.3f} {measure} <- {log_label} time elapsed")
        else:
            logging.info(f"  {time_elapsed} <- {log_label} time elapsed")

    return time_elapsed
