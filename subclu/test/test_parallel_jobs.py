"""
Job to test parallel processing
Reference:
https://hydra.cc/docs/plugins/joblib_launcher/

"""
from datetime import datetime
import logging
import os
import time

import hydra
from omegaconf import DictConfig

log = logging.getLogger(__name__)


@hydra.main(config_path='.', config_name="test_parallel_jobs")
def test_app(cfg: DictConfig) -> None:
    """Call from CLI
    single job:
    python test_parallel_jobs.py

    multi-job:
    python test_parallel_jobs.py --multirun task=1,2,3,4,5
    python test_parallel_jobs.py --multirun "task=range(0, 30, 2)"
    """
    log.info(f"{datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')} | START task {cfg.task} - Process ID {os.getpid()}")

    time.sleep(1.5)
    log.info(f"{datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')} | END task {cfg.task}")


"""
CONFIG:
===
defaults:
  # use joblib to run jobs in parallel
  - override hydra/launcher: joblib

task: 1  # single task
# multi-run doesn't work here... it only works on CLI
# task: "range(0, 30, 2)"

hydra:
  launcher:
    # override the number of jobs for joblib
    n_jobs: 10


Input:
---
Expect 15 jobs, 10 run in parallel. 5 run after.
python test_parallel_jobs.py --multirun "task=range(0, 30, 2)"

[2021-10-14 01:52:44,770][HYDRA] Joblib.Parallel(n_jobs=10,backend=loky,prefer=processes,require=None,verbose=0,timeout=None,pre_dispatch=2*n_jobs,batch_size=auto,temp_folder=None,max_nbytes=None,mmap_mode=r) is launching 15 jobs
[2021-10-14 01:52:44,770][HYDRA] Launching jobs, sweep output dir : multirun/2021-10-14/01-52-43
[2021-10-14 01:52:44,770][HYDRA] 	#0 : task=0
[2021-10-14 01:52:44,770][HYDRA] 	#1 : task=2
[2021-10-14 01:52:44,770][HYDRA] 	#2 : task=4
[2021-10-14 01:52:44,770][HYDRA] 	#3 : task=6
[2021-10-14 01:52:44,770][HYDRA] 	#4 : task=8
[2021-10-14 01:52:44,770][HYDRA] 	#5 : task=10
[2021-10-14 01:52:44,770][HYDRA] 	#6 : task=12
[2021-10-14 01:52:44,770][HYDRA] 	#7 : task=14
[2021-10-14 01:52:44,771][HYDRA] 	#8 : task=16
[2021-10-14 01:52:44,771][HYDRA] 	#9 : task=18
[2021-10-14 01:52:44,771][HYDRA] 	#10 : task=20
[2021-10-14 01:52:44,771][HYDRA] 	#11 : task=22
[2021-10-14 01:52:44,771][HYDRA] 	#12 : task=24
[2021-10-14 01:52:44,771][HYDRA] 	#13 : task=26
[2021-10-14 01:52:44,771][HYDRA] 	#14 : task=28
[2021-10-14 01:52:45,586][__main__][INFO] - 2021-10-14_06:52:45 | START task 2 - Process ID 23165
[2021-10-14 01:52:45,589][__main__][INFO] - 2021-10-14_06:52:45 | START task 0 - Process ID 23164
[2021-10-14 01:52:45,592][__main__][INFO] - 2021-10-14_06:52:45 | START task 4 - Process ID 23166
[2021-10-14 01:52:45,623][__main__][INFO] - 2021-10-14_06:52:45 | START task 6 - Process ID 23167
[2021-10-14 01:52:45,687][__main__][INFO] - 2021-10-14_06:52:45 | START task 8 - Process ID 23169
[2021-10-14 01:52:45,727][__main__][INFO] - 2021-10-14_06:52:45 | START task 10 - Process ID 23171
[2021-10-14 01:52:45,741][__main__][INFO] - 2021-10-14_06:52:45 | START task 14 - Process ID 23168
[2021-10-14 01:52:45,749][__main__][INFO] - 2021-10-14_06:52:45 | START task 12 - Process ID 23170
[2021-10-14 01:52:45,796][__main__][INFO] - 2021-10-14_06:52:45 | START task 16 - Process ID 23173
[2021-10-14 01:52:45,833][__main__][INFO] - 2021-10-14_06:52:45 | START task 18 - Process ID 23172
[2021-10-14 01:52:47,091][__main__][INFO] - 2021-10-14_06:52:47 | END task 2
[2021-10-14 01:52:47,091][__main__][INFO] - 2021-10-14_06:52:47 | END task 0
[2021-10-14 01:52:47,096][__main__][INFO] - 2021-10-14_06:52:47 | END task 4
[2021-10-14 01:52:47,128][__main__][INFO] - 2021-10-14_06:52:47 | END task 6
[2021-10-14 01:52:47,190][__main__][INFO] - 2021-10-14_06:52:47 | END task 8
[2021-10-14 01:52:47,231][__main__][INFO] - 2021-10-14_06:52:47 | END task 10
[2021-10-14 01:52:47,246][__main__][INFO] - 2021-10-14_06:52:47 | END task 14
[2021-10-14 01:52:47,253][__main__][INFO] - 2021-10-14_06:52:47 | END task 12
[2021-10-14 01:52:47,301][__main__][INFO] - 2021-10-14_06:52:47 | END task 16
[2021-10-14 01:52:47,306][__main__][INFO] - 2021-10-14_06:52:47 | START task 20 - Process ID 23165
[2021-10-14 01:52:47,309][__main__][INFO] - 2021-10-14_06:52:47 | START task 22 - Process ID 23164
[2021-10-14 01:52:47,334][__main__][INFO] - 2021-10-14_06:52:47 | END task 18
[2021-10-14 01:52:47,368][__main__][INFO] - 2021-10-14_06:52:47 | START task 24 - Process ID 23166
[2021-10-14 01:52:47,403][__main__][INFO] - 2021-10-14_06:52:47 | START task 26 - Process ID 23167
[2021-10-14 01:52:47,432][__main__][INFO] - 2021-10-14_06:52:47 | START task 28 - Process ID 23169
[2021-10-14 01:52:48,807][__main__][INFO] - 2021-10-14_06:52:48 | END task 20
[2021-10-14 01:52:48,813][__main__][INFO] - 2021-10-14_06:52:48 | END task 22
[2021-10-14 01:52:48,869][__main__][INFO] - 2021-10-14_06:52:48 | END task 24
[2021-10-14 01:52:48,909][__main__][INFO] - 2021-10-14_06:52:48 | END task 26
[2021-10-14 01:52:48,934][__main__][INFO] - 2021-10-14_06:52:48 | END task 28
"""

if __name__ == "__main__":
    test_app()
