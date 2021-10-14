"""
Job to test parallel processing
Reference:

https://hydra.cc/docs/advanced/override_grammar/basic/

https://hydra.cc/docs/tutorials/basic/running_your_app/multi-run
    https://hydra.cc/docs/plugins/joblib_launcher/
    https://hydra.cc/docs/plugins/ax_sweeper/

https://hydra.cc/docs/tutorials/basic/running_your_app/logging/
https://hydra.cc/docs/tutorials/basic/running_your_app/working_directory/

"""
from datetime import datetime
import logging
import os
import time

import hydra
from omegaconf import DictConfig

# NOTE: when running from CLI, run script as:
#  python -m subclu.test.test_parallel_jobs
# Because otherwise you'll get relative import errors
from ..utils.tqdm_logger import LogTQDM


log = logging.getLogger(__name__)


@hydra.main(config_path='.', config_name="test_parallel_jobs")
def test_app(cfg: DictConfig) -> None:
    """Call from CLI
    single job:
    python test_parallel_jobs.py

    multi-job WITHOUT relative imports:
    python test_parallel_jobs.py --multirun task=1,2,3,4,5
    python test_parallel_jobs.py --multirun "task=range(0, 30, 2)"

    multi-job WITH relative imports:
      We need to add the full path (subclu.test.<module_name>) to prevent relative import errors
    python -m subclu.test.test_parallel_jobs --multirun "task=range(0, 30, 2)"
    """
    log.info(f"{datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')} | START task {cfg.task} - Process ID {os.getpid()}")

    # ===============
    # CWD
    # ===
    """
    https://hydra.cc/docs/tutorials/basic/running_your_app/working_directory/
    When we use Hydra, it changes the CWD for each individual run. If we want to get the logs or outputs
     from that run, we can use: os.getcwd() to get the CWD.
    
    The logs created by hydra will look like:
    outputs/2019-09-25/15-16-17
    ├── .hydra
    │   ├── config.yaml
    │   ├── hydra.yaml
    │   └── overrides.yaml
    └── my_app.log

    Where `my_app.log` => the name of the module. So for this module it'll be called:
    `test_parallel_jobs.log`
    To make it generic we might need to glob (*.log) files
    """

    log.info(f"Current working directory : {os.getcwd()}")

    # We have to use a custom class wrapper around tqdm to get the logs written to file
    for i in LogTQDM(range(11), mininterval=.8, ncols=70, position=0, leave=True, ascii=True):
        time.sleep(.12)

    log.info(f"{datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')} | END task {cfg.task}")


if __name__ == "__main__":
    test_app()


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
python -m subclu.test.test_parallel_jobs --multirun "task=range(0, 30, 2)"

[2021-10-14 11:43:12,930][HYDRA] Joblib.Parallel(n_jobs=10,backend=loky,prefer=processes,require=None,verbose=0,timeout=None,pre_dispatch=2*n_jobs,batch_size=auto,temp_folder=None,max_nbytes=None,mmap_mode=r) is launching 15 jobs
[2021-10-14 11:43:12,930][HYDRA] Launching jobs, sweep output dir : multirun/2021-10-14/11-43-11
[2021-10-14 11:43:12,930][HYDRA] 	#0 : task=0
[2021-10-14 11:43:12,930][HYDRA] 	#1 : task=2
[2021-10-14 11:43:12,930][HYDRA] 	#2 : task=4
[2021-10-14 11:43:12,930][HYDRA] 	#3 : task=6
[2021-10-14 11:43:12,930][HYDRA] 	#4 : task=8
[2021-10-14 11:43:12,930][HYDRA] 	#5 : task=10
[2021-10-14 11:43:12,930][HYDRA] 	#6 : task=12
[2021-10-14 11:43:12,930][HYDRA] 	#7 : task=14
[2021-10-14 11:43:12,930][HYDRA] 	#8 : task=16
[2021-10-14 11:43:12,930][HYDRA] 	#9 : task=18
[2021-10-14 11:43:12,930][HYDRA] 	#10 : task=20
[2021-10-14 11:43:12,930][HYDRA] 	#11 : task=22
[2021-10-14 11:43:12,930][HYDRA] 	#12 : task=24
[2021-10-14 11:43:12,930][HYDRA] 	#13 : task=26
[2021-10-14 11:43:12,930][HYDRA] 	#14 : task=28
[2021-10-14 11:43:13,699][__main__][INFO] - 2021-10-14_16:43:13 | START task 0 - Process ID 28580
[2021-10-14 11:43:13,699][__main__][INFO] - Current working directory : /Users/david.bermejo/repos/subreddit_clustering_i18n/subclu/test/multirun/2021-10-14/11-43-11/0
[2021-10-14 11:43:13,708][__main__][INFO] - 2021-10-14_16:43:13 | START task 2 - Process ID 28582
[2021-10-14 11:43:13,708][__main__][INFO] - Current working directory : /Users/david.bermejo/repos/subreddit_clustering_i18n/subclu/test/multirun/2021-10-14/11-43-11/1
[2021-10-14 11:43:13,714][__main__][INFO] - 2021-10-14_16:43:13 | START task 4 - Process ID 28585
[2021-10-14 11:43:13,714][__main__][INFO] - Current working directory : /Users/david.bermejo/repos/subreddit_clustering_i18n/subclu/test/multirun/2021-10-14/11-43-11/2
[2021-10-14 11:43:13,748][__main__][INFO] - 2021-10-14_16:43:13 | START task 6 - Process ID 28588
[2021-10-14 11:43:13,748][__main__][INFO] - Current working directory : /Users/david.bermejo/repos/subreddit_clustering_i18n/subclu/test/multirun/2021-10-14/11-43-11/3
[2021-10-14 11:43:13,779][__main__][INFO] - 2021-10-14_16:43:13 | START task 8 - Process ID 28586
[2021-10-14 11:43:13,780][__main__][INFO] - Current working directory : /Users/david.bermejo/repos/subreddit_clustering_i18n/subclu/test/multirun/2021-10-14/11-43-11/4
[2021-10-14 11:43:13,827][__main__][INFO] - 2021-10-14_16:43:13 | START task 10 - Process ID 28583
[2021-10-14 11:43:13,827][__main__][INFO] - Current working directory : /Users/david.bermejo/repos/subreddit_clustering_i18n/subclu/test/multirun/2021-10-14/11-43-11/5
[2021-10-14 11:43:13,877][__main__][INFO] - 2021-10-14_16:43:13 | START task 12 - Process ID 28587
[2021-10-14 11:43:13,877][__main__][INFO] - Current working directory : /Users/david.bermejo/repos/subreddit_clustering_i18n/subclu/test/multirun/2021-10-14/11-43-11/6
[2021-10-14 11:43:13,915][__main__][INFO] - 2021-10-14_16:43:13 | START task 14 - Process ID 28589
[2021-10-14 11:43:13,915][__main__][INFO] - Current working directory : /Users/david.bermejo/repos/subreddit_clustering_i18n/subclu/test/multirun/2021-10-14/11-43-11/7
[2021-10-14 11:43:13,937][__main__][INFO] - 2021-10-14_16:43:13 | START task 16 - Process ID 28590
[2021-10-14 11:43:13,938][__main__][INFO] - Current working directory : /Users/david.bermejo/repos/subreddit_clustering_i18n/subclu/test/multirun/2021-10-14/11-43-11/8
[2021-10-14 11:43:13,985][__main__][INFO] - 2021-10-14_16:43:13 | START task 18 - Process ID 28591
[2021-10-14 11:43:13,985][__main__][INFO] - Current working directory : /Users/david.bermejo/repos/subreddit_clustering_i18n/subclu/test/multirun/2021-10-14/11-43-11/9
[2021-10-14 11:43:14,593][subclu.utils.tqdm_logger][INFO] - progress:  55%|#############           | 6/11 [00:00<00:00,  7.44it/s]
[2021-10-14 11:43:14,637][subclu.utils.tqdm_logger][INFO] - progress:  64%|###############2        | 7/11 [00:00<00:00,  7.54it/s]
[2021-10-14 11:43:14,637][subclu.utils.tqdm_logger][INFO] - progress:  64%|###############2        | 7/11 [00:00<00:00,  7.61it/s]
[2021-10-14 11:43:14,653][subclu.utils.tqdm_logger][INFO] - progress:  64%|###############2        | 7/11 [00:00<00:00,  7.53it/s]
[2021-10-14 11:43:14,687][subclu.utils.tqdm_logger][INFO] - progress:  55%|#############           | 6/11 [00:00<00:00,  7.50it/s]
[2021-10-14 11:43:14,687][subclu.utils.tqdm_logger][INFO] - progress:  64%|###############2        | 7/11 [00:00<00:00,  7.53it/s]
[2021-10-14 11:43:14,762][subclu.utils.tqdm_logger][INFO] - progress:  64%|###############2        | 7/11 [00:00<00:00,  7.56it/s]
[2021-10-14 11:43:14,855][subclu.utils.tqdm_logger][INFO] - progress:  64%|###############2        | 7/11 [00:00<00:00,  7.54it/s]
[2021-10-14 11:43:14,880][subclu.utils.tqdm_logger][INFO] - progress:  64%|###############2        | 7/11 [00:00<00:00,  7.52it/s]
[2021-10-14 11:43:14,921][subclu.utils.tqdm_logger][INFO] - progress:  64%|###############2        | 7/11 [00:00<00:00,  7.54it/s]
[2021-10-14 11:43:15,171][subclu.utils.tqdm_logger][INFO] - progress: 100%|#######################| 11/11 [00:01<00:00,  7.56it/s]
[2021-10-14 11:43:15,171][subclu.utils.tqdm_logger][INFO] - progress: 100%|#######################| 11/11 [00:01<00:00,  7.52it/s]


[2021-10-14 11:43:15,172][__main__][INFO] - 2021-10-14_16:43:15 | END task 0
[2021-10-14 11:43:15,172][__main__][INFO] - 2021-10-14_16:43:15 | END task 2
[2021-10-14 11:43:15,189][subclu.utils.tqdm_logger][INFO] - progress: 100%|#######################| 11/11 [00:01<00:00,  7.50it/s]

[2021-10-14 11:43:15,190][__main__][INFO] - 2021-10-14_16:43:15 | END task 4
[2021-10-14 11:43:15,220][subclu.utils.tqdm_logger][INFO] - progress: 100%|#######################| 11/11 [00:01<00:00,  7.52it/s]

[2021-10-14 11:43:15,221][__main__][INFO] - 2021-10-14_16:43:15 | END task 6
[2021-10-14 11:43:15,257][subclu.utils.tqdm_logger][INFO] - progress: 100%|#######################| 11/11 [00:01<00:00,  7.48it/s]

[2021-10-14 11:43:15,258][__main__][INFO] - 2021-10-14_16:43:15 | END task 8
[2021-10-14 11:43:15,296][subclu.utils.tqdm_logger][INFO] - progress: 100%|#######################| 11/11 [00:01<00:00,  7.53it/s]

[2021-10-14 11:43:15,296][__main__][INFO] - 2021-10-14_16:43:15 | END task 10
[2021-10-14 11:43:15,355][subclu.utils.tqdm_logger][INFO] - progress: 100%|#######################| 11/11 [00:01<00:00,  7.49it/s]

[2021-10-14 11:43:15,356][__main__][INFO] - 2021-10-14_16:43:15 | END task 12
[2021-10-14 11:43:15,385][__main__][INFO] - 2021-10-14_16:43:15 | START task 20 - Process ID 28582
[2021-10-14 11:43:15,385][__main__][INFO] - Current working directory : /Users/david.bermejo/repos/subreddit_clustering_i18n/subclu/test/multirun/2021-10-14/11-43-11/10
[2021-10-14 11:43:15,387][__main__][INFO] - 2021-10-14_16:43:15 | START task 22 - Process ID 28580
[2021-10-14 11:43:15,387][__main__][INFO] - Current working directory : /Users/david.bermejo/repos/subreddit_clustering_i18n/subclu/test/multirun/2021-10-14/11-43-11/11
[2021-10-14 11:43:15,389][subclu.utils.tqdm_logger][INFO] - progress: 100%|#######################| 11/11 [00:01<00:00,  7.52it/s]

[2021-10-14 11:43:15,390][__main__][INFO] - 2021-10-14_16:43:15 | END task 14
[2021-10-14 11:43:15,413][subclu.utils.tqdm_logger][INFO] - progress: 100%|#######################| 11/11 [00:01<00:00,  7.51it/s]

[2021-10-14 11:43:15,413][__main__][INFO] - 2021-10-14_16:43:15 | END task 16
[2021-10-14 11:43:15,432][__main__][INFO] - 2021-10-14_16:43:15 | START task 24 - Process ID 28585
[2021-10-14 11:43:15,433][__main__][INFO] - Current working directory : /Users/david.bermejo/repos/subreddit_clustering_i18n/subclu/test/multirun/2021-10-14/11-43-11/12
[2021-10-14 11:43:15,457][subclu.utils.tqdm_logger][INFO] - progress: 100%|#######################| 11/11 [00:01<00:00,  7.51it/s]

[2021-10-14 11:43:15,457][__main__][INFO] - 2021-10-14_16:43:15 | END task 18
[2021-10-14 11:43:15,470][__main__][INFO] - 2021-10-14_16:43:15 | START task 26 - Process ID 28588
[2021-10-14 11:43:15,470][__main__][INFO] - Current working directory : /Users/david.bermejo/repos/subreddit_clustering_i18n/subclu/test/multirun/2021-10-14/11-43-11/13
[2021-10-14 11:43:15,497][__main__][INFO] - 2021-10-14_16:43:15 | START task 28 - Process ID 28586
[2021-10-14 11:43:15,497][__main__][INFO] - Current working directory : /Users/david.bermejo/repos/subreddit_clustering_i18n/subclu/test/multirun/2021-10-14/11-43-11/14
[2021-10-14 11:43:16,233][subclu.utils.tqdm_logger][INFO] - progress:  55%|#############           | 6/11 [00:00<00:00,  7.50it/s]
[2021-10-14 11:43:16,317][subclu.utils.tqdm_logger][INFO] - progress:  64%|###############2        | 7/11 [00:00<00:00,  7.52it/s]
[2021-10-14 11:43:16,317][subclu.utils.tqdm_logger][INFO] - progress:  64%|###############2        | 7/11 [00:00<00:00,  7.53it/s]
[2021-10-14 11:43:16,403][subclu.utils.tqdm_logger][INFO] - progress:  64%|###############2        | 7/11 [00:00<00:00,  7.51it/s]
[2021-10-14 11:43:16,430][subclu.utils.tqdm_logger][INFO] - progress:  64%|###############2        | 7/11 [00:00<00:00,  7.50it/s]
[2021-10-14 11:43:16,843][subclu.utils.tqdm_logger][INFO] - progress: 100%|#######################| 11/11 [00:01<00:00,  7.56it/s]
[2021-10-14 11:43:16,843][subclu.utils.tqdm_logger][INFO] - progress: 100%|#######################| 11/11 [00:01<00:00,  7.55it/s]


[2021-10-14 11:43:16,843][__main__][INFO] - 2021-10-14_16:43:16 | END task 22
[2021-10-14 11:43:16,843][__main__][INFO] - 2021-10-14_16:43:16 | END task 20
[2021-10-14 11:43:16,893][subclu.utils.tqdm_logger][INFO] - progress: 100%|#######################| 11/11 [00:01<00:00,  7.53it/s]

[2021-10-14 11:43:16,893][__main__][INFO] - 2021-10-14_16:43:16 | END task 24
[2021-10-14 11:43:16,934][subclu.utils.tqdm_logger][INFO] - progress: 100%|#######################| 11/11 [00:01<00:00,  7.52it/s]

[2021-10-14 11:43:16,935][__main__][INFO] - 2021-10-14_16:43:16 | END task 26
[2021-10-14 11:43:16,962][subclu.utils.tqdm_logger][INFO] - progress: 100%|#######################| 11/11 [00:01<00:00,  7.51it/s]

[2021-10-14 11:43:16,962][__main__][INFO] - 2021-10-14_16:43:16 | END task 28
"""
