# VMs, Parameters & configs

Different parts of the process require different VM configurations.

### Vectorizing text (mUSE)

| Step | VM name | CPU count | Memory | GPU config | Machine type | Base Google VM | Primary libraries | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1. Vectorize text | `djb-subclu-inference-tf-2-3-20210630` | 16 or 32 vCPUs | 32 GB or 64 GB  | 1 x NVIDIA Tesla T4 (16 GB) | | |  `pandas` data loading. Tensorflow for multilingual-USE (mUSE) inference | Currently only runs one file sequentially. No parallel processing |
| 2. Aggregate embeddings & calculate subreddit distances | `djb-100-2021-04-28-djb-eda-german-subs` | 160+ vCPUs | 2000+ GB (2.0 TB)  | n/a | `m1-megamem-96` (1.4 TB RAM), `m1-ultramem-80` (1.88 TB RAM), `m1-megamem-160` (3.4 TB RAM) | | `pandas` based after `dask` approach failed silently. | RAM peaked at around 700GB for v0.4.0; So we may need to double it given that v0.4.1 has about twice as many subreddits, posts, & comments. |
| 3. Clustering Algos | `djb-100-2021-04-28-djb-eda-german-subs` | 96+ vCPUs | 630+ GB RAM | n/a | | RAM usage for v0.4.0 was around 45 GB  (with 4+ jobs in parallel). Most clustering algos are parallelizable, so more vCPUs = more iterations + faster completion rate. |
| 4. Analyze clusters & other EDA | `djb-100-2021-04-28-djb-eda-german-subs` | 32+ vCPUs | 120+ RAM (memory) | n/a | RAM depends on whether I'm loading the embeddings for ad-hoc analysis. |
|  |  | vCPUs | RAM (memory) |  |  |

