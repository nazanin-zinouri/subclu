subclu - SUBreddit CLUstering / Topic Modeling for i18n
==============================
[For more details, please see the 1-pager](https://docs.google.com/document/d/1MXE7SKnXJMVUE93IKuR2WvL8RgHpjmFISkMylXNdUjY/).

We want to create topic clusters/models to better understand the content of German-relevant subs so that we can use them to
- a) inform strategy for content creation (and maybe SEO as well?)
- b) improve discovery (use the post & subreddit topic scores as inputs to One Feed).

At the end of each iteration of this project we should have 
1. A set of topics (we'll need to create human-interpretable labels)
2. A topic assigned to each post/comment
3. A score for how common each topic is in each subreddit that is geo-relevant to Germany

For example, we should be able to say that in that past week in subreddit X:
- 80% of posts are about topic A
- 15% of posts are about topic B
- 5% are about other topics

The final score for a subreddit is TBD. Besides topic labels, we might end up aggregating all the posts (and comments) in a subreddit to get a single value (or a vector). Getting a single vector per subreddit might be the best path forward so that we can use distance metrics to compare "most similar" subreddits and use that for recommendations.

# Creating topic clusters process
The process is split into the steps below. Note that the actual code is in modules inside the `subclu` folder, but we're using notebooks to make it easier to log and document the process.
0. **Pull data** from BigQuery
   - See `subclu > data > v0.4.0_add_more_geo_and_active_subs`
      - `_01_select_subreddits.sql`
      - `_02_selects_posts_for_modeling.sql`
      - `_03_select_comments_for_modeling.sql`
1. **EDA** of training data
   - Notebooks that start with `djb_01`
   - Check data before modeling to spot anythig missing or odd about data distributions
2. **Vectorize** text (converting it into embeddings)
   - Notebooks that start with `djb_02` (name before v0.4.0: `djb_06.x`)
3. **Aggregate** the embeddings from posts and comments
   - Notebooks that start with `djb_03` (name before v0.4.0: `djb_10.x` or `djb_11.x`)
4. **Create** clusters
   - Notebooks that start with `djb_04` (name before v0.4.0: `djb_17.x` or `djb_18.x`)

Path to v0.4.0 notebooks (the latest version as of 2021-10):
- `subreddit_clustering_i18n`/`notebooks`/`v0.4.0`


# Getting started
## Environment
For quick experimentation, I started using GCP notebooks and PyCharm. See the markdown file for detailed instructions: [gcp_environment_setup.md](gcp_environment_setup.md).

TODO(djb) Work with MLEs (e.g., Elliott & Ugan) to set up a different workflow to make sure the models are ready for production.

## Installation
The fastest way to get started using utilities is to clone the repo to your environment and install the module in `editable` mode.
Note that the required libraries are listed in `setup.py` under `INSTALL_REQUIRES` (instead of `requirements.txt`). 

Editable mode (`--editable` or `-e`) makes it easy to continue editing your module and use the updated code without having to re-install it. This can speed up development when you pair it with jupyter's magic to automatically refresh edited code without having to re-import the package.

### Example installation in GCP notebooks
1) Assume sudo for your gcp user
<br>`sudo su - david.bermejo`
2) Use pip to install from location that's synced to PyCharm (active development).<br>
`pip install -e /home/david.bermejo/repos/subreddit_clustering_i18n/`
   - If you're installing a superset of requirements add: `[<extra_name>]` at the end of path

```
sudo su - david.bermejo

pip install -e /home/david.bermejo/repos/subreddit_clustering_i18n/

# or if you're installing a superset of requirements add `[<extra_name>]`
pip install -e /home/david.bermejo/repos/subreddit_clustering_i18n/[torch]
```

### Installing on a laptop/local
The heavy GPU work won't work on a standard laptop, but you can still install to do some local EDA. **NOTE** that the `[laptop_dev]` requirements includes `jupyterlab`.

Assuming you have `conda` installed, you can use the `Make` file to create a `venv` with the required libraries:<br>
`make install_requirements`

Otherwise, you'll have to do the steps manually:
1. Change your directory to my homework directory
2. Create a virtual environment (venv)
3. Activate the venv
4. Install the project's requirements via pip

```bash
# 1
cd XXX

# 2
python3 -m venv .venv

# 3
source .venv/bin/activate

# 4
python3 -m pip install -r requirements_laptop.txt
```

Then you can run a juypter lab server:
```bash
jupyter lab
```

### Reloading in jupyter
In jupyter, you can add this magic at the beginning of a notebook to reload edited code without having to re-import modules.
```
%load_ext autoreload
%autoreload 2

import subclu
```

# Project Organization (Cookiecutter)
**Note**: this is a work in progress, so some of this structure will change as I figure out my way around the available infrastructure. Some of the folders below might not be used.

    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── _00_external       <- Data from third party sources.
    │   ├── _01_raw            <- The original, immutable data dump.
    │   ├── _02_interim        <- Intermediate data that has been transformed.
    │   ├── _03_processed      <- The final, canonical data sets for modeling.
    │   └── embeddings         <- NLP embeddings used to convert posts & comments into vectors.
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         date notebook/analysis started, 
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `djb_1.00_2021-04-21-initial_data_exploration.ipynb`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │                         See full list in `setup.py` under `INSTALL_REQUIRES` variable
    │
    ├── setup.py           <- Makes project pip installable (e.g., pip install -e .)
    │                         This way we can `import subclu` and use it as any other library.
    │                         
    ├── subclu             <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
