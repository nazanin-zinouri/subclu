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


Project Organization (Cookiecutter)
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── _00_external       <- Data from third party sources.
    │   ├── _01_raw            <- The original, immutable data dump.
    │   ├── _02_interim        <- Intermediate data that has been transformed.
    │   └── _03_processed      <- The final, canonical data sets for modeling.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         date notebook/analysis started, 
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.00-2021-04-21-jqp-initial_data_exploration.ipynb`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
