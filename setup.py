"""
Reference setup files
https://github.com/pypa/sampleproject/blob/main/setup.py
https://github.com/dask/dask/blob/main/setup.py
"""
from setuptools import find_packages, setup

# These libraries get installed always
INSTALL_REQUIRES = [
    # For some reason leaving `click` (& a few other libraries) unpinned creates
    #  weird conflicts might need to pin different versions for each VM, though
    # google VM pins
    "jinja2 <= 2.11.3",
    "markupsafe <= 1.1.1",
    "pydantic <= 1.8.2",

    # DS/ML core
    # pandas, np, and sklearn versions are managed by Google notebooks and
    #  are not guaranteed to be the same, so don't pin them to prevent errors
    "mlflow == 1.16.0",
    # "pandas == 1.2.4",
    # "scikit-learn == 0.24.1",
    # "joblib == 1.0.1",
    # "numpy == 1.19.5",

    # Exclude dask & modin while I test out spark
    "dask[complete] == 2021.6.0",
    # "modin == 0.10.0",
    # "modin[dask]",
    #  "ray",

    # Auth
    # "pydata-google-auth",

    # NLP
    # for v0.3 I'm no longer using fse, fasttext or spacy,
    #  Instead I'm using USE-multilingual & tensorflow, so move them
    #  to cpu-only extras to reduce conflicts & reduce install time
    "gensim == 3.8.3",  # 4.0 is not compatible with fse

    # Visualization
    "seaborn == 0.11.1",
    "plotly == 4.14.3",
    "kaleido == 0.2.1",

]

# These libraries will be installed only if called with "extras" syntax:
#   $ pip install sampleproject[dev]
EXTRAS_REQUIRE = {
    "cookiecutter": [
        # cookie cutter tools
        "python-dotenv >= 0.5.1",
        "Sphinx", "coverage", "awscli", "flake8"
    ],

    "cpu_eda": [
        "ipython == 7.22.0",
        "jupyterlab == 1.2.16",
        "pyarrow == 3.0.0",

        # pre-installed google libraries
        #  pin them to prevent pip from trying to over-write them
        "google-api-core == 1.26.2",
        "google-api-python-client == 2.1.0",
        "google-auth == 1.28.0",
        "google-auth-httplib2 == 0.1.0",
        "google-auth-oauthlib == 0.4.3",
        "google-cloud-bigquery == 2.13.1",
        "google-cloud-bigquery-storage == 2.4.0",
        "google-cloud-bigtable == 2.0.0",
        "google-cloud-core == 1.6.0",
        "google-cloud-dataproc == 2.3.1",
        "google-cloud-datastore == 2.1.0",
        "google-cloud-firestore == 2.1.0",
        "google-cloud-kms == 2.2.0",
        "google-cloud-language == 2.0.0",
        "google-cloud-logging == 2.3.1",
        "google-cloud-monitoring == 2.2.1",
        "google-cloud-pubsub == 1.7.0",
        "google-cloud-scheduler == 2.2.0",
        "google-cloud-spanner == 3.3.0",
        "google-cloud-speech == 2.2.1",
        "google-cloud-storage == 1.37.1",
        "google-cloud-tasks == 2.2.0",
        "google-cloud-translate == 3.1.0",
        "google-cloud-videointelligence == 2.1.0",
        "google-cloud-vision  == 2.3.0",

        # NLP / embeddings
        "fasttext == 0.9.2",
        "fse == 0.1.15",
        "spacy == 3.0.5",

        # compression / visualization
        "umap-learn == 0.5.1",
        "openTSNE == 0.6.0",

        # clustering
        # hdbscan needs to be installed separately, because it needs to be
        #  re-compiled with the right version of numpy
        #  pip install hdbscan --no-build-isolation --no-binary :all:
        # "hdbscan",

        # Use hydra to manage config files
        "hydra-core == 1.1.0",
    ],

    "pytorch": [
        # torch = pytorch GPU machine
        "torch == 1.8.0",
        "torchvision == 0.9.0+cu111",
    ],

    # tensorflow = extra libraries needed to run TF models
    # for some reason there's a numpy conflict with base install that
    # can interfere with tf==2.3.2
    "tensorflow_232": [
        "click <= 8.0.1",
        "numpy == 1.19.5",

        "tensorflow == 2.3.2",
        # "tensorflow-cloud == 0.1.13",
        # "tensorflow-data-validation == 0.26.1",
        # "tensorflow-datasets == 3.0.0",
        # "tensorflow-estimator == 2.3.0",
        # "tensorflow-hub == 0.9.0",
        # "tensorflow-io == 0.15.0",
        # "tensorflow-metadata == 0.26.0",
        # "tensorflow-model-analysis == 0.26.1",
        # "tensorflow-probability == 0.11.0",
        # "tensorflow-serving-api == 2.3.0",
        # "tensorflow-transform == 0.26.0",

        "tensorflow-text == 2.3.0",

        "Werkzeug == 2.0.1",
    ],

    "tensorflow_233": [
        # Core libraries need to be pinned... otherwise `pip` tries to upgrade them
        "click == 8.0.1",
        "joblib == 1.0.1",
        "pyarrow == 4.0.1",
        "numpy == 1.19.5",

        # Google pre-installed/built-in services
        #  For some reason, pip wanted to update a bunch of these
        #  when installing hydra-core
        "google-api-core == 1.30.0",
        "google-api-python-client == 2.10.0",
        "google-apitools == 0.5.31",
        "google-auth == 1.30.2",
        "google-auth-httplib2 == 0.1.0",
        "google-auth-oauthlib == 0.4.4",
        "google-cloud-appengine-logging == 0.1.1",
        "google-cloud-audit-log == 0.1.0",
        "google-cloud-bigquery == 2.20.0",
        "google-cloud-bigquery-storage == 2.4.0",
        "google-cloud-bigtable == 2.2.0",
        "google-cloud-build == 2.0.0",
        "google-cloud-core == 1.7.0",
        "google-cloud-dataproc == 2.4.0",
        "google-cloud-datastore == 2.1.3",
        "google-cloud-dlp == 1.0.0",
        "google-cloud-firestore == 2.1.3",
        "google-cloud-kms == 2.3.0",
        "google-cloud-language == 2.1.0",
        "google-cloud-logging == 2.5.0",
        "google-cloud-monitoring == 2.3.0",
        "google-cloud-pubsub == 1.7.0",
        "google-cloud-scheduler == 2.2.0",
        "google-cloud-spanner == 3.5.0",
        "google-cloud-speech == 2.4.1",
        "google-cloud-storage == 1.39.0",
        "google-cloud-tasks == 2.3.0",
        "google-cloud-translate == 3.2.1",
        "google-cloud-videointelligence == 2.2.0",
        "google-cloud-vision == 2.3.2",

        # TF pre-installed libraries
        "tensorflow == 2.3.3",
        "tensorflow-cloud == 0.1.13",
        "tensorflow-data-validation == 0.26.1",
        "tensorflow-datasets == 3.0.0",
        "tensorflow-estimator == 2.3.0",
        "tensorflow-hub == 0.9.0",
        "tensorflow-io == 0.15.0",
        "tensorflow-metadata == 0.26.0",
        "tensorflow-model-analysis == 0.26.1",
        "tensorflow-probability == 0.11.0",
        "tensorflow-serving-api == 2.3.0",
        "tensorflow-transform == 0.26.0",

        # TF library needed to use USE-multilingual
        "tensorflow-text == 2.3.0",

        # Use hydra to manage config files
        "hydra-core == 1.1.0",
    ],

}

# Don't do a 'complete' because we might have conflicting requirements between
#  CPU & GPU environments or TF & pyTorch
# EXTRAS_REQUIRE["complete"] = sorted({v for req in EXTRAS_REQUIRE.values() for v in req})


setup(
    name='subclu',
    packages=find_packages(),
    version='0.4.0',
    description='A package to identify clusters of subreddits and/or posts',
    author='david.bermejo@reddit.com',
    license='',
    python_requires=">=3.7",
    install_requires=INSTALL_REQUIRES,

    # Additional groups of dependencies here (e.g. development dependencies).
    # Users will be able to install these using the "extras" syntax, for example:
    #   $ pip install sampleproject[dev]
    extras_require=EXTRAS_REQUIRE,

)
