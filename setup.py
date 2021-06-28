"""
Reference setup files
https://github.com/pypa/sampleproject/blob/main/setup.py
https://github.com/dask/dask/blob/main/setup.py
"""

from setuptools import find_packages, setup

# These libraries get installed always
INSTALL_REQUIRES = [
    # cookie cutter tools
    # "python-dotenv >= 0.5.1",
    # For some reason leaving click unpinned creates weird conflicts
    "click == 8.0.1",

    # DS/ML core
    # "pandas == 1.2.4",
    # "scikit-learn == 0.24.1",
    # "joblib == 1.0.1",
    # np version might not be tf2 version
    # "numpy == 1.19.5",
    "mlflow == 1.16.0",
    "dask == 2021.6.0",
    "modin == 0.10.0",

    # Auth
    "pydata-google-auth",

    # NLP
    "gensim == 3.8.3",  # 4.0 is not compatible with fse
    "fse == 0.1.15",
    "fasttext == 0.9.2",
    "spacy == 3.0.5",

    # Visualization
    "seaborn == 0.11.1",
    "plotly == 4.14.3",
    "kaleido == 0.2.1",
    "umap-learn == 0.5.1",
    "openTSNE == 0.6.0",

]

# These libraries will be installed only if called with "extras" syntax:
#   $ pip install sampleproject[dev]
EXTRAS_REQUIRE = {
    "cookiecutter": ["Sphinx", "coverage", "awscli", "flake8"],
    "devcpu": [
        "jupyterlab >= 1.2.16",
        "ipython == 7.22.0",
        "pyarrow == 3.0.0",
    ],

    "pytorch": [
        # torch = pytorch GPU machine
        "torch == 1.8.0",
        "torchvision == 0.9.0+cu111",
    ],

    # tensorflow = extra libraries needed to run TF models
    # for some reason there's a numpy conflict with base install that
    # interferes with tf==2.3.2
    "tensorflow_232": [
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
    ]
}

# Don't do a 'complete' because we might have conflicting requirements between
#  CPU & GPU environments or TF & pyTorch
# EXTRAS_REQUIRE["complete"] = sorted({v for req in EXTRAS_REQUIRE.values() for v in req})


setup(
    name='subclu',
    packages=find_packages(),
    version='0.1.1',
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
