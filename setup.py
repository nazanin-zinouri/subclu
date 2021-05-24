"""
Reference setup files
https://github.com/pypa/sampleproject/blob/main/setup.py
https://github.com/dask/dask/blob/main/setup.py
"""

from setuptools import find_packages, setup

# These libraries get installed always
INSTALL_REQUIRES = [
    # cookie cutter tools
    "python-dotenv >= 0.5.1",
    "click",

    # DS/ML core
    "pandas == 1.2.4",
    "scikit-learn == 0.24.1",
    "joblib == 1.0.1",
    "numpy == 1.19.5",
    "mlflow == 1.16.0",

    # Auth
    "pydata-google-auth",

    # NLP
    "gensim == 3.8.3",  # 4.0 is not compatible with fse
    "fse == 0.1.15",
    "fasttext == 0.9.2",

    # Visualization
    "seaborn == 0.11.1",
    "plotly == 4.14.3",
    "kaleido == 0.2.1",

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
    # torch = pytorch GPU machine
    "torch": [
        "torch == 1.8.0",
        "torchvision == 0.9.0+cu111",
    ],
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
