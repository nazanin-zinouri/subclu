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

    # Auth
    "pydata-google-auth",

    # NLP
    "gensim == 4.0.1",
    "fse == 0.1.15",

    # Visualization
    "seaborn == 0.11.1",
    "plotly == 4.14.3"

]

# These libraries will be installed only if called with "extras" syntax:
#   $ pip install sampleproject[dev]
EXTRAS_REQUIRE = {
    "cookiecutter": ["Sphinx", "coverage", "awscli", "flake8"],
    "dev": ["jupyterlab >= 1.2.16", "ipython == 7.22.0",
            "pyarrow == 3.0.0",
            ],
}
EXTRAS_REQUIRE["complete"] = sorted({v for req in EXTRAS_REQUIRE.values() for v in req})


setup(
    name='subclu',
    packages=find_packages(),
    version='0.1.0',
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
