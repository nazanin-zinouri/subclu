"""
Utils to preprocess text.
Using classes/functions/pipelines makes it easier to make sure
we apply the same preprocessing at training & inference.
"""
from functools import partial
import logging
from logging import info
import re
import string
from typing import Union, Iterable

from gensim.utils import tokenize
from fse import CSplitCIndexedList
import pandas as pd

# from spacy.lang import en, de
# can get stopwords via: de.STOP_WORDS

from sklearn.base import BaseEstimator, TransformerMixin


class TextPreprocessor(BaseEstimator, TransformerMixin):
    """"""
    # custom class to preprocess text before passing it
    # into NLP models. Core use case is fse/fastText models,
    #  which need a custom format
    def __init__(
            self,
            lowercase: bool = True,
            tokenizer_function: Union[callable, str] = None,
            remove_digits: bool = False,
            return_fse_format: bool = True,
            fse_format=CSplitCIndexedList,
            verbose: bool = True,
    ):
        self.lowercase = lowercase
        self.remove_digits = remove_digits
        self.return_fse_format = return_fse_format
        self.fse_format = fse_format
        self.verbose = verbose

        if tokenizer_function is not None:
            self.tokenizer_function = partial(
                transform_and_tokenize_text, tokenizer=tokenizer_function)
        else:
            self.tokenizer_function = None

    def fit(self, X, y=None):
        """
        In our case, we don't fit data, but the method is required
        by sklearn

        Args:
            X:
            y:

        Returns: self
        """
        return self

    def transform(self,
                  X: pd.Series,
                  y=None,
                  ) -> pd.Series:
        """
        Apply transformation functions.
        if return_fse_format = True, return format based on fse_fxn

        Args:
            X:
            y:

        Returns:

        """
        X_transformed = X.copy()

        # would it be faster to apply all functions at once?
        #  instead of applying them sequentially
        if self.lowercase:
            if self.verbose:
                info(f"Converting to lowercase...")
            X_transformed = X_transformed.str.lower()
        if self.remove_digits:
            if self.verbose:
                info(f"Removing digits...")
            re_remove_digits = re.compile(r"\d")
            X_transformed = X_transformed.apply(
                lambda text_: re.sub(re_remove_digits, '', text_)
            )
        # if self.return_fse_format:
        #     # TODO(djb): maybe this should belong to the fse/USIF model
        #     #  so that I can create the lookup dict functions inside of
        #     #  the FSE model instead of the preprocessing step
        #     #  leave tokenizing as a FSE/USIF step
        #     logging.warning(f"FSE NOT IMPLEMENTED YET")
        #     raise NotImplementedError
        # else:
        if self.tokenizer_function is not None:
            return X_transformed.apply(self.tokenizer_function)
        else:
            return X_transformed

    def fit_transform(self, X, y=None, **fit_params):
        return self.transform(X)


def transform_and_tokenize_text(
        doc: str,
        tokenizer: str,
        lowercase: bool = False,
        # remove_digits: bool = False,
) -> Iterable[str]:
    """
    sklearn: 2 or more characters.
        May remove contractions, which could screw up meanings and negations.
        Also, this tokenizer will remove single letter words, like "I" or "u"
        For example:
        - "I'll"    -> ["ll"]
        - "can't"   -> ["can"]

    sklearn_acronyms:
        Similar to sklear, but also tries to recover 2 or 3 letter acronyms like
        U.S.A, U.K., U.K that sklearn might miss.

    sklearn_emoji:
        Based on split_sklearn but adds a new clause to try to keep emoji

    sklearn_acronyms_emoji:
        3 clauses tries to get all items that sklearn gets PLUS acronyms PLUS
        emoji

    Args:
        doc:
        tokenizer:
        lowercase:

    Returns:

    """
    D_REGEX = {
        'sklearn': re.compile(r"(?u)\b\w\w+\b"),
        'sklearn_acronyms': re.compile(r"(?u)\b\w\w+\b|\b\w\.\w\.\w|\b\w\.\w"),
        'sklearn_emoji': re.compile(fr"(?u)\b\w\w+\b|[^\w\s\\{string.punctuation}]"),
        'sklearn_acronyms_emoji': re.compile(fr"(?u)\b\w\w+\b|\b\w\.\w\.\w|\b\w\.\w|[^\w\s\\{string.punctuation}]"),
    }
    try:
        # exclude lowercase check and do it at vector level?
        if lowercase:
            return D_REGEX[tokenizer].findall(doc.lower())
        else:
            return D_REGEX[tokenizer].findall(doc)

    except KeyError:
        if tokenizer == 'split':
            if lowercase:
                return doc.lower().split()
            else:
                return doc.split()

        elif tokenizer == 'gensim':
            return tokenize(doc, lowercase=lowercase)
        else:
            raise NotImplementedError(f"Unknown tokenizer: {tokenizer}")

#
# ~ fin
#
