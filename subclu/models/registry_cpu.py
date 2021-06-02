"""
Model registry for CPU-based models

When training a model, this should be the central location for CPU-based models.
I'm separating CPU v GPU models because we need to prevent errors when loading models in case
they are not installed in the current environment
"""
from functools import partial
import gc
import logging
from logging import info
import re
import string
from typing import Iterable

from fse.models.average import FAST_VERSION
from fse.models import uSIF
from gensim.utils import tokenize
from gensim.models.fasttext import load_facebook_vectors

from ..data.fasttext_utils import (
    download_ft_pretrained_model,
    get_df_for_most_similar,
    get_project_subfolder,
)


def get_fasttext_usif(
        lang_id: str = 'de',
        workers: int = 10,
        length: int = 11,
        lang_freq: str = 'de',
        verbose: bool = True,
):
    """"""
    # Load a pretrained fasttext model & set up as fse.uSIF
    if verbose:
        info(f"  Getting pretrained model for language: {lang_id}...")
    f_ft_model = download_ft_pretrained_model(lang_id=lang_id, if_exists='ignore')

    ft = load_facebook_vectors(str(f_ft_model))
    multicore_on = FAST_VERSION == 1
    if not multicore_on:
        logging.warn(f"Multi-core for FSE is OFF")

    if verbose:
        info(f"  {len(ft.vocab):,.0f} <- Model vocabulary")
        info(f"  {multicore_on} <- True if `fse` is running in parallel..")
    gc.collect()

    fse_usif = uSIF(ft, workers=workers, length=length, lang_freq=lang_freq)

    return fse_usif



D_MODELS_CPU = {
    'fasttext_usif_de': get_fasttext_usif,

}


# Custom functions to split text
# TODO(djb): would these functions be better as a class than as individual functions?
def split_base(
        text_string: str,
        lower: bool = False
) -> Iterable[str]:
    """"""
    if lower:
        return text_string.lower().split()
    else:
        return text_string.split()


def split_sklearn(
        text_string: str,
        lower: bool = False
) -> Iterable[str]:
    """
    NOTE: this tokenizer may remove contractions, which could screw up meanings and negations.
    Also, this tokenizer will remove single letter words, like "I" or "u"
    For example:
    - "I'll"    -> ["ll"]
    - "can't"   -> ["can"]

    Args:
        text_string:
        lower:

    Returns:
    """
    token_pattern = re.compile(r"(?u)\b\w\w+\b")
    if lower:
        return token_pattern.findall(text_string.lower())
    else:
        return token_pattern.findall(text_string)


def split_sklearn_plus_acronyms(
        text_string: str,
        lower: bool = False
) -> Iterable[str]:
    """
    NOTE: this tokenizer may remove contractions, which could screw up meanings and negations.
    Also, this tokenizer will remove single letter words, like "I" or "u"
    This one tries to recover some acronyms like U.S.A, U.K., U.K that sklearn might miss.

    Args:
        text_string:
        lower:

    Returns:
    """
    token_pattern = re.compile(r"(?u)\b\w\w+\b|\b\w\.\w\.\w|\b\w\.\w")
    if lower:
        return token_pattern.findall(text_string.lower())
    else:
        return token_pattern.findall(text_string)


def split_sklearn_remove_digits(
        text_string: str,
        lower: bool = False
) -> Iterable[str]:
    """"""
    remove_digits = re.compile(r"\d")
    if lower:
        return split_sklearn(re.sub(remove_digits, '', text_string),
                             lower=True)
    else:
        return split_sklearn(re.sub(remove_digits, '', text_string),
                             lower=False)


# def split_sklearn_remove_digits_lower(
#         text_string: str,
# ) -> Iterable[str]:
#     """"""
#     remove_digits = re.compile(r"\d")
#     return split_sklearn(re.sub(remove_digits, '', text_string),
#                          lower=True)


# def split_sklearn_lower(
#         text_string: str,
# ) -> Iterable[str]:
#     """
#
#     Args:
#         text_string:
#
#     Returns:
#
#     """
#     return split_sklearn(text_string, lower=True)


def split_sklearn_keep_emoji(text_string: str, lower: bool = False):
    """
    Based on split_sklearn but adds a new clause to try to keep emoji

    This pattern ended up not working, something about the way python sometimes handles emoji:
    r"\\[U|u][\d|\w|-]+"

    Args:
        text_string:
        lower:

    Returns: list of tokens
    """
    # keep question marks because they could help with Q&A detection \?
    # token_pattern = re.compile(r"(?u)\b\w\w+\b|[^\w\s\|\+\\:/,;\-.\(\)\[\]\<\>\#\!]")
    token_pattern = re.compile(fr"(?u)\b\w\w+\b|[^\w\s\\{string.punctuation}]")
    if lower:
        return token_pattern.findall(text_string.lower())
    else:
        return token_pattern.findall(text_string)


# def split_sklearn_keep_emoji_lower(text_string: str):
#     """
#     Force lower case
#     Args:
#         text_string:
#
#     Returns: list of tokens
#     """
#     return split_sklearn_keep_emoji(text_string, lower=True)


def split_sklearn_keep_emoji_remove_digits_lower(text_string: str):
    """"""
    remove_digits = re.compile(r"\d")
    return split_sklearn_keep_emoji(re.sub(remove_digits, '', text_string),
                                    lower=True)


D_CUSTOM_SPLIT = {
    # TODO(djb) these should be sklearn options, not their own fxns:
    #  - remove digits
    #  - keep emoji
    'base': split_base,
    'sklearn': split_sklearn,
    'sklearn_lower': partial(split_sklearn, lower=True),
    'gensim': tokenize,
    'sklearn_keep_emoji': split_sklearn_keep_emoji,
    # 'sklearn_keep_emoji_lower': split_sklearn_keep_emoji_lower,
    'sklearn_remove_digits': split_sklearn_remove_digits,
    # 'sklearn_remove_digits_lower': split_sklearn_remove_digits_lower,
    'sklearn_keep_emoji_remove_digits_lower': split_sklearn_keep_emoji_remove_digits_lower,
    'sklearn_plus_acronyms': split_sklearn_plus_acronyms,
}


#
# ~ fin
#
