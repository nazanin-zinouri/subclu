# imports
from datetime import datetime
import logging

import numpy as np
import pandas as pd
import plotly
import plotly.express as px
import seaborn as sns

import mlflow

import subclu
from subclu.utils import set_working_directory
from subclu.utils.eda import (
    setup_logging, counts_describe, value_counts_and_pcts,
    notebook_display_config, print_lib_versions,
    style_df_numeric
)
from subclu.utils.mlflow_logger import MlflowLogger
from subclu.eda.aggregates import compare_raw_v_weighted_language
from subclu.utils.data_irl_style import (
    get_colormap, theme_dirl
)

# notebook specific imports
import html
import io
import os

# Imports the Google Cloud client libraries
from google.api_core.exceptions import AlreadyExists
# from google.cloud import texttospeech
from google.cloud import translate_v3beta1 as translate3
from google.cloud import translate_v2 as translate2
# from google.cloud import vision


print_lib_versions([np, pd, plotly, sns, subclu])


# plotting
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.dates as mdates
plt.style.use('default')

setup_logging()
notebook_display_config()

"""
# Make basic call to API


Translate v2 docs
- https://cloud.google.com/translate/docs/basic/quickstart

Translate v3 docs
- https://cloud.google.com/translate/docs/advanced/hybrid-glossaries-tutorial
"""


def translate_text_glossary(
        text: str,
        source_language_code: str = None,
        target_language_code: str = 'de',
        project_id: str = None,
        # glossary_name,
):
    """Translates text to a given language using a glossary


    ARGS
    text: String of text to translate
    source_language_code: language of input text
    target_language_code: language of output text
    project_id: GCP project id
    glossary_name: name you gave your project's glossary
        resource when you created it

    RETURNS
    String of translated text
    """

    # Instantiates a client
    client = translate.TranslationServiceClient()

    # Designates the data center location that you want to use
    location = "us-central1"

    # glossary = client.glossary_path(project_id, location, glossary_name)
    # glossary_config = translate.TranslateTextGlossaryConfig(glossary=glossary)

    parent = f"projects/{project_id}/locations/{location}"

    result = client.translate_text(
        request={
            "parent": parent,
            "contents": [text],
            "mime_type": "text/plain",  # mime types: text/plain, text/html
            "source_language_code": source_language_code,
            "target_language_code": target_language_code,
            # "glossary_config": glossary_config,
        }
    )

    # Extract translated text from API response
    return result.glossary_translations[0].translated_text


def translate_text(
    text: str,
    source_language: str = None,
    target_language: str = 'de',
    format_: str = 'text',
):
    """Translates text into the target language.

    Target must be an ISO 639-1 language code.
    See https://g.co/cloud/translate/v2/translate-reference#supported_languages
    """
    translate_client = translate2.Client()

    # Text can also be a sequence of strings, in which case this method
    # will return a sequence of results for each text.
    result = translate_client.translate(
        text,
        source_language=source_language,
        target_language=target_language,
        format_=format_,
    )

    print(f"Input Text: {result['input']}")
    print(f"Translation: {result['translatedText']}")
    print(f"Detected source language: {result['detectedSourceLanguage']}")
    return result


r = translate_text('hello! will this translation work?')
"""
Input Text: hello! will this translation work?
Translation: Hallo! Wird diese Übersetzung funktionieren?
Detected source language: en
"""

for k, v in r.items():
    print(f"{k}: {v}")

"""
translatedText: Hallo! Wird diese Übersetzung funktionieren?
detectedSourceLanguage: en
input: hello! will this translation work?
"""


"""
# get documentation for translate fxn
translate_client = translate2.Client()

?translate_client.translate
"""
