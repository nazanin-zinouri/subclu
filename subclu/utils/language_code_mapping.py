"""
Utils to map codes to languages or countries

Note that we need different systems:
For google's cld3 library, I pulled the table in their readme:
- https://github.com/google/cld3#supported-languages
We could also use Unicode's cldr:
- https://github.com/unicode-cldr/cldr-localenames-modern/blob/master/main/en/languages.json

For IP geolocation we'll need to research what coding system they use.
TODO(djb)

"""
import numpy as np
import pandas as pd


D_CLD3_CODE_TO_LANGUAGE_NAME = {
    # manually added when language unknown
    "UNKNOWN": "Unknown",

    # manually added from JSON file:
    "tl": "Tagalog",
    "he": "Hebrew",
    "az": "Azerbaijani",
    "be": "Belarusian",

    # From Google's github page:
    # 'Output Code': 'Language Name'
    'af': 'Afrikaans',
    'am': 'Amharic',
    'ar': 'Arabic',
    'bg': 'Bulgarian',
    'bg-Latn': 'Bulgarian',
    'bn': 'Bangla',
    'bs': 'Bosnian',
    'ca': 'Catalan',
    'ceb': 'Cebuano',
    'co': 'Corsican',
    'cs': 'Czech',
    'cy': 'Welsh',
    'da': 'Danish',
    'de': 'German',
    'el': 'Greek',
    'el-Latn': 'Greek',
    'en': 'English',
    'eo': 'Esperanto',
    'es': 'Spanish',
    'et': 'Estonian',
    'eu': 'Basque',
    'fa': 'Persian',
    'fi': 'Finnish',
    'fil': 'Filipino',
    'fr': 'French',
    'fy': 'Western Frisian',
    'ga': 'Irish',
    'gd': 'Scottish Gaelic',
    'gl': 'Galician',
    'gu': 'Gujarati',
    'ha': 'Hausa',
    'haw': 'Hawaiian',

    'hi': 'Hindi',
    'hi-Latn': 'Hindi',

    'hmn': 'Hmong',
    'hr': 'Croatian',
    'ht': 'Haitian Creole',
    'hu': 'Hungarian',
    'hy': 'Armenian',
    'id': 'Indonesian',
    'ig': 'Igbo',
    'is': 'Icelandic',
    'it': 'Italian',
    'iw': 'Hebrew',

    'ja': 'Japanese',
    'ja-Latn': 'Japanese',

    'jv': 'Javanese',
    'ka': 'Georgian',
    'kk': 'Kazakh',
    'km': 'Khmer',
    'kn': 'Kannada',
    'ko': 'Korean',
    'ku': 'Kurdish',
    'ky': 'Kyrgyz',
    'la': 'Latin',
    'lb': 'Luxembourgish',
    'lo': 'Lao',
    'lt': 'Lithuanian',
    'lv': 'Latvian',
    'mg': 'Malagasy',
    'mi': 'Maori',
    'mk': 'Macedonian',
    'ml': 'Malayalam',
    'mn': 'Mongolian',
    'mr': 'Marathi',
    'ms': 'Malay',
    'mt': 'Maltese',
    'my': 'Burmese',
    'ne': 'Nepali',
    'nl': 'Dutch',
    'no': 'Norwegian',
    'ny': 'Nyanja',
    'pa': 'Punjabi',
    'pl': 'Polish',
    'ps': 'Pashto',
    'pt': 'Portuguese',
    'ro': 'Romanian',
    'ru': 'Russian',
    'ru-Latn': 'Russian',

    'sd': 'Sindhi',
    'si': 'Sinhala',
    'sk': 'Slovak',
    'sl': 'Slovenian',
    'sm': 'Samoan',
    'sn': 'Shona',
    'so': 'Somali',
    'sq': 'Albanian',
    'sr': 'Serbian',
    'st': 'Southern Sotho',
    'su': 'Sundanese',
    'sv': 'Swedish',
    'sw': 'Swahili',
    'ta': 'Tamil',
    'te': 'Telugu',
    'tg': 'Tajik',
    'th': 'Thai',
    'tr': 'Turkish',
    'uk': 'Ukrainian',
    'ur': 'Urdu',
    'uz': 'Uzbek',
    'vi': 'Vietnamese',
    'xh': 'Xhosa',
    'yi': 'Yiddish',
    'yo': 'Yoruba',

    'zh': 'Chinese',
    'zh-cn': 'Chinese',  # Chinese simplified
    'zh-Latn': 'Chinese',
    'zh-tw': 'Chinese',

    'zu': 'Zulu',
}

# The BigQuery table is broken, so we need to map some numeric IDs to text codes
#  data-prod-165221.language_detection.language_code_reference
MAP_CLD3_IDS_TO_LANGUAGE_CODES = {
    'af': 1,  # (a)f
    'am': 2,  # (a)m
    'ar': 3,  # (a)r
    'bg': 4,  # (b)g
    'bg-Latn': 5,  # (b)g-Latn
    'bn': 6,  # (b)n
    'bs': 7,  # (b)s
    'ca': 8,  # (c)a
    'ceb': 9,  # (c)eb
        'co': 10,
    'cs': 11,
    'cy': 12,
    'da': 13,
    'de': 14,
    'el': 15,
    'el-Latn': 16,
    'en': 17,
    'eo': 18,
    'es': 19,
    'et': 20,
    'eu': 21,
    'fa': 22,
    'fi': 23,
    'fil': 24,
    'fr': 25,
    'fy': 26,
    'ga': 27,
    'gd': 28,
    'gl': 29,
    'gu': 30,
    'ha': 31,
    'haw': 32,
    'hi': 33,
    'hi-Latn': 34,
    'hmn': 35,
    'hr': 36,
    'ht': 37,
    'hu': 38,
    'hy': 39,
    'id': 40,
    'ig': 41,
    'is': 42,
    'it': 43,
    'iw': 44,
    'ja': 45,
    'ja-Latn': 46,
    'jv': 47,
    'ka': 48,
    'kk': 49,
    'km': 50,
    'kn': 51,
    'ko': 52,
    'ku': 53,
    'ky': 54,
    'la': 55,
    'lb': 56,
    'lo': 57,
    'lt': 58,
    'lv': 59,
    'mg': 60,
    'mi': 61,
    'mk': 62,
    'ml': 63,
    'mn': 64,
    'mr': 65,
    'ms': 66,
    'mt': 67,
    'my': 68,
    'ne': 69,
    'nl': 70,
    'no': 71,
    'ny': 72,
    'pa': 73,
    'pl': 74,
    'ps': 75,
    'pt': 76,
    'ro': 77,
    'ru': 78,
    'ru-Latn': 79,
    'sd': 80,
    'si': 81,
    'sk': 82,
    'sl': 83,
    'sm': 84,
    'sn': 85,
    'so': 86,
    'sq': 87,
    'sr': 88,
    'st': 89,
    'su': 90,
    'sv': 91,
    'sw': 92,
    'ta': 93,
    'te': 94,
    'tg': 95,
    'th': 96,
    'tr': 97,
    'uk': 98,
    'ur': 99,
    'uz': 100,
    'vi': 101,
    'xh': 102,
    'yi': 103,
    'yo': 104,
    'zh': 105,
    'zh-Latn': 106,
    'zu': 107,
}


L_USE_MULTILINGUAL_LANGUAGE_NAMES = [
    'Arabic',
    # USE-multilingual differentiates simplified & traditional Chinese
    #  cld3 does not, so create a Chinese w/o qualifier so that it matches cld3 output:
    'Chinese',
    'Chinese-simplified',
    'Chinese-traditional',
    'English',
    'French',
    'German',
    'Italian',
    'Japanese',
    'Korean',
    'Dutch',
    'Polish',
    'Portuguese',
    'Spanish',
    'Thai',
    'Turkish',
    'Russian',
]

# create map of language codes that belong to a USE-multilingual language
#  what way we can filter the output columns without having to replace/convert
#  all the codes to the Language before filtering
L_CLD3_CODES_FOR_LANGUAGES_IN_USE_MULTILINGUAL = list()
for lang_code, lang_name in D_CLD3_CODE_TO_LANGUAGE_NAME.items():
    if lang_name in L_USE_MULTILINGUAL_LANGUAGE_NAMES:
        L_CLD3_CODES_FOR_LANGUAGES_IN_USE_MULTILINGUAL.append(lang_code)

# These codes were extracted from top subreddit POSTS extract from:
#  top_subreddits_2021-07_16.yaml
# Languages with * next to them are high priority for i18n

# New approach:
# It's easier to flag the languages to group as "other language"
#   than the "top" language
L_CLD3_CODES_FOR_LANGUAGE_NAMES_TO_GROUP_AS_OTHER = [
    'lo',
    'km',
    'gu',
    'my',
    'kn',
    'am',
    'si',
    'hy',
    'yi',
    'ps',
    'ka',
    'bn',
]

L_CLD3_CODES_FOR_TOP_LANGUAGES_USED_AT_REDDIT = [
    'en',  # English
    'de',  # * German *
    'pt',  # * Portuguese *
    'es',  # * Spanish *
    'fr',  # * French *
    'it',  # * Italian *

    'af',  # Afrikaans - usually English slang can be misclassified this way
    'nl',  # Dutch
    'no',  # Norwegian - English/Germanic language mix ups

    'tl',  # Tagalog
    'so',  # Somali
    'id',  # Indonesian

    'da',  # Danish - English/Germanic language mix ups
    'cy',  # Welsh - not sure how this is so high
    'ca',  # Catalan - Spanish & Portuguese gets misclassified as this
    'ro',  # Romanian - Latin mix (Spanish, Portuguese, Italian?)

    'sv',  # Swedish
    'tr',  # Turkish
    'et',  # Estonian

    'pl',  # Polish
    'fi',  # Finnish
    'sw',  # Swahili

    'sl',  # Slovenian
    'hr',  # Croatian

    'vi',  # Vietnamese
    'ru',  # Russian
    'ru-Latn',  # Russian

    'ja',  # Japanese
    'ja-Latn',

    'zh',  # Chinese
    'zh-Latn',
    'zh-cn',  # Chinese, simplified
    'zh-tw',  # Chinese, Taiwan

    # Languages in India
    'hi',   # Hindi
    'hi-Latn',  # Hindi
    'mr',  # 'Marathi'
    'ta',  # 'Tamil'
    'te',  # 'Telugu'
    'ml',  # 'Malayalam'

    'la',  # latin -- it's prob mis-classification of Romance languages
    'fy',  # Western Frisian is prob mis-classification of Germanic languages
    'hu',  # Hungarian
    'lb',  # Luxemburgish -- also prob a misclassification of German/French/Dutch
    'sq',  # Albanian

    'el',  # Greek
    'el-Latn',

    # Other top detected languages 2022-08-04
    'mg',   # Malagasy
    'mt',   # Maltese
    'haw',  # Hawaiian - Seems common with abbreviations
    'jv',   # Javanese
    'fil',  # Filipino
    'sn',   # Shona
    'ig',   # Igbo
    'zu',   # Zulu
    'gl',   # Galician


    # Other languages from this analysis
    # https://towardsdatascience.com/the-most-popular-languages-on-reddit-analyzed-with-snowflake-and-a-java-udtf-4e58c8ba473c
    'bs',  # Bosnian
    'sr',  # Serbian
    'is',  # Icelandic
    'bg',  # 'Bulgarian'
    'bg-Latn',  # 'Bulgarian'
    'ms',  # 'Malay'
    "he",  # "Hebrew"
    'iw',  # 'Hebrew'
    'ko',  # 'Korean'
    'st',  # 'Southern Sotho'

]

L_CLD3_CODES_FOR_TOP_LANGUAGES_AND_USE_MULTILINGUAL = list(
    set(L_CLD3_CODES_FOR_LANGUAGES_IN_USE_MULTILINGUAL) |
    set(L_CLD3_CODES_FOR_TOP_LANGUAGES_USED_AT_REDDIT)
)


def get_df_language_mapping(
) -> pd.DataFrame:
    """
    Use the dicts & lists above to create a dataframe with language information
    we can then use this df to create a table in bigQuery.
    TODO(djb): Dislike using local global vars, but we'll fix it later
    """
    df_language_mapping = (
        pd.DataFrame(
            [D_CLD3_CODE_TO_LANGUAGE_NAME]
        ).T
            .reset_index()
            .rename(columns={'index': 'language_code',
                             0: 'language_name'})
    )

    df_language_mapping['language_name_top_only'] = np.where(
        df_language_mapping['language_code'].isin(L_CLD3_CODES_FOR_LANGUAGE_NAMES_TO_GROUP_AS_OTHER),
        'Other_language',
        df_language_mapping['language_name']
    )
    df_language_mapping['language_in_use_multilingual'] = np.where(
        df_language_mapping['language_name'].isin(L_USE_MULTILINGUAL_LANGUAGE_NAMES),
        True,
        False
    )

    mask_langcode_has_id = df_language_mapping['language_code'].isin(MAP_CLD3_IDS_TO_LANGUAGE_CODES.keys())
    df_language_mapping['language_id'] = np.where(
        mask_langcode_has_id,
        df_language_mapping['language_code'].replace(MAP_CLD3_IDS_TO_LANGUAGE_CODES),
        -1,
    )
    df_language_mapping['language_id'] = df_language_mapping['language_id'].astype(int)

    df_language_mapping = df_language_mapping.sort_values(
        by=['language_name'], ascending=True
    )
    return df_language_mapping


DF_LANGUAGE_MAPPING = get_df_language_mapping()


# TODO(djb) add language family mapping(?)
#  i.e., Germanic & Romance languages b/c individual language can be noisy?
#  Potential sources:
#  https://en.wikipedia.org/wiki/ISO_639
#  https://opentext.wsu.edu/introtohumangeography/chapter/5-3-classification-and-distribution-of-languages/
#  https://www.theguardian.com/education/gallery/2015/jan/23/a-language-family-tree-in-pictures
#  https://ielanguages.com/classification-languages.html


#
# ~ fin
#

