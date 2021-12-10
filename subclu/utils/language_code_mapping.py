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
    'ro',  # Romanian - Latin mix? (Spanish, Portuguese, Italian?)

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
    'ru-Latn', # Russian

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

    # 'la',  # latin -- exclude this b/c it's prob mis-classification of Romance languages
    # 'fy',  # Western Frisian is prob mis-classification of Dutch or Germanic languages
    'hu',  # Hungarian
    # 'lb',  # Luxemburgish -- also prob a misclassification of German/French/Dutch
    'sq',  # Albanian

    'el',  # Greek
    'el-Latn',

    # Languages from this analysis
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

# Create a dataframe with language information
# we can then use this df to create a table in bigQuery
DF_LANGUAGE_MAPPING = (
    pd.DataFrame(
        [D_CLD3_CODE_TO_LANGUAGE_NAME]
    ).T
    .reset_index()
    .rename(columns={'index': 'language_code',
                     0: 'language_name'})
)

DF_LANGUAGE_MAPPING['language_name_top_only'] = np.where(
    DF_LANGUAGE_MAPPING['language_code'].isin(['UNKNOWN'] + L_CLD3_CODES_FOR_TOP_LANGUAGES_AND_USE_MULTILINGUAL),
    DF_LANGUAGE_MAPPING['language_name'],
    'Other_language'
)
DF_LANGUAGE_MAPPING['language_in_use_multilingual'] = np.where(
    DF_LANGUAGE_MAPPING['language_name'].isin(L_USE_MULTILINGUAL_LANGUAGE_NAMES),
    True,
    False
)
DF_LANGUAGE_MAPPING = DF_LANGUAGE_MAPPING.sort_values(
    by=['language_name'], ascending=True
)


#
# ~ fin
#

