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


D_CLD3_CODE_TO_LANGUAGE_NAME = {
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
    'zh-Latn': 'Chinese',
    'zu': 'Zulu'
}

L_USE_MULTILINGUAL_LANGUAGES = [
    'Arabic',
    'Chinese-simplified',
    'Chinese-traditional',
    # Create a Chinese w/o qualifier so that it matches cld3 output:
    'Chinese',
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
    if lang_name in L_USE_MULTILINGUAL_LANGUAGES:
        L_CLD3_CODES_FOR_LANGUAGES_IN_USE_MULTILINGUAL.append(lang_code)


#
# ~ fin
#

