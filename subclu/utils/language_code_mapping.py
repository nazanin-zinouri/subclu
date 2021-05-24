"""
Utils to map codes to languages or countries

Note that we need different systems:
For google's cld3 library we use Unicode's cldr:
- https://github.com/unicode-cldr/cldr-localenames-modern/blob/master/main/en/languages.json
TODO(djb): load json file as a python dict

For IP geolocation we'll need to research what coding system they use.
TODO(djb)
"""

#
# ~ fin
#
