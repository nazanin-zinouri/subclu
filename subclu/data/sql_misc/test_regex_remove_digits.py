
"""
Regex to remove digits
Mostly used to clean up OCR text.

MOstly digits but also remove other characters that might not be useful


Test in:
https://regex101.com/
"""


regex_remove_digits = r"""
\d+[-:,\.]\d+([-:,\.]\d{2,4}){0,1}|\d|http[s]{0,1}://|www\.|/r/|\.html|reddit|\.com|\.org
"""


test_str = """
hellow 
Lineup today, 3 points? 
. 6 Monaten zusammen, 
* 20-40 Jahre, 
* keine hormonelle Verhütung, 
20,40
20,4,23
20,4,60234

http://
https://

12:30 AM
09-53-1234 to 09-21-09 or maybe 09.21.09
schalke04
2 Days to go 

Hey, w22 Für fußbilder und Nudes addet mich auf snapchat, sarah_h2462 30 Bilder und Videos plus Snappen, 20 Euro 

:  Alexisshv Q MyFans/a Video ....mov Video ...().mov Video 5.3.1.123.mov Video ....mov Video ....mov Video ....mov MFans Video .315.32.34.mov Video 
"""
