
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
$ , II I > l + ,  e + 
Lineup today, 3 points? 
. 6 Monaten zusammen, 
* 20-40 Jahre, 
* keine hormonelle Verhütung, 
20,40
20,4,23
20,4,60234

In #Leipzig #Stötteritz
r/Fuss
u/terty

http://
https://

-  ii  II I>   .M all +.M ,   A   @  e +  +  + A+  Fort Kaldwin
 II I>   M all M ,   A   @  e      A
 $ M ll - 


12:30 AM
09-53-1234 to 09-21-09 or maybe 09.21.09
schalke04
2 Days to go 

Hey, w22 Für fußbilder und Nudes addet mich auf snapchat, sarah_h2462 30 Bilder und Videos plus Snappen, 20 Euro 

:  Alexisshv Q MyFans/a Video ....mov Video ...().mov Video 5.3.1.123.mov Video ....mov Video ....mov Video ....mov MFans Video .315.32.34.mov Video 

لطالما كنت مفتونًا بالهياكل التاريخية ، خاصة السكك الحديدية والأنفاق والجسور. هذه ببساطة أشياء كانت موجودة في كثير من الأحيان لأكثر من 100 عام ، والتي أسأل نفسي عنها دائمًا: ما الذي حدث بالفعل هناك أنه هو نفسه تمامًا اليوم. في شتوتجارت مرة أخرى صارخ بشكل خاص بسبب موقع الحوض ..

# JAPANESE
私は常に歴史的建造物、特​​に鉄道、トンネル、橋に魅了されてきました。 これらは、100年以上前から存在していることが多いものであり、私は常に自分自身に問いかけています。それは、今日とまったく同じであるということで、そこですでに起こったことに違いありません。 シュトゥットガルトでは、盆地の場所のために再び特に露骨に..

# CHINESE SIMPLIFIED
我一直对历史建筑着迷，尤其是铁路、隧道、桥梁。 这些只是通常已经存在了 100 多年的事情，为此我总是问自己：那里一定已经发生了什么，今天完全一样。 在斯图加特再次因为盆地位置而特别公然..

 
"""
