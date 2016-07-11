# -*- coding: utf-8 -*-

import json
import string
import unicodedata
from textblob import TextBlob
from pyspark import SparkContext, SparkConf
import os

conf = SparkConf().setAppName("SocialBase")
sc = SparkContext(conf=conf)

relevant_terms = ['socialbasebr','endomarketing','marketing','marketingdigital','comunicacaocomotime', 'planejamento','culturacolaborativa', 'redesocialcorporativa', 'comunicacao','lideranca','ownership','phygitalmarketing','inteligenciaemocional','digitalworkplace','gestaodoconhecimento','mobilidade','visaoemmarketing','gestaodepessoas','culturaorganizacional']

def remove_accents(data):
    return ''.join(x for x in unicodedata.normalize('NFKD', data) if x in string.ascii_letters).lower()


def filter_data(line):
    global relevant_terms
    try:
        tweet = json.loads(line)
        if not 'delete' in tweet.keys():
            blob = TextBlob(tweet['text'])
            if  blob.detect_language() != 'pt':
                text_pt = unicode(str(blob.translate(to="pt")),'utf-8')
            else:
                text_pt = tweet['text']
            text_pt = remove_accents(text_pt)

            for term in relevant_terms:
                if term in text_pt or term in tweet['text']:
                    return True
    except:
        pass
    return False

def compute_relevance(line):
    global relevant_terms
    tweet = json.loads(line)
    blob = TextBlob(tweet['text'])
    if  blob.detect_language() != 'pt':
        text_pt = unicode(str(blob.translate(to="pt")),'utf-8')
    else:
        text_pt = tweet['text']
    text_pt = remove_accents(text_pt)

    frequency = 0.0 
    for term in relevant_terms:
        if term in text_pt or term in tweet['text']:
            if term == 'socialbasebr':
                frequency += 10.0
            else:
                frequency += 1.0

    #mutual information
    value = 0.2*int(tweet['retweet_count']) + 0.3*int(tweet['favorite_count']) + 0.1*len(tweet['entities']['user_mentions']) + 0.4*frequency
    return (tweet['text'], value)

f = sc.textFile('tweets.json').cache()
results = f.filter(lambda line: filter_data(line))\
           .map(lambda line: compute_relevance(line))\
           .takeOrdered(100, key = lambda x: -x[1])

for r in results:
    try:
        blob = TextBlob(r[0])
        if  blob.detect_language() != 'en':
            blob = blob.translate(to="en")
        print "tweet:{} relevant:{} sentiment:{}".format(r[0].encode('utf8'), r[1], blob.sentiment.polarity)    
    except Exception as e:
        print str(e)