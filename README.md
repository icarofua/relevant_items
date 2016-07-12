# relevant_items

Using some relevant terms extracted from tweets, I develop a spark code to filter the tweets that contain at least one term in the tweet. For each term found in the tweet it is calculated a frequency.

Finally, I calculated a mutual information using the frequency of the relevant terms, retweet count, mention count and favorites count.

To use the code you need update the tweets.json and you need the Spark library, after you can run:
```
spark-submit tweets_spark.py
```
 
