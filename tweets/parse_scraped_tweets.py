'''
These functions process scraped tweets from the MBTA twitter feed. The tweets are stored in a directory with the following structure:

- MBTA_Twitter:
    - MBTA_tweets:
        tweet_id_1.json
        tweet_id_2.json
        ...
    - MBTA_images:
        image_id_1.jpg
        image_id_2.jpg
        ...

Where tweet_id is the unique identifier for each tweet. That is, you can always find the tweet by going to twitter.com/MBTA/status/tweet_id. Image ids are for 
the twing address for attached images. You can always find an image by going to pbs.twimg.com/media/<image_id>?format=png&name=orig

In their original format, it's hard to work with the tweets.
'''

import json
import os

def tweets_to_csv(tweets_dir, output_file):
    '''
    This function takes the tweets from the tweets directory and writes them to a csv file. The csv file has the following columns:
    