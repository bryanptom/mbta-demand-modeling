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
import pandas as pd

def image_or_video_id_from_url(url: str) -> str:
    '''
    This function takes a url and returns the image or video id. The url is in the format:

        https://pbs.twimg.com/media/<image_id>?format=jpg&name=orig

    for images or

        https://video.twimg.com/amplify_video/XXXXXXXX/vid/1280x720/<video_id>.mp4?tag=13

    for videos. This function returns the id from images and the full url from videos, since those are not dowmloaded. 

    params:
    - url: The url to extract the id from
    returns:
    - id/url: The id of the image or url from the video
    '''
    try:
        if url.startswith('https://pbs.twimg.com/media/'):
            return url.split('/')[-1].split('?')[0]
        elif url.startswith('https://video.twimg.com/amplify_video/'):
            return url
        else:
            print(f'Invalid url {url}')
            raise ValueError('Invalid url')
    
    except Exception as e:
        print(f'Error processing url {url}')
        raise ValueError('Invalid url')

def tweet_jsons_to_csv(tweets_dir: str | os.PathLike, output_file: str | os.PathLike):
    '''
    This function takes the tweets from the tweets directory and writes them to a csv file. An example tweet json is:

    {
        "address": "https://twitter.com/MBTA/status/1717602383570620896",
        "dataMode": "json",
        "hotlinkProtected": false,
        "connectionTimeOut": -1,
        "otherPropertiesMap": {
            "CustomFolderName": "MBTA",
            "CustomFileName": "1717602383570620896",
            "FileExtensionHandleMethod": "workoutExtension",
            "created_at": "Thu Oct 26 18:01:34 +0000 2023",
            "status_id": "1717602383570620896",
            "full_url": "https://twitter.com/MBTA/status/1717602383570620896",
            "tweet_text": "The MBTA Board of Directors Subcommittees will hold virtual or in-...",
            "owner_display_name": "MBTA",
            "favorite_count": "4",
            "quote_count": "1",
            "retweet_count": "2",
            "reply_count": "1",
            "is_retweet": "false",
            "is_quoted_tweet": "false",
            "is_reply_tweet": "true",
            "target_tweet_id": "1717602383570620871",
            "target_tweet_url": "https://twitter.com/MBTA/status/1717602383570620871",
            "has_media": "true",
            "media_urls": "https://pbs.twimg.com/media/F9YmRAnWsAA4xpI?format=jpg&name=orig",
            "media_details": [
                {
                    "type": "image",
                    "url": "https://pbs.twimg.com/media/F9YmRAnWsAA4xpI?format=jpg&name=orig"
                }
            ]
        }
    }

    The csv file will have the following columns:
    - tweet_id
    - created_at
    - tweet_text
    - tweet_url
    - is_quoted_tweet
    - is_reply_tweet
    - target_tweet_id
    - target_tweet_url
    - has_media
    - media_1_type
    - media_1_url
    - media_2_type
    - media_2_url
    ... (up to n media items, n is max in the dataset)

    params:
    - tweets_dir: The directory containing the tweet jsons
    - output_file: The file to write the tweets to, must be a csv file

    '''

    # Get all the tweet jsons
    tweet_jsons = os.listdir(tweets_dir)

    # Create a list to store the tweets
    tweets = []
    error_count = 0
    for tweet_json in tweet_jsons:
        with open(os.path.join(tweets_dir, tweet_json)) as f:
            tweet = json.load(f)

        # Create a new dictionary with the same tweet data, adhering to the csv format
        try:
            tweet_data = {
                "tweet_id": tweet["otherPropertiesMap"]["status_id"],
                "created_at": tweet["otherPropertiesMap"]["created_at"],
                "tweet_text": tweet["otherPropertiesMap"]["tweet_text"],
                "tweet_url": tweet["otherPropertiesMap"]["full_url"],
                "is_quoted_tweet": tweet["otherPropertiesMap"]["is_quoted_tweet"],
                "is_reply_tweet": tweet["otherPropertiesMap"]["is_reply_tweet"],
                "target_tweet_id": tweet["otherPropertiesMap"]["target_tweet_id"],
                "target_tweet_url": tweet["otherPropertiesMap"]["target_tweet_url"],
                "has_media": tweet["otherPropertiesMap"]["has_media"]
            }

            tweet_data['image_ids'] = []
            tweet_data['video_urls'] = []
            if tweet["otherPropertiesMap"]["has_media"] == "true":
                for media in enumerate(tweet["otherPropertiesMap"]["media_details"]):
                    if media["type"] == 'image':
                        tweet_data['image_ids'].append(image_or_video_id_from_url(media["url"]))
                    elif media["type"] == 'video':
                        tweet_data['video_urls'].append(image_or_video_id_from_url(media["url"]))
                    

        except KeyError as e:
            print(f"Error processing tweet {tweet['otherPropertiesMap']['status_id']}")
            print(f'Missing key {e.args[0]}')
            error_count += 1

            if error_count > 10:
                print('Too many errors, stopping')
                break
            
            continue


        tweets.append(tweet_data)

    # Create a dataframe with the tweets
    df = pd.DataFrame(tweets)

    print(df.head())
    print(df.dtypes)

    # Configure appropriate settings for the dataframe
    df['tweet_id'] = df['tweet_id'].astype(int)
    df.set_index("tweet_id", inplace = True)
    df["created_at"] = pd.to_datetime(df["created_at"])
    df['is_quoted_tweet'] = df['is_quoted_tweet'].map({'true': True, 'false': False})
    df['is_reply_tweet'] = df['is_reply_tweet'].map({'true': True, 'false': False})
    df['has_media'] = df['has_media'].map({'true': True, 'false': False})

    df['target_tweet_id'] = df['target_tweet_id'].replace('null', None)
    df['target_tweet_id'] = df['target_tweet_id'].astype(int)
    df['target_tweet_url'] = df['target_tweet_url'].replace('null', None)

    # Write the dataframe to a csv file
    df.to_csv(output_file)
